# SPDX-License-Identifier: Apache-2.0
"""Reload the fleet file without a restart (docs/event-driven-maintenance.md §4).

**Primary: mtime polling**, every :data:`POLL_SECONDS`. In Kubernetes a
ConfigMap update is projected into the container and the file changes on disk;
no signal is deliverable without ``kubectl exec``. Projection swaps a symlink
(``fleet.yaml -> ..data/fleet.yaml``, and ``..data`` itself is the link that
moves), so a change is detected by ``stat``-ing *through* the links -- the
inode, mtime and size of what the path resolves to now -- rather than by the
link's own mtime, which does not move.

**Secondary: SIGHUP**, sent by ``zamboni config-reload`` via a pid file, for
deployments that are not containers. It loads immediately.

**The invariant: a reload can never stop maintenance by being wrong.** A file
that fails validation leaves the running config in place and is reported --
once per distinct bad file, not every poll, so a broken generator does not bury
the log. Validation is :class:`~zamboni.fleet.FleetConfig`'s own, which refuses
an empty fleet for exactly this reason.

**A change must settle before it is loaded.** Polling can catch a writer in the
middle of rewriting the file in place, and a half-written file can be *valid*
-- a list of warehouses cut short is still a list, and loading it would stop
maintaining every warehouse after the cut. So a changed fingerprint is loaded
only when the next poll sees the same one. ConfigMap projection is atomic and
never shows a half-written file, so there this costs one interval of latency,
inside the kubelet's own ~1 minute sync. A writer elsewhere should still write
a temporary file and rename it over the old one.

What a reload changes is the *next* decision: :meth:`Scheduler.rearm
<zamboni.scheduler.Scheduler.rearm>` re-arms schedules, and the candidate queue
resolves each table against the newest fleet when it is taken. A table already
being maintained keeps the config its work item was built from.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .fleet import FleetConfig, FleetConfigError

logger = logging.getLogger(__name__)

#: How often the fleet file is polled. The design's range is 30-60 s: the
#: kubelet's sync period puts a ConfigMap update on disk within about a minute
#: regardless, so polling faster buys nothing.
POLL_SECONDS = 30

#: ``(resolved path, inode, mtime_ns, size)`` per watched file; ``None`` fields
#: for a file that is not there.
Fingerprint = tuple[tuple[str, int | None, int | None, int | None], ...]


def fingerprint(paths: tuple[Path, ...]) -> Fingerprint:
    """What the watched files look like now, followed through symlinks."""
    out: list[tuple[str, int | None, int | None, int | None]] = []
    for path in paths:
        try:
            st = path.stat()  # follows links: the projected file, not the link
            out.append((os.path.realpath(path), st.st_ino, st.st_mtime_ns, st.st_size))
        except OSError:
            out.append((str(path), None, None, None))
    return tuple(out)


@dataclass(frozen=True)
class ReloadOutcome:
    """What one check did. ``fleet`` is set only when a new config was adopted."""

    changed: bool
    fleet: FleetConfig | None = None
    error: str | None = None


class FleetWatcher:
    """Holds the running fleet and decides when to replace it."""

    def __init__(self, path: str | Path, fleet: FleetConfig | None = None) -> None:
        self.path = Path(path)
        self.fleet = fleet if fleet is not None else FleetConfig.load(self.path)
        #: Increments on every adopted reload; the state file reports it (#122).
        self.generation = 1
        self.last_error: str | None = None
        self.last_reload: datetime | None = None
        self._seen = fingerprint(self._watched())
        self._pending: Fingerprint | None = None
        self._reported: Fingerprint | None = None

    def _watched(self) -> tuple[Path, ...]:
        # The fleet file always, even when the running fleet was built in code
        # and has no sources; plus every table config the running fleet read.
        return tuple(dict.fromkeys((self.path, *self.fleet.sources)))

    def check(self, *, force: bool = False) -> ReloadOutcome:
        """Poll once. ``force`` (SIGHUP) skips the settle and loads now."""
        now = fingerprint(self._watched())
        if now == self._seen and not force:
            self._pending = None
            return ReloadOutcome(changed=False)
        if now == self._reported and not force:
            # The same bad file as last time: already reported, not re-parsed.
            return ReloadOutcome(changed=False, error=self.last_error)
        if not force and now != self._pending:
            # First sight of this change: wait one poll for it to settle.
            self._pending = now
            return ReloadOutcome(changed=False)

        self._pending = None
        try:
            fleet = FleetConfig.load(self.path)
        except FleetConfigError as exc:
            self.last_error = str(exc)
            if now != self._reported:
                logger.error(
                    "fleet file %s is invalid; keeping generation %d: %s",
                    self.path,
                    self.generation,
                    exc,
                )
                self._reported = now
            # Not adopted, so `_seen` stays: the next change to the file -- the
            # fix -- is still a change.
            return ReloadOutcome(changed=False, error=str(exc))

        self.fleet = fleet
        self.generation += 1
        self.last_error = None
        self.last_reload = datetime.now(UTC)
        self._reported = None
        # Re-taken over the *new* sources: a reload can add or drop a
        # referenced table config, and the watch set follows it.
        self._seen = fingerprint(self._watched())
        logger.info(
            "fleet file %s reloaded: generation %d, %d warehouse(s)",
            self.path,
            self.generation,
            len(fleet.warehouses),
        )
        return ReloadOutcome(changed=True, fleet=fleet)


async def watch(
    watcher: FleetWatcher,
    stop: asyncio.Event,
    *,
    on_reload: Callable[[FleetConfig], None],
    hangup: asyncio.Event | None = None,
    interval: float = POLL_SECONDS,
) -> None:
    """Poll until ``stop``; a set ``hangup`` forces an immediate load.

    ``on_reload`` receives each adopted fleet -- the service passes one that
    calls ``Scheduler.rearm``. It runs between ticks on the same loop, so a
    reload is never observed half-applied.
    """
    hangup = hangup or asyncio.Event()
    while not stop.is_set():
        waiters = {asyncio.ensure_future(stop.wait()), asyncio.ensure_future(hangup.wait())}
        _, pending = await asyncio.wait(
            waiters, timeout=interval, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if stop.is_set():
            return
        forced = hangup.is_set()
        hangup.clear()
        outcome = watcher.check(force=forced)
        if outcome.fleet is not None:
            try:
                on_reload(outcome.fleet)
            except Exception:  # the old schedule keeps running; say so
                logger.exception("applying fleet generation %d", watcher.generation)


def install_hangup(loop: asyncio.AbstractEventLoop, hangup: asyncio.Event) -> None:
    """Route SIGHUP to ``hangup``. Reload-on-HUP is the convention a sysadmin expects."""
    loop.add_signal_handler(signal.SIGHUP, hangup.set)


# -- the pid file, for `zamboni config-reload` ----------------------------------


class PidFileError(RuntimeError):
    """No running service could be found to signal. The CLI's exit 2."""


def write_pid_file(path: str | Path) -> None:
    """Record this process's pid, atomically: a reader sees the old or the new, never half."""
    path = Path(path)
    tmp = path.with_name(f".{path.name}.{os.getpid()}")
    tmp.write_text(f"{os.getpid()}\n")
    tmp.replace(path)


def send_reload(path: str | Path, *, proc: Path = Path("/proc")) -> int:
    """SIGHUP the service named by ``path``; returns its pid.

    **Checks the pid is Zamboni before signalling it.** A pid file outlives its
    process, the pid is reused, and SIGHUP's default action is to *terminate* --
    so signalling a stale pid would kill whatever unrelated process now has the
    number. ``/proc/<pid>/cmdline`` must mention zamboni; where ``/proc`` is
    absent the check cannot be made and the signal is refused rather than sent
    on trust.
    """
    path = Path(path)
    try:
        pid = int(path.read_text().strip())
    except (OSError, ValueError) as exc:
        raise PidFileError(f"{path}: no service pid to signal ({exc})") from None
    try:
        cmdline = (proc / str(pid) / "cmdline").read_bytes().replace(b"\0", b" ")
    except OSError:
        raise PidFileError(
            f"{path}: pid {pid} is not running (or {proc} is unavailable); the pid file is stale"
        ) from None
    if b"zamboni" not in cmdline:
        raise PidFileError(
            f"{path}: pid {pid} is not a zamboni process ({cmdline.decode(errors='replace')!r}); "
            "refusing to signal it -- the pid file is stale and the number was reused"
        )
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGHUP)
        return pid
    raise PidFileError(f"{path}: pid {pid} exited before it could be signalled")
