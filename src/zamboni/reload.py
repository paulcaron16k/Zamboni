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
import logging
import os
import signal
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psutil

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
    on_reload: Callable[[FleetConfig], object],
    on_error: Callable[[str], None] | None = None,
    hangup: asyncio.Event | None = None,
    interval: float = POLL_SECONDS,
) -> None:
    """Poll until ``stop``; a set ``hangup`` forces an immediate load.

    ``on_reload`` receives each adopted fleet -- :class:`FileFleetProvider`
    passes the runtime's, which calls ``Scheduler.rearm``. It runs between
    ticks on the same loop, so a reload is never observed half-applied.
    ``on_error`` receives a refused reload's reason, on every poll that still
    sees the bad file.
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
        if outcome.error is not None and on_error is not None:
            on_error(outcome.error)
        if outcome.fleet is not None:
            try:
                on_reload(outcome.fleet)
            except Exception:  # the old schedule keeps running; say so
                logger.exception("applying fleet generation %d", watcher.generation)


class FileFleetProvider:
    """The fleet file as a runtime's `~zamboni.runtime.FleetProvider`: ``zamboni serve``'s.

    Loads the file on construction, so an invalid one raises
    :class:`~zamboni.fleet.FleetConfigError` before anything starts, and then
    polls it every ``interval`` seconds -- or at once when ``hangup`` is set.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        interval: float = POLL_SECONDS,
        hangup: asyncio.Event | None = None,
    ) -> None:
        self.watcher = FleetWatcher(path)
        self.interval = interval
        self.hangup = hangup or asyncio.Event()

    @property
    def fleet(self) -> FleetConfig:
        return self.watcher.fleet

    async def run(
        self,
        adopt: Callable[[FleetConfig], int],
        refused: Callable[[str], None],
        stop: asyncio.Event,
    ) -> None:
        await watch(
            self.watcher,
            stop,
            on_reload=adopt,
            on_error=refused,
            hangup=self.hangup,
            interval=self.interval,
        )


def install_hangup(loop: asyncio.AbstractEventLoop, hangup: asyncio.Event) -> None:
    """Route SIGHUP to ``hangup``. Reload-on-HUP is the convention a sysadmin expects.

    Windows has no SIGHUP, so there the fleet file is reloaded by polling alone
    (ZMBNI-236) -- the primary mechanism everywhere; SIGHUP only skips the wait.
    """
    if sys.platform == "win32":
        return
    loop.add_signal_handler(signal.SIGHUP, hangup.set)


# -- the pid file, for `zamboni config-reload` ----------------------------------


class PidFileError(RuntimeError):
    """No running service could be found to signal. The CLI's exit 2."""


def write_pid_file(path: str | Path) -> None:
    """Record this process's pid, atomically: a reader sees the old or the new, never half."""
    path = Path(path)
    tmp = path.with_name(f".{path.name}.{os.getpid()}")
    tmp.write_text(f"{os.getpid()}\n", encoding="utf-8")
    tmp.replace(path)


#: How long after its pid file was written a process may appear to have
#: started and still be the process that wrote it. Not zero because psutil's
#: Linux ``create_time`` is ``/proc/stat``'s ``btime`` -- whole seconds -- plus
#: the start in clock ticks (``_pslinux.Process.create_time``), so it can read
#: up to a second late; macOS answers in microseconds. A pid reused within two
#: seconds of a serve writing its file is not a case worth trading that for.
#:
#: Expected from the kernel, not reproduced: Linux moves ``btime`` when the wall
#: clock is stepped, so a clock stepped *forward* after serve started -- NTP's
#: first sync on a board with no RTC -- makes a genuine serve look younger than
#: its pid file, and the reload is refused. That is the safe direction, the
#: :data:`POLL_SECONDS` poll still reloads, and the message names the cause.
PID_FILE_SLACK_SECONDS = 2.0


def send_reload(path: str | Path) -> int:
    """SIGHUP the service named by ``path``; returns its pid.

    **Checks the pid is the Zamboni that wrote the file before signalling it.**
    A pid file outlives its process, the pid is reused, and SIGHUP's default
    action is to *terminate* -- so signalling a stale pid would kill whatever
    unrelated process now has the number. Two checks, both through psutil so
    they answer on Linux and macOS alike (ZMBNI-235; until then they read
    ``/proc``, and on macOS every reload was refused):

    - the process's command line must mention zamboni;
    - it must have started no later than the pid file was written (allowing
      :data:`PID_FILE_SLACK_SECONDS`). The cmdline check alone passes a second
      zamboni process that was later given the same number. The file's own
      mtime is the write time, so the pid file stays one pid on one line --
      what ``kill $(cat serve.pid)`` expects -- and a file written before this
      check existed needs no migration.

    Anything that cannot be checked -- a process owned by another user, whose
    command line the OS will not show -- is refused rather than signalled on
    trust.

    Windows has no SIGHUP, so there it refuses outright; the service still
    polls the fleet file (ZMBNI-236).
    """
    path = Path(path)
    if sys.platform == "win32":
        raise PidFileError(
            "config-reload sends SIGHUP, which Windows does not have; the service "
            f"reloads the fleet file on its own within {POLL_SECONDS} s of a change"
        )
    try:
        pid = int(path.read_text(encoding="utf-8").strip())
        written = path.stat().st_mtime
    except (OSError, ValueError) as exc:
        raise PidFileError(f"{path}: no service pid to signal ({exc})") from None
    try:
        process = psutil.Process(pid)
        cmdline = " ".join(process.cmdline())
        started = process.create_time()
    except psutil.NoSuchProcess:  # ZombieProcess included: it is a subclass
        raise PidFileError(f"{path}: pid {pid} is not running; the pid file is stale") from None
    except psutil.AccessDenied:
        raise PidFileError(
            f"{path}: pid {pid} belongs to another user, so whether it is zamboni cannot be "
            "checked; refusing to signal it"
        ) from None
    if "zamboni" not in cmdline:
        raise PidFileError(
            f"{path}: pid {pid} is not a zamboni process ({cmdline!r}); "
            "refusing to signal it -- the pid file is stale and the number was reused"
        )
    if started > written + PID_FILE_SLACK_SECONDS:
        raise PidFileError(
            f"{path}: pid {pid} started {started - written:.0f} s after the pid file was "
            "written, so it is not the zamboni that wrote it; refusing to signal it -- the "
            "pid file is stale and the number was reused (or the clock was stepped forward "
            f"since; the fleet file is still polled every {POLL_SECONDS} s)"
        )
    # is_running() re-reads the start time and compares it with the one above;
    # send_signal() only consults the flag that leaves behind, so without it a
    # pid reused since the checks would be signalled (psutil 7.2.2's
    # Process._send_signal / _raise_if_pid_reused).
    try:
        if process.is_running():
            process.send_signal(signal.SIGHUP)
            return pid
    except psutil.NoSuchProcess:
        pass
    except psutil.AccessDenied:
        # Linux shows any user's command line, so another user's serve gets
        # this far; the kill is where it is refused.
        raise PidFileError(
            f"{path}: pid {pid} is a zamboni owned by another user; run config-reload as that user"
        ) from None
    raise PidFileError(f"{path}: pid {pid} exited before it could be signalled")
