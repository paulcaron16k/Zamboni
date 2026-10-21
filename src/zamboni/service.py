# SPDX-License-Identifier: Apache-2.0
"""``zamboni serve``: a maintenance runtime built from files, as one process.

::

  fleet file ─▶ FileFleetProvider ─▶ MaintenanceRuntime ─▶ per-table results ─▶ state file, run log
                  (#119)              (zamboni.runtime, #162)                     (#122)

The scheduling -- schedules, event feeds, pressure checks, the queue and the
worker pool -- is :class:`~zamboni.runtime.MaintenanceRuntime`'s, shared with
any application that embeds Zamboni (docs/maintenance-runtime.md §3). This
module is the assembly ``serve`` makes of it: what it owns is the
**lifecycle** (refusing to start, signals, the single-instance lock) and **what
an operator can see** (the state file, the run log, the probes).
docs/event-driven-maintenance.md §7.

**Shutdown is the requirement with teeth.** On the first SIGTERM or SIGINT the
scheduler and the watcher stop, the queue stops being taken from, and tables
already in flight finish; then the workers retire and the process exits 0. A
second signal stops the in-flight tables too, by killing their workers. That is
safe for the reason a hard kill is: a compaction commits one snapshot at the
end (`ReplaceCommitter.commit`), so a rewrite killed before it commits nothing
-- with ``--partial-progress`` the groups already committed stay committed and
consistent -- and the files it had written are orphans for the next
``remove-orphans``. It is not *free*: a service killed mid-compaction on every
deploy never finishes a large table, which is why the grace period must exceed
the longest table.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import fcntl
import json
import logging
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .pool import ItemResult, PoolSize, WorkerConfig, WorkerPool
from .reload import POLL_SECONDS, FileFleetProvider, write_pid_file
from .runstate import RuntimeState, WarehouseState
from .runtime import (
    CREATED,
    STOPPED,
    MaintenanceRuntime,
    now_utc,
    pressure_offers,  # noqa: F401 -- moved to zamboni.runtime; imported from here too
    run_pressure_checks,  # noqa: F401 -- likewise
)
from .scheduler import MAX_SLEEP, WorkItem

logger = logging.getLogger(__name__)

#: Liveness fails when the scheduler has not ticked for this long. The loop
#: ticks at least every :data:`~zamboni.scheduler.MAX_SLEEP` (60 s), so three
#: missed ticks is a loop that is wedged rather than one that is busy.
LIVENESS_STALE = MAX_SLEEP * 3

STATE_VERSION = 1
#: How often an event source's connection and counters are checked for a change
#: worth writing. The runtime reports no event for either, so on a quiet service
#: the file would wait for the next tick, up to MAX_SLEEP away -- readiness a
#: minute behind a broker outage, and a fresh start unready for a minute after
#: it connected. A write happens
#: only on a change, so a quiet service costs one comparison a second.
EVENTS_OBSERVE_SECONDS = 1.0

PROBES = ("liveness", "readiness", "startup")


def _iso(moment: datetime | None) -> str | None:
    return moment.isoformat().replace("+00:00", "Z") if moment else None


# -- what an operator sees ------------------------------------------------------

#: The runtime's phases as the state file names them: a runtime not yet running
#: is a service still starting.
_SERVE_PHASE = {CREATED: "starting"}


def _plain(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    return value


def _warehouse(state: WarehouseState) -> dict[str, Any]:
    """Only what has happened: a field still ``None`` is left out, not written null."""
    return {
        key: _plain(value) for key, value in dataclasses.asdict(state).items() if value is not None
    }


@dataclass(frozen=True)
class ServiceState:
    """``serve``'s own part of the state file ``service-status`` reads.

    Everything else in the file is the runtime's :class:`RuntimeState`
    (#166): this adds only what the runtime cannot know -- the process, the
    fleet file, whether ``--yes`` was given.
    """

    fleet_path: str
    dry_run: bool
    pid: int = field(default_factory=os.getpid)
    started_at: datetime = field(default_factory=now_utc)

    def as_dict(self, state: RuntimeState) -> dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "pid": self.pid,
            "fleet_path": self.fleet_path,
            "dry_run": self.dry_run,
            "phase": _SERVE_PHASE.get(state.phase, state.phase),
            "started_at": _iso(self.started_at),
            "last_tick": _iso(state.last_tick),
            "generation": state.generation,
            "workers": state.workers,
            "busy": state.busy,
            "nats_connected": state.sources_connected,
            "events": _plain(state.events),
            "last_error": state.last_error,
            "reload_error": state.reload_error,
            "warehouses": {name: _warehouse(w) for name, w in state.warehouses.items()},
        }

    def write(self, path: Path, state: RuntimeState) -> None:
        """Atomically: a probe reads the old file or the new one, never half."""
        tmp = path.with_name(f".{path.name}.{os.getpid()}")
        tmp.write_text(json.dumps(self.as_dict(state), indent=2, sort_keys=True) + "\n")
        tmp.replace(path)


class StateFileObserver:
    """Rewrites the state file whenever the runtime reports a change to it.

    Called on the runtime's loop, as before #166: a write is one small file,
    and a probe must see each phase as it happens. ``offered`` is not a change
    to the file -- the queue is not in it -- so it is not observed.
    """

    def __init__(
        self, path: Path, service: ServiceState, state: Callable[[], RuntimeState]
    ) -> None:
        self.path = path
        self.service = service
        self._state = state

    def write(self, state: RuntimeState | None = None) -> None:
        try:
            self.service.write(self.path, self._state() if state is None else state)
        except OSError:
            # The state file is for observers. Failing to write it must not stop
            # maintenance; liveness will notice the staleness, which is right.
            logger.exception("could not write the state file %s", self.path)

    def _changed(self, *_: Any) -> None:
        self.write()

    phase_changed = ticked = fired = started = finished = _changed
    fleet_changed = refused = pressure_checked = _changed


def read_state(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def probe(
    state: dict[str, Any],
    kind: str,
    *,
    now: datetime | None = None,
    proc: Path = Path("/proc"),
) -> tuple[bool, str]:
    """Whether a Kubernetes probe of ``kind`` passes, and why.

    Deliberately narrow (§7). **Liveness** asserts that the scheduler loop
    ticked recently and nothing else: checking a catalog or NATS would turn an
    external outage into a restart loop, and a restart fixes neither.
    **Readiness** is "running on a valid config" -- busy is not unready, and a
    refused reload leaves the running config valid. **Startup** is "the first
    config load and the capability probe finished".

    All three first check that the pid the file names is alive, because a file
    written by a process that has since died says nothing about the present.
    """
    now = now or now_utc()
    pid = state.get("pid")
    if not pid or not (proc / str(pid)).exists():
        return False, f"pid {pid} is not running"
    phase = state.get("phase")
    if kind == "startup":
        return (phase in ("running", "stopping"), f"phase {phase}")
    if kind == "readiness":
        ok = phase == "running" and int(state.get("generation") or 0) >= 1
        # An event source that is configured but disconnected makes the service
        # unready, never unalive: maintenance continues on the schedule, and a
        # restart would not reconnect a broker that is down (§7).
        if state.get("nats_connected") is False:
            return False, f"phase {phase}, event source disconnected"
        return ok, f"phase {phase}, config generation {state.get('generation')}"
    if kind == "liveness":
        if phase == "stopped":
            return False, "stopped"
        stamp = state.get("last_tick") or state.get("started_at")
        if not stamp:
            return False, "no tick recorded"
        age = now - datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        ok = age <= LIVENESS_STALE
        limit = int(LIVENESS_STALE.total_seconds())
        return ok, f"last tick {int(age.total_seconds())} s ago (limit {limit} s)"
    raise ValueError(f"unknown probe {kind!r}; expected one of {PROBES}")


# -- the run log ----------------------------------------------------------------


def run_record(result: ItemResult) -> dict[str, Any]:
    """The line ``zamboni runs`` reads for this table.

    The worker's own ``--json`` record when there is one -- the same shape a
    cron run writes, so ``zamboni runs`` reads the service and cron alike. A
    worker that died wrote nothing, so one is made: exit 1, and one *failed*
    unit of work, because the per-operation counts died with it and counting
    the table as considered-and-failed keeps ``considered == skipped +
    maintained + failed`` true across the series.

    A table dropped at its deadline never started, so it did no work: exit 0,
    ``dropped`` and the deadline, and no unit in any bucket -- as #201 leaves
    work a stop never started out of the counts. ``zamboni runs`` reads it as
    a run of that table with nothing considered.
    """
    if result.record is not None:
        return {**result.record, "source": "serve"}
    ended = now_utc()
    if result.dropped is not None:
        not_after = result.item.not_after if isinstance(result.item, WorkItem) else None
        return {
            "source": "serve",
            "warehouse": result.item.warehouse,
            "tables": [result.item.table],
            "exit_code": result.exit_code,
            "dropped": result.dropped,
            "not_after": _iso(not_after),
            "started_at": _iso(ended),
            "ended_at": _iso(ended),
            "duration_seconds": 0.0,
            "counters": {
                "tables": 1,
                "considered": 0,
                "skipped": 0,
                "maintained": 0,
                "failed": 0,
                "skip_rate": 0.0,
            },
            "outcomes": [],
        }
    return {
        "source": "serve",
        "warehouse": result.item.warehouse,
        "tables": [result.item.table],
        "exit_code": result.exit_code,
        "failures": 1,
        "died": result.died,
        "started_at": _iso(ended - timedelta(seconds=result.seconds)),
        "ended_at": _iso(ended),
        "duration_seconds": result.seconds,
        "counters": {
            "tables": 1,
            "considered": 1,
            "skipped": 0,
            "maintained": 0,
            "failed": 1,
            "skip_rate": 0.0,
        },
        "outcomes": [],
    }


class RunLogObserver:
    """Appends each table's :func:`run_record` to the run log ``zamboni runs`` reads.

    A write that fails is logged and the runtime carries on: a run that did its
    work does not fail over its reporting (`zamboni.observe`).
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def finished(self, result: ItemResult) -> None:
        with self.path.open("a") as out:
            out.write(json.dumps(run_record(result), default=str) + "\n")


# -- the single-instance lock -----------------------------------------------------


class AlreadyRunning(RuntimeError):
    """Another ``zamboni serve`` holds this pid file's lock. The CLI's exit 2."""


@contextlib.contextmanager
def single_instance(pid_file: Path):
    """Hold an exclusive lock beside the pid file for the life of the service.

    Host-local only: it stops a second ``serve`` on the same machine and pid
    file, the cheapest overlap to rule out. It is not a claim protocol -- two
    hosts, or two pods, are not excluded, which is why the deployment notes say
    ``replicas: 1`` and ``strategy: Recreate``.
    """
    lock_path = pid_file.with_name(pid_file.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("w")
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AlreadyRunning(
                f"{lock_path} is held: another `zamboni serve` is running with this pid file"
            ) from None
        write_pid_file(pid_file)
        yield
    finally:
        with contextlib.suppress(OSError):
            if pid_file.read_text().strip() == str(os.getpid()):
                pid_file.unlink()
        handle.close()


# -- the service ------------------------------------------------------------------


class Service:
    """One ``zamboni serve``: a :class:`~zamboni.runtime.MaintenanceRuntime` built
    from the fleet file, observed by the state file and the run log, behind the
    capability check."""

    def __init__(
        self,
        fleet_path: str | Path,
        *,
        size: PoolSize,
        worker_config: WorkerConfig,
        state_path: str | Path,
        run_log: str | Path | None = None,
        poll_seconds: float = POLL_SECONDS,
        pool_factory: Callable[[PoolSize, WorkerConfig], Any] = WorkerPool,
        clock: Callable[[], datetime] = now_utc,
        scheduler_wait: Any = None,
        capability_check: Callable[[], str | None] | None = None,
        events: Sequence[Any] = (),
        feed_observer: Any = None,
    ) -> None:
        self.fleet_path = Path(fleet_path)
        self.size = size
        self.worker_config = worker_config
        self.poll_seconds = poll_seconds
        self.pool_factory = pool_factory
        self.clock = clock
        self.scheduler_wait = scheduler_wait
        self.capability_check = capability_check or _capabilities_unsupported
        #: Event sources (`zamboni.events.feed.EventSource`); each gets a feed.
        self.events = tuple(events)
        #: Told what each feed does (`zamboni.events.feed.FeedObserver`) -- the
        #: OpenTelemetry one under `serve --metrics otel` (#181).
        self.feed_observer = feed_observer
        self.state_file = StateFileObserver(
            Path(state_path),
            ServiceState(fleet_path=str(self.fleet_path), dry_run=not worker_config.commit),
            self._state,
        )
        self.run_log = RunLogObserver(Path(run_log)) if run_log else None
        #: Built by :meth:`run`, once the fleet file has loaded and PyIceberg passed.
        self.runtime: MaintenanceRuntime | None = None
        #: What the state file says before there is a runtime to ask.
        self._unstarted = RuntimeState(
            phase=CREATED,
            generation=0,
            workers=size.workers,
            last_tick=None,
            queued=(),
            in_flight=(),
            dirty=(),
        )
        #: Set once the runtime has stopped. Ends the event-state loop, so the
        #: file stays current through a drain.
        self.stop = asyncio.Event()
        self.hangup = asyncio.Event()
        self._stop_requests = 0

    @property
    def feeds(self) -> list[Any]:
        return self.runtime.feeds if self.runtime is not None else []

    def _state(self) -> RuntimeState:
        return self.runtime.state() if self.runtime is not None else self._unstarted

    def request_stop(self) -> None:
        """First call: graceful. Second: kill the in-flight tables' workers too."""
        self._stop_requests += 1
        if self.runtime is not None:
            self.runtime.request_stop()

    async def run(self) -> int:
        """Run until stopped. 0 on a clean stop; 2 if it could not start."""
        self.state_file.write()

        # The fleet first -- it is the cheap check and the likely mistake.
        provider = FileFleetProvider(  # raises FleetConfigError: exit 2
            self.fleet_path, interval=self.poll_seconds, hangup=self.hangup
        )

        reason = await asyncio.to_thread(self.capability_check)
        if reason is not None:
            self._unstarted = dataclasses.replace(self._unstarted, phase=STOPPED, last_error=reason)
            self.state_file.write()
            logger.error("cannot maintain anything with this PyIceberg: %s", reason)
            return 2

        observers: list[Any] = [self.state_file]
        if self.run_log is not None:
            observers.insert(0, self.run_log)
        self.runtime = runtime = MaintenanceRuntime(
            fleet=provider,
            pool=self.size,
            worker=self.worker_config,
            events=self.events,
            observers=observers,
            feed_observer=self.feed_observer,
            clock=self.clock,
            pool_factory=self.pool_factory,
            wait=self.scheduler_wait,
        )
        # A stop that arrived while starting still counts, and so does a second.
        for _ in range(self._stop_requests):
            runtime.request_stop()

        if not self.worker_config.commit:
            logger.warning("dry run: no --yes, so every table previews and nothing is committed")
        watching = asyncio.create_task(self._watch_events()) if self.feeds else None
        try:
            # The state file is written as the runtime reports, through
            # `stopped` -- told even when the run raises.
            await runtime.run()
        finally:
            self.stop.set()
            if watching is not None:
                await watching
        return 0

    async def _watch_events(self) -> None:
        """Write the state file when an event source's connection or counters change."""
        last = None
        while not self.stop.is_set():
            state = self._state()
            seen = (state.sources_connected, json.dumps(_plain(state.events), sort_keys=True))
            if seen != last:
                last = seen
                self.state_file.write(state)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop.wait(), EVENTS_OBSERVE_SECONDS)


def _capabilities_unsupported() -> str | None:
    """Why this PyIceberg cannot be used, or ``None`` -- `zamboni doctor`'s answer."""
    from .capabilities import detect

    return detect().unsupported_reason()
