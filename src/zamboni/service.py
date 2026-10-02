# SPDX-License-Identifier: Apache-2.0
"""``zamboni serve``: the scheduler, the pool and the reload watcher, as one process.

::

  fleet file ─▶ FleetWatcher ─rearm─▶ Scheduler ─offer─▶ CandidateQueue ─▶ run_workers ─▶ WorkerPool
                  (#119)              (#120, #144)          (#120)          (#121, #147)
                                                                                           │
                       state file, run log  ◀──────────── per-table results ◀─────────────┘

Three loops on one asyncio event loop, sharing one ``stop`` event. Nothing here
decides whether a table needs work -- that is still ``maintain()``'s due-check,
reached through the CLI each worker runs -- so this module is assembly: what it
owns is the **lifecycle** (start, run, stop) and **what an operator can see**
(the state file, the run log, the probes). docs/event-driven-maintenance.md §7.

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
import fcntl
import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .fleet import FleetConfig
from .pool import ItemResult, PoolSize, WorkerConfig, WorkerPool, run_workers
from .reload import POLL_SECONDS, FleetWatcher, watch, write_pid_file
from .scheduler import MAX_SLEEP, CandidateQueue, Scheduler, run_scheduler

logger = logging.getLogger(__name__)

#: Liveness fails when the scheduler has not ticked for this long. The loop
#: ticks at least every :data:`~zamboni.scheduler.MAX_SLEEP` (60 s), so three
#: missed ticks is a loop that is wedged rather than one that is busy.
LIVENESS_STALE = MAX_SLEEP * 3

STATE_VERSION = 1

PROBES = ("liveness", "readiness", "startup")


def now_utc() -> datetime:
    return datetime.now(UTC)


def _iso(moment: datetime | None) -> str | None:
    return moment.isoformat().replace("+00:00", "Z") if moment else None


# -- what an operator sees ------------------------------------------------------


@dataclass
class _Sweep:
    """The tables one firing asked for, until every one of them has a result.

    **Only the tables the firing actually asked for.** A table already being
    maintained when the schedule fires is not in it: the queue refused that
    offer, and the run in flight started before the firing, so no result will
    ever answer it. A table still *queued* is in it, and its one run answers
    every firing that coalesced into it. Each firing is its own sweep, so a
    schedule faster than a sweep cannot keep one open forever.
    """

    fired_at: datetime
    pending: set[str]
    tables: int
    worst_exit_code: int = 0
    failures: int = 0


@dataclass
class ServiceState:
    """Everything ``service-status`` reads. Written whole, atomically."""

    fleet_path: str
    dry_run: bool
    workers: int
    pid: int = field(default_factory=os.getpid)
    started_at: datetime = field(default_factory=now_utc)
    #: ``starting`` -> ``running`` -> ``stopping`` -> ``stopped``.
    phase: str = "starting"
    last_tick: datetime | None = None
    generation: int = 0
    busy: int = 0
    #: No NATS consumer exists yet (#110); ``None`` says "not configured"
    #: rather than "disconnected", so readiness does not wait for it.
    nats_connected: bool | None = None
    last_error: str | None = None
    #: The last reload that was refused. The running config is still valid.
    reload_error: str | None = None
    warehouses: dict[str, dict[str, Any]] = field(default_factory=dict)
    _sweeps: dict[str, list[_Sweep]] = field(default_factory=dict, repr=False)

    def fired(self, warehouse: str, tables: list[str], at: datetime) -> None:
        """A firing that asked for ``tables`` -- excluding any already in flight."""
        entry = self.warehouses.setdefault(warehouse, {})
        entry["last_fired"] = _iso(at)
        if tables:
            self._sweeps.setdefault(warehouse, []).append(_Sweep(at, set(tables), len(tables)))
        entry["open_sweeps"] = len(self._sweeps.get(warehouse, []))

    def finished(self, result: ItemResult, at: datetime) -> None:
        warehouse = result.item.warehouse
        entry = self.warehouses.setdefault(warehouse, {})
        entry["last_result"] = _iso(at)
        entry["last_exit_code"] = result.exit_code
        if result.exit_code:
            self.last_error = f"{warehouse}/{result.item.table}: " + (
                result.died or f"exit {result.exit_code}"
            )
        for sweep in self._sweeps.get(warehouse, []):
            if result.item.table in sweep.pending:
                sweep.pending.discard(result.item.table)
                sweep.worst_exit_code = max(sweep.worst_exit_code, result.exit_code)
                sweep.failures += bool(result.exit_code)
        self._close_finished(warehouse, at)

    def adopt(self, fleet: FleetConfig, at: datetime) -> None:
        """A reload: forget what the new fleet no longer has."""
        for name in list(self._sweeps):
            try:
                tables = set(fleet[name].table_config.tables)
            except KeyError:
                del self._sweeps[name]
                continue
            for sweep in self._sweeps[name]:
                sweep.pending &= tables
            self._close_finished(name, at)
        for name in list(self.warehouses):
            if name not in fleet.names:
                del self.warehouses[name]

    def _close_finished(self, warehouse: str, at: datetime) -> None:
        sweeps = self._sweeps.get(warehouse, [])
        done = [s for s in sweeps if not s.pending]
        if not done:
            return
        latest = max(done, key=lambda s: s.fired_at)
        entry = self.warehouses.setdefault(warehouse, {})
        entry["last_sweep"] = {
            "fired_at": _iso(latest.fired_at),
            "finished_at": _iso(at),
            "tables": latest.tables,
            "failures": latest.failures,
            "worst_exit_code": latest.worst_exit_code,
        }
        self._sweeps[warehouse] = [s for s in sweeps if s.pending]
        entry["open_sweeps"] = len(self._sweeps[warehouse])

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "pid": self.pid,
            "fleet_path": self.fleet_path,
            "dry_run": self.dry_run,
            "phase": self.phase,
            "started_at": _iso(self.started_at),
            "last_tick": _iso(self.last_tick),
            "generation": self.generation,
            "workers": self.workers,
            "busy": self.busy,
            "nats_connected": self.nats_connected,
            "last_error": self.last_error,
            "reload_error": self.reload_error,
            "warehouses": self.warehouses,
        }

    def write(self, path: Path) -> None:
        """Atomically: a probe reads the old file or the new one, never half."""
        tmp = path.with_name(f".{path.name}.{os.getpid()}")
        tmp.write_text(json.dumps(self.as_dict(), indent=2, sort_keys=True) + "\n")
        tmp.replace(path)


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
    """
    if result.record is not None:
        return {**result.record, "source": "serve"}
    ended = now_utc()
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
    """One ``zamboni serve``."""

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
    ) -> None:
        self.fleet_path = Path(fleet_path)
        self.size = size
        self.worker_config = worker_config
        self.state_path = Path(state_path)
        self.run_log = Path(run_log) if run_log else None
        self.poll_seconds = poll_seconds
        self.pool_factory = pool_factory
        self.clock = clock
        self.scheduler_wait = scheduler_wait
        self.capability_check = capability_check or _capabilities_unsupported
        self.state = ServiceState(
            fleet_path=str(self.fleet_path),
            dry_run=not worker_config.commit,
            workers=size.workers,
        )
        self.pool: Any = None
        self.stop = asyncio.Event()
        self.hangup = asyncio.Event()
        self._forced = False

    def _write(self) -> None:
        try:
            self.state.write(self.state_path)
        except OSError:
            # The state file is for observers. Failing to write it must not stop
            # maintenance; liveness will notice the staleness, which is right.
            logger.exception("could not write the state file %s", self.state_path)

    def request_stop(self) -> None:
        """First call: graceful. Second: kill the in-flight tables' workers too."""
        if self.stop.is_set():
            if not self._forced and self.pool is not None and hasattr(self.pool, "kill"):
                self._forced = True
                logger.warning("second stop signal: killing in-flight workers")
                self.pool.kill()
            return
        logger.info("stopping: no new tables; letting %d in flight finish", self.state.busy)
        self.stop.set()

    async def run(self) -> int:
        """Run until stopped. 0 on a clean stop; 2 if it could not start."""
        self.state.phase = "starting"
        self._write()

        # The fleet first -- it is the cheap check and the likely mistake.
        watcher = FleetWatcher(self.fleet_path)  # raises FleetConfigError: exit 2
        self.state.generation = watcher.generation

        reason = await asyncio.to_thread(self.capability_check)
        if reason is not None:
            self.state.phase, self.state.last_error = "stopped", reason
            self._write()
            logger.error("cannot maintain anything with this PyIceberg: %s", reason)
            return 2

        scheduler = Scheduler(watcher.fleet, self.clock())
        queue = CandidateQueue()
        self.pool = self.pool_factory(self.size, self.worker_config)
        self.state.phase = "running"
        self._write()
        if self.state.dry_run:
            logger.warning("dry run: no --yes, so every table previews and nothing is committed")

        def on_tick(at: datetime) -> None:
            self.state.last_tick = at
            self.state.busy = len(queue.in_flight)
            self._write()

        def on_fired(at: datetime, fired: list[str]) -> None:
            # Called in the same loop step as the tick, so `in_flight` is
            # exactly what the queue refused these offers for.
            for name in fired:
                busy = {table for wh, table in queue.in_flight if wh == name}
                asked = sorted(set(scheduler.fleet[name].table_config.tables) - busy)
                self.state.fired(name, asked, at)

        def on_result(result: ItemResult) -> None:
            self.state.finished(result, self.clock())
            # Still counts the table reporting here; it is released just after.
            self.state.busy = len(queue.in_flight)
            if self.run_log is not None:
                with self.run_log.open("a") as out:
                    out.write(json.dumps(run_record(result), default=str) + "\n")
            self._write()

        def on_reload(fleet: FleetConfig) -> None:
            scheduler.rearm(fleet, self.clock())
            self.state.adopt(fleet, self.clock())
            self.state.generation = watcher.generation
            self.state.reload_error = None
            self._write()

        def on_reload_error(reason: str) -> None:
            if reason != self.state.reload_error:
                self.state.reload_error = reason
                self._write()

        async def mark_stopping() -> None:
            await self.stop.wait()
            self.state.phase = "stopping"
            self._write()

        try:
            await asyncio.gather(
                run_scheduler(
                    scheduler,
                    queue,
                    self.stop,
                    clock=self.clock,
                    on_tick=on_tick,
                    on_fired=on_fired,
                    wait=self.scheduler_wait,
                ),
                run_workers(
                    queue, lambda: scheduler.fleet, self.pool, self.stop, on_result=on_result
                ),
                watch(
                    watcher,
                    self.stop,
                    on_reload=on_reload,
                    on_error=on_reload_error,
                    hangup=self.hangup,
                    interval=self.poll_seconds,
                ),
                mark_stopping(),
            )
        finally:
            if hasattr(self.pool, "close"):
                await self.pool.close()
            self.state.phase, self.state.busy = "stopped", 0
            self._write()
        return 0


def _capabilities_unsupported() -> str | None:
    """Why this PyIceberg cannot be used, or ``None`` -- `zamboni doctor`'s answer."""
    from .capabilities import detect

    return detect().unsupported_reason()
