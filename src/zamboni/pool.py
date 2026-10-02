# SPDX-License-Identifier: Apache-2.0
"""A bounded pool of worker processes over tables: the back half of ``zamboni serve``.

::

  CandidateQueue ──▶ run_workers ──▶ WorkerPool ──▶ worker process ──▶ zamboni maintenance <table>
                     (takes only       (N slots)      (long-lived,
                      when a slot      sized from     recycled)
                      is free)         the cgroup

**Processes, not threads**, because the GIL is held through avro decode and
reachable-set arithmetic -- where the measured ~2,035 ms of an orphan scan sits
-- and because a process is the unit an OOM kill takes
(docs/event-driven-maintenance.md §3).

**A worker runs the CLI's own ``maintenance`` path**, for one table, in-process.
Not a second implementation of it: the resolution order (flag > env > profile),
the session, the engine, the reporter, the exit codes and the ``--json`` run
record are all the code a cron line already runs, so the service cannot drift
from it. What crosses the process boundary is an argv of strings, a
table-config file and the run record coming back -- never a ``CatalogSession``,
and never a secret on a command line: the worker reads ``.env`` and the profile
itself.

**Long-lived workers, not one process per table.** Measured on this repo's dev
machine (``/usr/bin/time``, three runs, 2026-10-01): importing what a worker
needs costs 1.7-2.1 s and ~174 MB. Per table, that is 2 s of startup for a table
whose due-check then takes ~25 ms -- for 1,000 tables on 4 workers, ~8 minutes
of every sweep spent importing. A forkserver with the imports preloaded would
fork in ~17 ms (measured), but ``pyarrow`` and ``duckdb`` start native threads
at import -- 1 thread before, 8 after -- and forking a threaded process can
leave a lock held by a thread that no longer exists. So workers are *spawned*,
handle many tables each, and are recycled.

**A worker dying takes one table, not the pool.** That is why this is not
:class:`concurrent.futures.ProcessPoolExecutor`, which marks itself broken when
any worker dies and fails every pending future with it. Here the slot reports
its one table as failed (exit 1, Python's own code for an uncaught crash, so a
dead worker reads the same as a crashed CLI run) and spawns a replacement for
the next table.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import math
import os
import sys
import time
import traceback
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from multiprocessing import connection, get_context
from pathlib import Path
from typing import Any

from .fleet import FleetConfig
from .scheduler import Candidate, CandidateQueue, WorkItem

logger = logging.getLogger(__name__)

#: A worker exits and is replaced after this many tables, to hand back memory
#: that a long-lived Python, Arrow and DuckDB process fragments rather than
#: frees. **Chosen, not measured**: there is no fleet figure for growth per
#: table yet. Low enough to bound growth, high enough that the 2 s spawn is
#: amortised to well under 5% of a sweep.
RECYCLE_AFTER_TABLES = 50

#: Resident memory of a worker before it touches a table: ~174 MB maxrss
#: measured importing `zamboni.cli`, `pyiceberg.catalog.rest`, `duckdb` and
#: `pyarrow` (3 runs, 2026-10-01), rounded up to leave room for the catalog
#: client and the session.
WORKER_BASELINE_BYTES = 256 * 1024 * 1024

#: The share of the memory limit the pool plans to use. The rest is headroom for
#: what `memory_budget_bytes` does not bound -- Arrow buffers in flight, DuckDB's
#: own working set -- because the kernel answers an overrun with SIGKILL.
MEMORY_HEADROOM = 0.8

#: Where cgroups are mounted, and where a process finds its own.
CGROUP_ROOT = Path("/sys/fs/cgroup")
PROC_CGROUP = Path("/proc/self/cgroup")

#: cgroup v1 reports "no limit" as the largest page-aligned value that fits.
_V1_UNLIMITED = 1 << 62


# -- sizing ---------------------------------------------------------------------


def cgroup_cpu_limit(root: Path = CGROUP_ROOT, proc: Path = PROC_CGROUP) -> float | None:
    """CPUs the cgroup allows, as a fraction (1.5 = one and a half), or ``None``.

    cgroup v2: ``cpu.max`` is ``"<quota> <period>"`` or ``"max <period>"``, at
    every level from the process's own cgroup up to the root, and the
    effective limit is the **smallest** -- a pod's limit is set on a parent of
    the container's cgroup, not on it. v1: ``cpu.cfs_quota_us`` /
    ``cpu.cfs_period_us``, with -1 meaning none.
    """
    limits = []
    for directory in _cgroup_dirs(root, proc, v1_controller="cpu"):
        v2 = directory / "cpu.max"
        if v2.is_file():
            quota, _, period = v2.read_text().strip().partition(" ")
            if quota != "max" and period:
                limits.append(int(quota) / int(period))
            continue
        quota_file, period_file = directory / "cpu.cfs_quota_us", directory / "cpu.cfs_period_us"
        if quota_file.is_file() and period_file.is_file():
            quota_us = int(quota_file.read_text())
            if quota_us > 0:
                limits.append(quota_us / int(period_file.read_text()))
    return min(limits) if limits else None


def cgroup_memory_limit(root: Path = CGROUP_ROOT, proc: Path = PROC_CGROUP) -> int | None:
    """Bytes the cgroup allows, or ``None``. The smallest up the tree, as for CPU."""
    limits = []
    for directory in _cgroup_dirs(root, proc, v1_controller="memory"):
        for name in ("memory.max", "memory.limit_in_bytes"):
            path = directory / name
            if path.is_file():
                text = path.read_text().strip()
                if text != "max" and int(text) < _V1_UNLIMITED:
                    limits.append(int(text))
                break
    return min(limits) if limits else None


def _cgroup_dirs(root: Path, proc: Path, *, v1_controller: str) -> list[Path]:
    """The process's cgroup directories, leaf first, for one controller."""
    try:
        lines = proc.read_text().splitlines()
    except OSError:
        return []
    for line in lines:
        _, controllers, path = line.split(":", 2)
        if controllers == "":  # cgroup v2: one unified hierarchy
            base = root
        elif v1_controller in controllers.split(","):
            base = root / controllers  # e.g. /sys/fs/cgroup/cpu,cpuacct
            if not base.is_dir():
                base = root / v1_controller
        else:
            continue
        parts = [p for p in path.split("/") if p]
        return [base.joinpath(*parts[:n]) for n in range(len(parts), -1, -1)]
    return []


def available_cpus() -> int:
    """Whole CPUs this process may use: affinity, then the cgroup quota.

    Not ``os.cpu_count()``, which reports the host -- a pod limited to two CPUs
    on a 64-core node would size a 64-worker pool and be throttled into the
    ground. A fractional quota rounds up, because 1.5 CPUs of DuckDB threads
    still use the half.
    """
    cpus = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count()
    quota = cgroup_cpu_limit()
    if quota is not None:
        cpus = min(cpus or 1, math.ceil(quota))
    return max(1, cpus or 1)


@dataclass(frozen=True)
class PoolSize:
    workers: int
    #: DuckDB threads per worker, so N workers share the CPUs rather than each
    #: assuming all of them.
    threads_per_worker: int
    #: Which constraint decided ``workers``, for the startup log.
    reason: str


def size_pool(
    *,
    cpus: int,
    memory_limit: int | None,
    memory_budget_bytes: int,
    requested: int | None = None,
) -> PoolSize:
    """How many workers fit, and how many DuckDB threads each gets.

    Memory is the binding constraint, not CPU (§3): each worker holds its own
    DuckDB connection and its own ``memory_budget_bytes`` group, so the
    footprint is ``workers x (WORKER_BASELINE_BYTES + memory_budget_bytes)``,
    planned against ``MEMORY_HEADROOM`` of the limit. ``requested`` can lower
    the answer and never raise it past what fits.
    """
    per_worker = WORKER_BASELINE_BYTES + memory_budget_bytes
    by_cpu = max(1, cpus)
    by_memory = (
        max(1, int(memory_limit * MEMORY_HEADROOM) // per_worker)
        if memory_limit is not None
        else None
    )
    workers, reason = by_cpu, f"{cpus} CPU(s)"
    if by_memory is not None and by_memory < workers:
        workers = by_memory
        reason = f"memory: {memory_limit} bytes x {MEMORY_HEADROOM} / {per_worker} bytes per worker"
    if requested is not None and requested < workers:
        workers, reason = max(1, requested), "requested"
    elif requested is not None and requested > workers:
        logger.warning(
            "%d workers requested; %d fit (%s). Using %d.", requested, workers, reason, workers
        )
    return PoolSize(workers, max(1, cpus // workers), reason)


# -- the worker -----------------------------------------------------------------


@dataclass(frozen=True)
class WorkerConfig:
    """What every worker shares. Nothing here is a live object."""

    #: The operator profile and env file, as the parent resolved them, passed
    #: through as paths -- the worker reads them itself.
    profile_path: str | None = None
    env_path: str | None = None
    #: The service's ``--yes``. **False previews**, as everywhere else.
    commit: bool = False
    threads: int = 1
    #: The environment a worker restores before every table. Must be taken
    #: *before* anything loads ``.env`` into ``os.environ``: ``load_env`` lets
    #: the real environment win, so a worker inheriting already-loaded keys
    #: would never see a credential rotated in ``.env``. Restoring it per table
    #: makes every table read ``.env`` fresh, exactly as a cron run would.
    base_environ: Mapping[str, str] = field(default_factory=lambda: dict(os.environ))
    #: ``module:function`` taking an argv and returning an exit code. The CLI;
    #: replaceable so a test can make a worker crash on cue.
    entry: str = "zamboni.cli:main"
    #: Where per-table config and run-record files go.
    workdir: Path = field(default_factory=lambda: Path.cwd())


def worker_argv(
    item: WorkItem, config: WorkerConfig, table_config: Path, record: Path
) -> list[str]:
    """The ``zamboni`` command line a worker runs for ``item``. Strings only."""
    argv = [
        "maintenance",
        item.table,
        "--warehouse",
        item.warehouse,
        "--table-config",
        str(table_config),
        "--json",
        str(record),
        "--threads",
        str(config.threads),
    ]
    if item.uri:
        argv += ["--uri", item.uri]
    if config.profile_path:
        argv += ["--profile", config.profile_path]
    if config.env_path:
        argv += ["--env", config.env_path]
    if config.commit:
        argv.append("--yes")
    return argv


def _worker_main(conn: connection.Connection, base_environ: dict[str, str], entry: str) -> None:
    """A worker process: run jobs until told to stop or the parent goes away."""
    import importlib

    module, _, name = entry.partition(":")
    run = getattr(importlib.import_module(module), name)
    # Logging to whatever `sys.stderr` is *now*, so the per-table capture below
    # catches it. `cli.main`'s own basicConfig is then a no-op.
    logging.basicConfig(level=logging.INFO, handlers=[_CurrentStderr()], force=True)

    while True:
        try:
            job = conn.recv()
        except EOFError:
            return
        if job is None:
            return
        argv, record_path = job
        os.environ.clear()
        os.environ.update(base_environ)
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            try:
                code = run(argv)
            except SystemExit as exc:  # argparse's `parser.error` is exit 2
                code = exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)
            except BaseException:  # reported, not swallowed
                traceback.print_exc()
                code = 1
        conn.send((int(code or 0), out.getvalue(), _read_record(Path(record_path))))


class _CurrentStderr(logging.StreamHandler):
    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        self.stream = sys.stderr
        super().emit(record)


def _read_record(path: Path) -> dict[str, Any] | None:
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return None
    return json.loads(lines[-1]) if lines else None


@dataclass(frozen=True)
class ItemResult:
    item: WorkItem
    #: The CLI's exit code for this table -- the same number a cron run of
    #: ``zamboni maintenance <table>`` would exit with -- or 1 if the worker
    #: died, which is Python's own code for an uncaught crash.
    exit_code: int
    #: The ``--json`` run record, or ``None`` if the worker died first.
    record: dict[str, Any] | None
    #: What the run printed, stdout and stderr together.
    output: str
    #: Set only when the worker process itself died.
    died: str | None
    seconds: float


class _Slot:
    def __init__(self) -> None:
        self.process: Any = None
        self.conn: connection.Connection | None = None
        self.tables = 0


class WorkerPool:
    """``size.workers`` long-lived worker processes."""

    def __init__(
        self,
        size: PoolSize,
        config: WorkerConfig,
        *,
        recycle_after: int = RECYCLE_AFTER_TABLES,
    ) -> None:
        self.size = size
        self.config = config
        self.recycle_after = recycle_after
        # Spawn, never fork: see the module docstring on native threads.
        self._context = get_context("spawn")
        self._slots = [_Slot() for _ in range(size.workers)]
        self._idle: asyncio.Queue[_Slot] = asyncio.Queue()
        for slot in self._slots:
            self._idle.put_nowait(slot)

    async def run(self, item: WorkItem) -> ItemResult:
        """Maintain one table on the next free worker."""
        slot = await self._idle.get()
        try:
            return await asyncio.to_thread(self._run_blocking, slot, item)
        finally:
            self._idle.put_nowait(slot)

    def _run_blocking(self, slot: _Slot, item: WorkItem) -> ItemResult:
        started = time.monotonic()
        job_id = uuid.uuid4().hex
        table_config = self.config.workdir / f"zamboni-{job_id}.table-config.json"
        record = self.config.workdir / f"zamboni-{job_id}.run.jsonl"
        # The item's own config, frozen when it was taken: a reload that edits
        # the file it came from cannot reach a table already in flight.
        item.table_config.dump(table_config)
        try:
            if slot.process is None:
                self._spawn(slot)
            assert slot.conn is not None
            slot.conn.send((worker_argv(item, self.config, table_config, record), str(record)))
            ready = connection.wait([slot.conn, slot.process.sentinel])
            if slot.conn in ready:
                try:
                    code, output, run_record = slot.conn.recv()
                except EOFError:
                    return self._died(slot, item, started)
                slot.tables += 1
                if slot.tables >= self.recycle_after:
                    self._retire(slot)
                return ItemResult(item, code, run_record, output, None, time.monotonic() - started)
            return self._died(slot, item, started)
        finally:
            table_config.unlink(missing_ok=True)
            record.unlink(missing_ok=True)

    def _spawn(self, slot: _Slot) -> None:
        parent, child = self._context.Pipe()
        process = self._context.Process(
            target=_worker_main,
            args=(child, dict(self.config.base_environ), self.config.entry),
            name="zamboni-worker",
            daemon=True,
        )
        process.start()
        child.close()
        slot.process, slot.conn, slot.tables = process, parent, 0

    def _died(self, slot: _Slot, item: WorkItem, started: float) -> ItemResult:
        slot.process.join(timeout=5)
        code = slot.process.exitcode
        why = f"killed by signal {-code}" if code is not None and code < 0 else f"exit {code}"
        logger.error("worker for %s/%s died (%s)", item.warehouse, item.table, why)
        self._reset(slot)
        return ItemResult(item, 1, None, "", f"worker died ({why})", time.monotonic() - started)

    def _retire(self, slot: _Slot) -> None:
        with contextlib.suppress(OSError):
            assert slot.conn is not None
            slot.conn.send(None)
        slot.process.join(timeout=30)
        if slot.process.is_alive():
            slot.process.terminate()
            slot.process.join()
        self._reset(slot)

    @staticmethod
    def _reset(slot: _Slot) -> None:
        if slot.conn is not None:
            slot.conn.close()
        slot.process, slot.conn, slot.tables = None, None, 0

    async def close(self) -> None:
        """Retire every worker. Call when nothing is in flight."""
        for slot in self._slots:
            if slot.process is not None:
                await asyncio.to_thread(self._retire, slot)


# -- feeding it -------------------------------------------------------------------


async def run_workers(
    queue: CandidateQueue,
    fleet: Callable[[], FleetConfig],
    pool: Any,
    stop: asyncio.Event,
    *,
    on_result: Callable[[ItemResult], None] | None = None,
) -> None:
    """Take from ``queue`` into ``pool`` until ``stop``; then let in-flight tables finish.

    **Takes only when a worker is free.** Taking eagerly would empty the queue
    into tasks waiting for a worker -- and an item out of the queue no longer
    coalesces with a later offer of the same table, and has already been
    resolved against a fleet a reload may since have changed. Left in the queue
    until a worker can start it, it keeps both properties.

    Every outcome is per table and independent: a table's result -- including a
    worker dying under it -- reaches ``on_result`` and the loop carries on.
    ``queue.finish`` runs however a table ends, or a table whose worker died
    would be refused by the queue forever.
    """
    free = asyncio.Semaphore(pool.size.workers)
    running: set[asyncio.Task[None]] = set()

    async def one(item: WorkItem) -> None:
        try:
            try:
                result = await pool.run(item)
            except Exception as exc:  # one table's failure, not the loop's
                logger.exception("running %s/%s", item.warehouse, item.table)
                result = ItemResult(item, 1, None, "", f"pool error: {exc!r}", 0.0)
            if on_result is not None:
                try:
                    on_result(result)
                except Exception:  # a reporting hook never stops maintenance
                    logger.exception("on_result for %s/%s", item.warehouse, item.table)
        finally:
            queue.finish(item)
            free.release()

    while not stop.is_set():
        # Waiting for a worker is raced against stop too. A loop blocked here
        # without watching stop takes the next table the moment a worker frees
        # up -- after stop -- which is how an earlier draft dropped one.
        if not await _unless_stopped(free.acquire(), stop):
            break
        work = await _unless_stopped(queue.get(fleet), stop)
        if work is None or stop.is_set():
            free.release()
            if work is not None:
                # Taken in the same instant stop arrived. Put back rather than
                # dropped, so a stop never loses a table from the queue.
                queue.finish(work)
                queue.offer(Candidate(work.warehouse, work.table, work.reason))
            break
        task = asyncio.create_task(one(work))
        running.add(task)
        task.add_done_callback(running.discard)

    if running:
        await asyncio.gather(*running)


async def _unless_stopped(awaitable: Any, stop: asyncio.Event) -> Any:
    """``awaitable``'s result, or ``None`` if ``stop`` is set first.

    When both finish together the result wins, so an acquired semaphore or a
    taken item is never silently lost; the caller checks ``stop`` again.
    """
    task = asyncio.ensure_future(awaitable)
    stopping = asyncio.ensure_future(stop.wait())
    await asyncio.wait({task, stopping}, return_when=asyncio.FIRST_COMPLETED)
    stopping.cancel()
    if task.done():
        return task.result()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    # Cancellation can lose the race with completion; honour the completion.
    return None if task.cancelled() else task.result()
