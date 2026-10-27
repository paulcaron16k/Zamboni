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

**A worker runs an entry** (docs/maintenance-runtime.md §7): a
``module:function`` taking the :class:`WorkItem` and a :class:`WorkerContext`
and returning the run record. What crosses the process boundary is the pickled
item -- config, never a ``CatalogSession`` and never a secret -- and the record
coming back. The default entry, :func:`run_table`, runs the CLI's own
``maintenance`` path for the one table, in-process: the resolution order (flag >
env > profile), the session, the engine, the reporter, the exit codes and the
``--json`` record are all the code a cron line already runs, so the service
cannot drift from it, and the worker reads ``.env`` and the profile itself. An
application supplies its own entry to build its catalog session its own way.

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
import dataclasses
import io
import json
import logging
import math
import os
import signal
import sys
import time
import traceback
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from multiprocessing import connection, get_context
from pathlib import Path
from typing import Any

import psutil

from .claims import Claim, Claimer
from .fleet import FleetConfig
from .scheduler import CandidateQueue, PressureCheck, WorkItem

# The parent's end of a worker's pipe. Windows' Pipe() returns a
# PipeConnection, a sibling of Connection rather than a subclass, so
# `mypy --platform win32` needs the alias (ZMBNI-236).
if sys.platform == "win32":
    from multiprocessing.connection import PipeConnection as WorkerConnection
else:
    from multiprocessing.connection import Connection as WorkerConnection

logger = logging.getLogger(__name__)

#: What a worker runs: a table, or a warehouse's pressure check (#175).
Job = WorkItem | PressureCheck

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

#: The least DuckDB is given when the plan does not fit -- one worker forced
#: onto a limit smaller than its own baseline. **Chosen, not measured**: enough
#: for DuckDB to start and spill, and a warning says the plan did not fit.
DUCKDB_MIN_MEMORY_BYTES = 64 * 1024 * 1024

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
            quota, _, period = v2.read_text(encoding="utf-8").strip().partition(" ")
            if quota != "max" and period:
                limits.append(int(quota) / int(period))
            continue
        quota_file, period_file = directory / "cpu.cfs_quota_us", directory / "cpu.cfs_period_us"
        if quota_file.is_file() and period_file.is_file():
            quota_us = int(quota_file.read_text(encoding="utf-8"))
            if quota_us > 0:
                limits.append(quota_us / int(period_file.read_text(encoding="utf-8")))
    return min(limits) if limits else None


def cgroup_memory_limit(root: Path = CGROUP_ROOT, proc: Path = PROC_CGROUP) -> int | None:
    """Bytes the cgroup allows, or ``None``. The smallest up the tree, as for CPU."""
    limits = []
    for directory in _cgroup_dirs(root, proc, v1_controller="memory"):
        for name in ("memory.max", "memory.limit_in_bytes"):
            path = directory / name
            if path.is_file():
                text = path.read_text(encoding="utf-8").strip()
                if text != "max" and int(text) < _V1_UNLIMITED:
                    limits.append(int(text))
                break
    return min(limits) if limits else None


def _cgroup_dirs(root: Path, proc: Path, *, v1_controller: str) -> list[Path]:
    """The process's cgroup directories, leaf first, for one controller."""
    try:
        lines = proc.read_text(encoding="utf-8").splitlines()
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


def available_cpus(root: Path = CGROUP_ROOT, proc: Path = PROC_CGROUP) -> int:
    """Whole CPUs this process may use: affinity, then the cgroup quota.

    Not ``os.cpu_count()``, which reports the host -- a pod limited to two CPUs
    on a 64-core node would size a 64-worker pool and be throttled into the
    ground. A fractional quota rounds up, because 1.5 CPUs of DuckDB threads
    still use the half.
    """
    cpus = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count()
    quota = cgroup_cpu_limit(root, proc)
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
    #: DuckDB's ``memory_limit`` per worker: the worker's share of the planned
    #: memory, less what the process holds before DuckDB allocates anything.
    #: ``None`` when no memory figure was known, which leaves DuckDB its own
    #: default (see ``CatalogSession.memory_limit_bytes`` for why that is not
    #: safe with several workers).
    duckdb_memory_bytes: int | None = None


def size_pool(
    *,
    cpus: int,
    memory_limit: int | None,
    memory_budget_bytes: int,
    requested: int | None = None,
    reserved_memory_bytes: int = 0,
    reserved_cpus: int = 0,
) -> PoolSize:
    """How many workers fit, and how many DuckDB threads each gets.

    ``reserved_memory_bytes`` and ``reserved_cpus`` are what the host process
    keeps for itself (docs/maintenance-runtime.md §9.4): the runtime's own
    baseline, and in single-process mode the web server too. They come off the
    top, and the pool plans against the rest. A reservation that leaves too
    little for one worker still gets one, with the warning a plan that does not
    fit always gives.

    Memory is the binding constraint, not CPU (§3): each worker holds its own
    DuckDB connection and its own ``memory_budget_bytes`` group, so the
    footprint is ``workers x (WORKER_BASELINE_BYTES + memory_budget_bytes)``,
    planned against ``MEMORY_HEADROOM`` of the limit. ``requested`` can lower
    the answer and never raise it past what fits.

    The same arithmetic gives each worker's DuckDB limit (ZMBNI-147), so the
    plan is enforced where the memory is actually allocated rather than only
    assumed: ``MEMORY_HEADROOM x memory_limit / workers - WORKER_BASELINE_BYTES``.
    """
    if reserved_memory_bytes < 0 or reserved_cpus < 0:
        raise ValueError(
            f"reservations cannot be negative: {reserved_memory_bytes} bytes, {reserved_cpus} CPUs"
        )
    cpus = max(1, cpus - reserved_cpus)
    if memory_limit is not None:
        memory_limit = max(0, memory_limit - reserved_memory_bytes)
    per_worker = WORKER_BASELINE_BYTES + memory_budget_bytes
    by_cpu = cpus
    by_memory = (
        max(1, int(memory_limit * MEMORY_HEADROOM) // per_worker)
        if memory_limit is not None
        else None
    )
    workers, reason = (
        by_cpu,
        f"{cpus} CPU(s)" + (f" after {reserved_cpus} reserved" if reserved_cpus else ""),
    )
    if by_memory is not None and by_memory < workers:
        workers = by_memory
        reason = f"memory: {memory_limit} bytes x {MEMORY_HEADROOM} / {per_worker} bytes per worker"
        if reserved_memory_bytes:
            reason += f", after {reserved_memory_bytes} bytes reserved"
    if requested is not None and requested < workers:
        workers, reason = max(1, requested), "requested"
    elif requested is not None and requested > workers:
        logger.warning(
            "%d workers requested; %d fit (%s). Using %d.", requested, workers, reason, workers
        )
    duckdb = None
    if memory_limit is not None:
        duckdb = int(memory_limit * MEMORY_HEADROOM) // workers - WORKER_BASELINE_BYTES
        if duckdb < DUCKDB_MIN_MEMORY_BYTES:
            logger.warning(
                "the memory plan does not fit: %d bytes x %s over %d worker(s) leaves "
                "%d bytes for DuckDB after a %d-byte baseline; giving it %d",
                memory_limit,
                MEMORY_HEADROOM,
                workers,
                duckdb,
                WORKER_BASELINE_BYTES,
                DUCKDB_MIN_MEMORY_BYTES,
            )
            duckdb = DUCKDB_MIN_MEMORY_BYTES
    return PoolSize(workers, max(1, cpus // workers), reason, duckdb)


def physical_memory() -> int | None:
    """Host RAM, where the platform says.

    From psutil rather than ``os.sysconf``, which Windows lacks (ZMBNI-236).
    On macOS both gave 34359738368 (measured 2026-10-27). On Linux psutil reads
    ``/proc/meminfo``'s ``MemTotal`` (``_pslinux.virtual_memory``, read); that
    the kernel fills it from the same ``totalram`` glibc's ``_SC_PHYS_PAGES``
    reports is recalled from those sources, not re-read.
    """
    try:
        return int(psutil.virtual_memory().total)
    except (OSError, RuntimeError):
        return None


def plan_pool(
    *,
    memory_budget_bytes: int,
    requested: int | None = None,
    reserved_memory_bytes: int = 0,
    reserved_cpus: int = 0,
    root: Path = CGROUP_ROOT,
    proc: Path = PROC_CGROUP,
) -> PoolSize:
    """Size the pool for *this* process: its CPUs, and the memory it may use.

    Memory is the cgroup's limit where there is one, else host RAM. Planning
    against host RAM rather than declining to plan is the safe direction: with
    no figure, every worker's DuckDB would assume 80% of the host for itself.
    The reservations are :func:`size_pool`'s. ``root`` and ``proc`` locate the
    cgroup tree, for a test.
    """
    return size_pool(
        cpus=available_cpus(root, proc),
        memory_limit=cgroup_memory_limit(root, proc) or physical_memory(),
        memory_budget_bytes=memory_budget_bytes,
        requested=requested,
        reserved_memory_bytes=reserved_memory_bytes,
        reserved_cpus=reserved_cpus,
    )


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
    #: DuckDB's memory limit per worker; ``None`` leaves DuckDB's default.
    duckdb_memory_bytes: int | None = None
    #: The environment a worker restores before every table. Must be taken
    #: *before* anything loads ``.env`` into ``os.environ``: ``load_env`` lets
    #: the real environment win, so a worker inheriting already-loaded keys
    #: would never see a credential rotated in ``.env``. Restoring it per table
    #: makes every table read ``.env`` fresh, exactly as a cron run would.
    #:
    #: Out of the repr: this is a copy of the process environment, so in a
    #: container it holds every secret the Secret injected. Nothing reprs a
    #: WorkerConfig today, but a ``%r``, a traceback that prints locals or a
    #: debug print would have published all of them (ZMBNI-194).
    base_environ: Mapping[str, str] = field(default_factory=lambda: dict(os.environ), repr=False)
    #: The worker entry: ``module:function`` taking a :class:`WorkItem` and a
    #: :class:`WorkerContext` and returning the run record. Importable by name
    #: in a spawned interpreter. The default runs the CLI (:func:`run_table`).
    entry: str = "zamboni.pool:run_table"
    #: Where per-table config and run-record files go.
    workdir: Path = field(default_factory=lambda: Path.cwd())

    def __post_init__(self) -> None:
        # Accept a str, refuse anything else, here rather than as a TypeError
        # at the first table, which is where a str used to fail (#155).
        if isinstance(self.workdir, str):
            object.__setattr__(self, "workdir", Path(self.workdir))
        if not isinstance(self.workdir, Path):
            raise TypeError(f"workdir must be a path, not {type(self.workdir).__name__}")

    @classmethod
    def from_pool(cls, size: PoolSize, **settings: Any) -> WorkerConfig:
        """A config carrying ``size``'s per-worker threads and DuckDB memory.

        The way to build one for a sized pool, so the numbers the sizing chose
        are the numbers the workers get -- not restated beside it.
        """
        return cls(
            threads=size.threads_per_worker,
            duckdb_memory_bytes=size.duckdb_memory_bytes,
            **settings,
        )

    def context(self, report: Callable[[str], None] | None = None) -> WorkerContext:
        """What a worker hands its entry with each table; ``report`` carries progress."""
        return WorkerContext(
            threads=self.threads,
            duckdb_memory_bytes=self.duckdb_memory_bytes,
            workdir=self.workdir,
            profile_path=self.profile_path,
            env_path=self.env_path,
            base_environ=self.base_environ,
            report=report or _ignore,
        )


def _ignore(note: str) -> None:
    pass


@dataclass(frozen=True)
class WorkerContext:
    """What a worker entry is given besides its :class:`WorkItem` (§7).

    Built in the worker, never sent: it carries the environment, and ``report``
    is the worker's end of the pipe.
    """

    #: DuckDB threads for the session the entry builds -- this worker's share
    #: of the CPUs, so N workers do not each assume all of them.
    threads: int
    #: DuckDB's memory limit for that session; ``None`` leaves DuckDB its default.
    duckdb_memory_bytes: int | None
    #: Where an entry may put per-table scratch files.
    workdir: Path
    #: The operator profile and env file, for an entry that resolves its
    #: settings as the CLI does. The default entry passes them on.
    profile_path: str | None = None
    env_path: str | None = None
    #: The environment the worker restored before this table. Out of the repr,
    #: as on :class:`WorkerConfig`: it holds every secret the process was given.
    base_environ: Mapping[str, str] = field(default_factory=dict, repr=False)
    report: Callable[[str], None] = field(default=_ignore, repr=False, compare=False)

    def progress(self, note: str) -> None:
        """Tell the runtime this table is still moving, and where it is.

        Call at every operation boundary at least: a long table that reports
        nothing cannot be told from a hung one (§7.1).
        """
        self.report(str(note))


def worker_argv(item: Job, context: WorkerContext, table_config: Path, record: Path) -> list[str]:
    """The ``zamboni`` command line the default entry runs for ``item``. Strings only.

    A table is ``maintenance <table>``, with ``--yes`` only when the item says
    ``commit``; a :class:`PressureCheck` is ``health`` over every table in the
    warehouse's config -- read-only, so never ``--yes``.
    """
    if isinstance(item, PressureCheck):
        argv = [
            "health",
            "--warehouse",
            item.warehouse,
            "--table-config",
            str(table_config),
            "--json",
            str(record),
        ]
    else:
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
            str(context.threads),
        ]
        # Every merged reason's operations. Each reason is looked up, never
        # `.get`: one missing from the table would read as None, which means
        # every operation, reclaim included (ZMBNI-195).
        if (operations := item.operations) is not None:
            argv += ["--operations", ",".join(operations)]
        if (rule := item.interval_rule) is not None:
            interval, anyway = rule
            argv += ["--min-interval-hours", str(interval / timedelta(hours=1))]
            if anyway:
                argv += ["--due-anyway", ",".join(anyway)]
        if context.duckdb_memory_bytes is not None:
            argv += ["--duckdb-memory-limit-bytes", str(context.duckdb_memory_bytes)]
    if item.uri:
        argv += ["--uri", item.uri]
    if context.profile_path:
        argv += ["--profile", context.profile_path]
    if context.env_path:
        argv += ["--env", context.env_path]
    if isinstance(item, WorkItem) and item.commit:
        argv.append("--yes")
    return argv


def run_table(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """The default worker entry: ``zamboni maintenance <table>``, in-process.

    The command line a cron run of the same table would be, run by the CLI's
    own ``main`` -- so ``serve`` and cron cannot drift -- and its ``--json``
    record returned.
    """
    return run_cli(item, context)


def run_cli(
    item: WorkItem, context: WorkerContext, *, main: Callable[[list[str]], int] | None = None
) -> dict[str, Any]:
    """Run ``item`` through the command line ``main`` parses, and return its record.

    A run that wrote no record -- a usage error, which writes none -- raises
    ``SystemExit`` with its code, which the worker reports as that exit code
    with no record, as a cron run of the same line would. ``main`` is the
    CLI's; a test passes a stand-in that crashes on cue.
    """
    context.progress(f"maintenance {item.table}")
    code, record = _run_argv(item, context, main)
    if record is None:
        raise SystemExit(code)
    return record


def _run_argv(
    item: Job, context: WorkerContext, main: Callable[[list[str]], int] | None
) -> tuple[int, dict[str, Any] | None]:
    """``main(worker_argv(item, ...))``: its exit code and the record it wrote."""
    if main is None:
        from .cli import main as cli_main

        main = cli_main
    job_id = uuid.uuid4().hex
    table_config = context.workdir / f"zamboni-{job_id}.table-config.json"
    record = context.workdir / f"zamboni-{job_id}.run.jsonl"
    # The item's own config, frozen when it was taken: a reload that edits
    # the file it came from cannot reach a table already in flight.
    item.table_config.dump(table_config)
    try:
        try:
            code = main(worker_argv(item, context, table_config, record))
        except SystemExit as exc:  # argparse's `parser.error` is exit 2
            code = _exit_code(exc)
        return int(code or 0), _read_record(record)
    finally:
        table_config.unlink(missing_ok=True)
        record.unlink(missing_ok=True)


def _exit_code(exc: SystemExit) -> int:
    return exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)


def _run_job(job: Job, run: Callable[..., Any], context: WorkerContext) -> tuple[int, Any]:
    """One job's exit code and run record, however the entry ends.

    A table goes to the entry, whose record carries its own ``exit_code``. A
    pressure check is always ``health`` through the CLI: it is the runtime's
    own measurement, not maintenance an application builds a session for.
    """
    if isinstance(job, PressureCheck):
        return _run_argv(job, context, None)
    try:
        record = run(job, context)
    except SystemExit as exc:
        return _exit_code(exc), None
    except BaseException:  # reported, not swallowed
        traceback.print_exc()
        return 1, None
    code = record.get("exit_code") if isinstance(record, dict) else None
    if not isinstance(code, int) or isinstance(code, bool):
        # Not the record `maintenance --json` writes, so not a result the run
        # log can read: a broken entry, reported as a failure, never a success.
        print(f"the worker entry returned no run record with an exit_code: {record!r:.200}")
        return 1, None
    return code, record


def _worker_main(conn: WorkerConnection, config: WorkerConfig) -> None:
    """A worker process: run jobs until told to stop or the parent goes away."""
    import importlib

    # The parent coordinates shutdown. systemd's default KillMode=control-group
    # sends SIGTERM to every process in the unit, and Ctrl-C sends SIGINT to the
    # whole foreground process group -- either would kill a table mid-rewrite
    # instead of letting the service drain. So a worker ignores both, and the
    # parent retires it, or SIGKILLs it on a forced stop.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    module, _, name = config.entry.partition(":")
    run = getattr(importlib.import_module(module), name)
    context = config.context(report=lambda note: conn.send(("progress", note)))
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
        os.environ.clear()
        os.environ.update(config.base_environ)
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            try:
                code, record = _run_job(job, run, context)
            except BaseException:  # the pressure path's own failure
                traceback.print_exc()
                code, record = 1, None
        conn.send(("done", code, out.getvalue(), record))


class _CurrentStderr(logging.StreamHandler):
    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        self.stream = sys.stderr
        super().emit(record)


def _read_record(path: Path) -> dict[str, Any] | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    return json.loads(lines[-1]) if lines else None


@dataclass(frozen=True)
class ItemResult:
    item: Job
    #: The CLI's exit code for this table -- the same number a cron run of
    #: ``zamboni maintenance <table>`` would exit with -- or 1 if the worker
    #: died, which is Python's own code for an uncaught crash.
    exit_code: int
    #: The ``--json`` run record, or ``None`` if the worker died first.
    record: dict[str, Any] | None
    #: What the run printed, stdout and stderr together.
    output: str
    #: Why the table failed without a run record: its worker died -- killed for
    #: a lost claim among the ways -- or the pool or the claim store failed.
    died: str | None
    seconds: float
    #: Why the table was never started, with exit 0: ``"deadline"`` when its
    #: ``not_after`` had passed by the time a worker was free (§3.3);
    #: ``"claimed elsewhere"`` when another holder had its claim (§6). ``None``
    #: for a table that ran.
    dropped: str | None = None
    #: The last ``context.progress`` note the entry sent, if any.
    progress: str | None = None


class _Slot:
    def __init__(self) -> None:
        self.process: Any = None
        self.conn: WorkerConnection | None = None
        self.tables = 0
        #: What the worker is running now, so one table's worker can be killed.
        self.job: Job | None = None
        #: ``job`` must not run: :meth:`WorkerPool.kill_table` was called for it.
        self.doomed = False


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

    async def run(self, item: Job) -> ItemResult:
        """Maintain one table on the next free worker."""
        slot = await self._idle.get()
        slot.job = item
        try:
            return await asyncio.to_thread(self._run_blocking, slot, item)
        finally:
            slot.job, slot.doomed = None, False
            self._idle.put_nowait(slot)

    def _run_blocking(self, slot: _Slot, item: Job) -> ItemResult:
        started = time.monotonic()
        # The runtime's consent travels on the item, which is what an entry
        # honours (§7 rule 4). Stamped on the copy sent; `item` itself is the
        # queue's, and is what the result and `kill_table` are matched against.
        job = (
            dataclasses.replace(item, commit=self.config.commit)
            if isinstance(item, WorkItem)
            else item
        )
        progress: str | None = None
        if slot.process is not None and not slot.process.is_alive():
            # Died between tables -- killed, OOM, crashed after its last
            # reply. That is not this table's failure: replace the worker
            # and run the table, rather than sending it into a dead pipe.
            self._reset(slot)
        if slot.process is None:
            self._spawn(slot)
        assert slot.conn is not None
        if slot.doomed:
            # Killed before it was sent: the kill may have landed on the
            # previous process, which the check above then replaced.
            return ItemResult(item, 1, None, "", "worker killed before the table started", 0.0)
        # Any failure on the pipe is the worker dying, whichever call sees
        # it. A worker that dies during startup surfaces as
        # ConnectionResetError, not EOFError, because it exits with our job
        # unread in its socket buffer; catching EOFError alone left the slot
        # holding a dead process and failed every later table on it (#155).
        try:
            slot.conn.send(job)
            while True:
                ready = connection.wait([slot.conn, slot.process.sentinel])
                if slot.conn not in ready:
                    return self._died(slot, item, started, progress)
                message = slot.conn.recv()
                if message[0] != "progress":
                    break
                progress = message[1]
            _, code, output, run_record = message
        except (OSError, EOFError):
            return self._died(slot, item, started, progress)
        slot.tables += 1
        if slot.tables >= self.recycle_after:
            self._retire(slot)
        return ItemResult(
            item, code, run_record, output, None, time.monotonic() - started, progress=progress
        )

    def _spawn(self, slot: _Slot) -> None:
        parent, child = self._context.Pipe()
        process = self._context.Process(
            target=_worker_main,
            args=(
                child,
                dataclasses.replace(self.config, base_environ=dict(self.config.base_environ)),
            ),
            name="zamboni-worker",
            daemon=True,
        )
        process.start()
        child.close()
        slot.process, slot.conn, slot.tables = process, parent, 0

    def _died(self, slot: _Slot, item: Job, started: float, progress: str | None) -> ItemResult:
        slot.process.join(timeout=5)
        if slot.process.is_alive():
            # The pipe failed but the process did not exit: it can no longer be
            # talked to, so it is dead to us. Kill it rather than leak it.
            slot.process.kill()
            slot.process.join()
        code = slot.process.exitcode
        why = f"killed by signal {-code}" if code is not None and code < 0 else f"exit {code}"
        logger.error("worker for %s/%s died (%s)", item.warehouse, item.table, why)
        self._reset(slot)
        return ItemResult(
            item, 1, None, "", f"worker died ({why})", time.monotonic() - started, progress=progress
        )

    def _retire(self, slot: _Slot) -> None:
        with contextlib.suppress(OSError):
            assert slot.conn is not None
            slot.conn.send(None)
        slot.process.join(timeout=30)
        if slot.process.is_alive():
            # kill, not terminate: a worker ignores SIGTERM (see _worker_main).
            slot.process.kill()
            slot.process.join()
        self._reset(slot)

    @staticmethod
    def _reset(slot: _Slot) -> None:
        if slot.conn is not None:
            slot.conn.close()
        slot.process, slot.conn, slot.tables = None, None, 0

    def kill(self) -> None:
        """SIGKILL every live worker: a forced stop. Their tables report as died."""
        for slot in self._slots:
            # Read once: the slot's thread may replace or clear it meanwhile.
            process = slot.process
            if process is not None and process.is_alive():
                process.kill()

    def kill_table(self, item: Job) -> bool:
        """Stop ``item`` for good: it reports as died. ``False`` if it was not running.

        Its worker is SIGKILLed, and the slot is marked so that ``item`` is
        never sent to a worker if the kill landed first -- otherwise a slot
        replacing a worker it found dead would run the table after all.
        Marked before the kill, and checked after the worker is spawned, so
        one of the two always catches it.
        """
        for slot in self._slots:
            if slot.job is item:
                slot.doomed = True
                # Read once: the slot's thread may replace or clear it meanwhile.
                process = slot.process
                if process is not None and process.is_alive():
                    process.kill()
                return True
        return False

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
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    on_start: Callable[[WorkItem, Claim | None], None] | None = None,
    claims: Claimer | None = None,
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

    **A table is never started after its deadline** (docs/maintenance-runtime.md
    §3.3). It is checked with a worker already free, the last moment before
    the table would start, and reported with ``dropped="deadline"`` and exit 0:
    a window closing is not a failure.

    **With ``claims``, a table starts only once its claim is acquired** (§6),
    and the claim is held, renewed and released around its run. One held by
    another replica drops the table as ``"claimed elsewhere"``, exit 0; a claim
    store that cannot answer fails it, exit 1, because an unclaimed table must
    not start. A claim lost while the table runs kills its worker
    (``pool.kill_table``), and the result says why.
    """
    free = asyncio.Semaphore(pool.size.workers)
    running: set[asyncio.Task[None]] = set()

    async def start(item: WorkItem, claim: Claim | None) -> ItemResult:
        if on_start is not None:
            try:
                on_start(item, claim)
            except Exception:  # a reporting hook never stops maintenance
                logger.exception("on_start for %s/%s", item.warehouse, item.table)
        result: ItemResult = await pool.run(item)
        return result

    async def claimed(item: WorkItem, claims: Claimer) -> ItemResult:
        try:
            holding = await claims.acquire(item.warehouse, item.table, reason=item.reason)
        except Exception as exc:
            logger.exception("claiming %s/%s", item.warehouse, item.table)
            return ItemResult(item, 1, None, "", f"claim store error: {exc!r}", 0.0)
        if holding is None:
            logger.info("not starting %s/%s: claimed elsewhere", item.warehouse, item.table)
            return ItemResult(item, 0, None, "", None, 0.0, dropped="claimed elsewhere")
        async with claims.kept(holding, kill=lambda: pool.kill_table(item)):
            result = await start(item, holding.claim)
        if holding.lost is not None and result.died is not None:
            result = dataclasses.replace(
                result, died=f"claim lost ({holding.lost}); worker killed: {result.died}"
            )
        return result

    async def one(item: WorkItem) -> None:
        try:
            try:
                if item.not_after is not None and clock() > item.not_after:
                    logger.info(
                        "not starting %s/%s: its deadline %s has passed",
                        item.warehouse,
                        item.table,
                        item.not_after.isoformat(),
                    )
                    result = ItemResult(item, 0, None, "", None, 0.0, dropped="deadline")
                elif claims is None:
                    result = await start(item, None)
                else:
                    result = await claimed(item, claims)
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
                queue.offer(work.candidate())
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
