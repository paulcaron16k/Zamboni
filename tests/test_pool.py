# SPDX-License-Identifier: Apache-2.0
"""The worker pool: sizing from the cgroup, isolation, and feeding it.

The sizing tests read a fake cgroup tree, because the property is "the
smallest limit up the tree wins" and a real machine shows one tree. The worker
tests spawn real processes -- the claims are about process boundaries, and a
mock of a process boundary proves nothing about one -- with entry points from
`tests/pool_entries.py` that crash on cue.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse
from zamboni.pool import (
    MEMORY_HEADROOM,
    WORKER_BASELINE_BYTES,
    ItemResult,
    PoolSize,
    WorkerConfig,
    WorkerPool,
    cgroup_cpu_limit,
    cgroup_memory_limit,
    run_workers,
    size_pool,
    worker_argv,
)
from zamboni.scheduler import Candidate, CandidateQueue, WorkItem
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

MiB = 1024 * 1024


def table_config(*tables: str, namespace: str = "raw", warehouse: str = "acme") -> TableConfig:
    return TableConfig(
        warehouse=warehouse,
        namespaces={namespace: NamespaceSettings(tables={t: TableSettings() for t in tables})},
    )


def item(table: str = "raw.events", **kw) -> WorkItem:
    return WorkItem(
        warehouse="acme",
        table=table,
        reason="schedule",
        table_config=kw.pop("table_config", table_config(table.split(".", 1)[1])),
        **kw,
    )


# -- cgroups ----------------------------------------------------------------------


def cgroup_tree(
    tmp_path: Path, files: dict[str, str], *, v1: str | None = None
) -> tuple[Path, Path]:
    root = tmp_path / "cgroup"
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    proc = tmp_path / "proc-self-cgroup"
    proc.write_text(v1 or "0::/kubepods/pod1/ctr\n")
    return root, proc


def test_cgroup_v2_quota_is_the_smallest_up_the_tree(tmp_path):
    """A pod's limit sits on a parent of the container's cgroup, not on it."""
    root, proc = cgroup_tree(
        tmp_path,
        {
            "kubepods/pod1/ctr/cpu.max": "max 100000\n",
            "kubepods/pod1/cpu.max": "150000 100000\n",
            "kubepods/cpu.max": "800000 100000\n",
        },
    )
    assert cgroup_cpu_limit(root, proc) == 1.5


def test_no_quota_anywhere_is_none(tmp_path):
    root, proc = cgroup_tree(tmp_path, {"kubepods/pod1/ctr/cpu.max": "max 100000\n"})
    assert cgroup_cpu_limit(root, proc) is None
    assert cgroup_memory_limit(root, proc) is None


def test_cgroup_v2_memory_is_the_smallest_up_the_tree(tmp_path):
    root, proc = cgroup_tree(
        tmp_path,
        {"kubepods/pod1/ctr/memory.max": "max\n", "kubepods/pod1/memory.max": f"{2048 * MiB}\n"},
    )
    assert cgroup_memory_limit(root, proc) == 2048 * MiB


def test_cgroup_v1(tmp_path):
    root, proc = cgroup_tree(
        tmp_path,
        {
            "cpu,cpuacct/docker/abc/cpu.cfs_quota_us": "200000\n",
            "cpu,cpuacct/docker/abc/cpu.cfs_period_us": "100000\n",
            "memory/docker/abc/memory.limit_in_bytes": f"{512 * MiB}\n",
            "memory/memory.limit_in_bytes": "9223372036854771712\n",  # v1's "unlimited"
        },
        v1="4:memory:/docker/abc\n3:cpu,cpuacct:/docker/abc\n",
    )
    assert cgroup_cpu_limit(root, proc) == 2.0
    assert cgroup_memory_limit(root, proc) == 512 * MiB


def test_an_unreadable_proc_file_means_no_limit(tmp_path):
    assert cgroup_cpu_limit(tmp_path, tmp_path / "missing") is None


# -- sizing -------------------------------------------------------------------------


def test_memory_decides_when_it_is_the_tighter_bound():
    budget = 256 * MiB
    limit = 2048 * MiB
    size = size_pool(cpus=16, memory_limit=limit, memory_budget_bytes=budget)
    expected = int(limit * MEMORY_HEADROOM) // (WORKER_BASELINE_BYTES + budget)
    assert size.workers == expected < 16
    assert "memory" in size.reason
    assert size.threads_per_worker == 16 // expected


def test_cpu_decides_without_a_memory_limit():
    size = size_pool(cpus=4, memory_limit=None, memory_budget_bytes=256 * MiB)
    assert size == PoolSize(4, 1, "4 CPU(s)")


def test_a_request_can_lower_the_pool_and_never_raise_it():
    lower = size_pool(cpus=8, memory_limit=None, memory_budget_bytes=0, requested=2)
    assert (lower.workers, lower.threads_per_worker) == (2, 4)
    higher = size_pool(cpus=2, memory_limit=None, memory_budget_bytes=0, requested=10)
    assert higher.workers == 2


def test_there_is_always_one_worker():
    size = size_pool(cpus=1, memory_limit=64 * MiB, memory_budget_bytes=1024 * MiB)
    assert size.workers == 1 and size.threads_per_worker == 1


# -- what crosses the boundary ----------------------------------------------------


def test_a_worker_is_given_strings_and_never_a_secret(tmp_path):
    """No live session crosses: the job is an argv of strings. And no secret is
    on it -- argv is readable by any local user from `ps`."""
    config = WorkerConfig(
        profile_path="/etc/zamboni/zamboni.yml",
        env_path="/etc/zamboni/.env",
        commit=True,
        threads=3,
        base_environ={"ZAMBONI_CREDENTIAL": "client:s3cr3t"},
        workdir=tmp_path,
    )
    argv = worker_argv(
        item(uri="https://c/catalog"), config, tmp_path / "t.json", tmp_path / "r.jsonl"
    )
    assert all(isinstance(a, str) for a in argv)
    assert not any("s3cr3t" in a for a in argv)
    assert argv[:2] == ["maintenance", "raw.events"]
    for flag, value in [
        ("--warehouse", "acme"),
        ("--threads", "3"),
        ("--uri", "https://c/catalog"),
        ("--profile", "/etc/zamboni/zamboni.yml"),
        ("--env", "/etc/zamboni/.env"),
    ]:
        assert argv[argv.index(flag) + 1] == value
    assert "--yes" in argv


def test_without_commit_a_worker_previews(tmp_path):
    argv = worker_argv(item(), WorkerConfig(workdir=tmp_path), tmp_path / "t", tmp_path / "r")
    assert "--yes" not in argv


def test_the_worker_argv_parses_as_a_maintenance_command(tmp_path):
    """Built here, parsed by the CLI's own parser: they cannot disagree on a flag."""
    from zamboni.cli import _build_parser

    argv = worker_argv(
        item(uri="https://c/catalog"),
        WorkerConfig(commit=True, threads=2, workdir=tmp_path),
        tmp_path / "t.json",
        tmp_path / "r.jsonl",
    )
    args = _build_parser().parse_args(argv)
    assert (args.command, args.table, args.warehouse, args.threads, args.yes) == (
        "maintenance",
        "raw.events",
        "acme",
        2,
        True,
    )


# -- real workers ---------------------------------------------------------------------


def run_items(tmp_path, items, *, workers=1, recycle_after=50, environ=None) -> list[ItemResult]:
    config = WorkerConfig(
        entry="tests.pool_entries:record",
        workdir=tmp_path,
        base_environ=environ or {"PATH": "/usr/bin:/bin"},
    )

    async def scenario():
        pool = WorkerPool(PoolSize(workers, 1, "test"), config, recycle_after=recycle_after)
        try:
            return [await pool.run(i) for i in items]
        finally:
            await pool.close()

    return asyncio.run(scenario())


def test_a_worker_dying_takes_one_table_not_the_pool(tmp_path):
    results = run_items(tmp_path, [item("raw.a"), item("raw.die"), item("raw.b")])

    assert [r.exit_code for r in results] == [0, 1, 0]
    assert results[1].died is not None and "signal 9" in results[1].died
    assert results[1].record is None
    # The pool replaced the worker: the table after the death ran, in a new process.
    assert results[2].record["pid"] != results[0].record["pid"]


def test_a_crash_and_a_usage_error_report_their_codes(tmp_path):
    boom, usage, blocked = run_items(
        tmp_path, [item("raw.boom"), item("raw.usage"), item("raw.blocked")]
    )
    assert (boom.exit_code, boom.died) == (1, None)
    assert "RuntimeError: boom" in boom.output
    assert usage.exit_code == 2
    assert blocked.exit_code == 3, "the CLI's own exit code reaches the result unchanged"
    # One worker survived all three: none of them killed the process.
    assert boom.died is usage.died is blocked.died is None


def test_every_table_starts_from_the_environment_the_worker_was_given(tmp_path):
    """So a credential rotated in .env is read by the next table, not the next
    worker: the entry leaves a variable behind and the next table must not see it."""
    first, second = run_items(tmp_path, [item("raw.a"), item("raw.b")])
    assert first.record["pid"] == second.record["pid"], "same worker"
    assert first.record["marker"] is None
    assert second.record["marker"] is None


def test_a_worker_is_recycled_after_its_quota_of_tables(tmp_path):
    results = run_items(tmp_path, [item("raw.a"), item("raw.b"), item("raw.c")], recycle_after=2)
    pids = [r.record["pid"] for r in results]
    assert pids[0] == pids[1] != pids[2]


def test_a_worker_runs_the_items_own_table_config_and_cleans_up(tmp_path):
    config = table_config("a", "b")
    (result,) = run_items(tmp_path, [item("raw.a", table_config=config)])
    assert TableConfig.from_dict(json.loads(result.record["table_config"])).tables.keys() == {
        "raw.a",
        "raw.b",
    }
    assert result.output.strip() == "maintained raw.a"
    assert list(tmp_path.glob("zamboni-*")) == [], "per-table files are removed"


def test_the_real_cli_maintains_a_table_in_a_worker(tmp_path, monkeypatch, unpartitioned):
    """End to end: a worker runs `zamboni maintenance` against a local catalog,
    builds its own session, previews (no --yes), and returns the run record."""
    import os

    workdir = tmp_path / "work"
    workdir.mkdir()
    # A clean directory, so a developer's own zamboni.yml or .env is not read.
    monkeypatch.chdir(workdir)
    environ = {
        "PATH": os.environ.get("PATH", ""),
        "ZAMBONI_LOCAL_WAREHOUSE": str(tmp_path / "warehouse"),
    }
    config = WorkerConfig(workdir=workdir, base_environ=environ)
    work = WorkItem(
        warehouse="acme",
        table="db.unpartitioned",
        reason="schedule",
        table_config=table_config("unpartitioned", namespace="db"),
    )

    async def scenario():
        pool = WorkerPool(PoolSize(1, 1, "test"), config)
        try:
            return await pool.run(work)
        finally:
            await pool.close()

    result = asyncio.run(scenario())

    assert result.exit_code == 0, result.output
    assert result.record is not None
    assert result.record["warehouse"] == "acme"
    assert result.record["tables"] == ["db.unpartitioned"]
    assert "dry run" in result.output
    # Previewed: six small files are still six.
    assert len(list(unpartitioned.refresh().scan().plan_files())) == 6


# -- feeding the pool ---------------------------------------------------------------


class FakePool:
    """The pool's interface without processes, recording concurrency."""

    def __init__(
        self, workers: int, fail: set[str] = frozenset(), hold: asyncio.Event | None = None
    ) -> None:
        self.size = PoolSize(workers, 1, "fake")
        self.fail = fail
        #: When given, every table waits on it: a test that needs tables to be
        #: *in flight* holds them there instead of racing a sleep against them.
        self.hold = hold
        self.running = 0
        self.peak = 0
        self.started: list[str] = []

    async def run(self, work: WorkItem) -> ItemResult:
        self.running += 1
        self.peak = max(self.peak, self.running)
        self.started.append(work.table)
        try:
            if self.hold is not None:
                await self.hold.wait()
            await asyncio.sleep(0.01)
            if work.table in self.fail:
                raise RuntimeError("pool trouble")
            return ItemResult(work, 0, None, "", None, 0.01)
        finally:
            self.running -= 1


def fleet_of(*tables: str) -> FleetConfig:
    return FleetConfig(
        warehouses=(
            FleetWarehouse(
                name="acme",
                schedule=CronSchedule("0 2 * * *"),
                table_config=table_config(*tables),
            ),
        )
    )


def feed(pool, tables, *, fail_hook=None) -> tuple[list[ItemResult], CandidateQueue]:
    current = fleet_of(*tables)
    queue = CandidateQueue()
    for t in tables:
        queue.offer(Candidate("acme", f"raw.{t}"))
    results: list[ItemResult] = []

    async def scenario():
        stop = asyncio.Event()

        def on_result(result: ItemResult) -> None:
            results.append(result)
            if len(results) == len(tables):
                stop.set()
            if fail_hook:
                fail_hook(result)

        await asyncio.wait_for(
            run_workers(queue, lambda: current, pool, stop, on_result=on_result), 5
        )

    asyncio.run(scenario())
    return results, queue


def test_concurrency_never_exceeds_the_pool():
    pool = FakePool(2)
    results, queue = feed(pool, [f"t{i}" for i in range(7)])
    assert len(results) == 7
    assert pool.peak == 2
    assert queue.in_flight == frozenset(), "every table was finished"


def test_one_tables_failure_is_its_own():
    pool = FakePool(2, fail={"raw.t1"})
    results, queue = feed(pool, ["t0", "t1", "t2"])
    codes = {r.item.table: r.exit_code for r in results}
    assert codes == {"raw.t0": 0, "raw.t1": 1, "raw.t2": 0}
    assert queue.in_flight == frozenset()


def test_a_failing_result_hook_does_not_stop_maintenance():
    def explode(_result):
        raise ValueError("metrics endpoint down")

    results, _ = feed(FakePool(1), ["t0", "t1"], fail_hook=explode)
    assert len(results) == 2


def test_items_wait_in_the_queue_until_a_worker_is_free():
    """Taken eagerly, they would stop coalescing. With one worker busy, the rest
    stay queued, and a re-offer of one of them still coalesces."""
    current = fleet_of("a", "b", "c")
    queue = CandidateQueue()
    for t in ("a", "b", "c"):
        queue.offer(Candidate("acme", f"raw.{t}"))

    async def scenario():
        hold = asyncio.Event()
        pool = FakePool(1, hold=hold)
        stop = asyncio.Event()
        task = asyncio.create_task(run_workers(queue, lambda: current, pool, stop))
        while not pool.started:
            await asyncio.sleep(0)
        queued_while_busy = len(queue)
        coalesced = not queue.offer(Candidate("acme", "raw.c"))
        stop.set()
        hold.set()
        await asyncio.wait_for(task, 5)
        return queued_while_busy, coalesced

    queued_while_busy, coalesced = asyncio.run(scenario())
    assert queued_while_busy == 2
    assert coalesced


def test_stop_lets_in_flight_tables_finish_and_starts_no_more():
    """The two in flight are *held* there while stop arrives -- an earlier
    version slept 1 ms and hoped they had not finished, and under a loaded test
    run they sometimes had, so two more started."""
    current = fleet_of(*(f"t{i}" for i in range(6)))
    queue = CandidateQueue()
    for i in range(6):
        queue.offer(Candidate("acme", f"raw.t{i}"))
    done: list[ItemResult] = []

    async def scenario():
        hold = asyncio.Event()
        pool = FakePool(2, hold=hold)
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_workers(queue, lambda: current, pool, stop, on_result=done.append)
        )
        while len(pool.started) < 2:
            await asyncio.sleep(0)
        stop.set()
        await asyncio.sleep(0.01)
        assert not done, "in flight, not finished, when stop arrived"
        hold.set()
        await asyncio.wait_for(task, 5)
        return pool

    pool = asyncio.run(scenario())
    assert len(done) == len(pool.started) == 2, "the two in flight finished; none started after"
    assert len(queue) == 4
    assert queue.in_flight == frozenset()


@pytest.mark.parametrize("workers", [1, 3])
def test_every_queued_table_runs_exactly_once(workers):
    pool = FakePool(workers)
    tables = [f"t{i}" for i in range(9)]
    feed(pool, tables)
    assert sorted(pool.started) == sorted(f"raw.{t}" for t in tables)


def test_the_worker_thread_budget_reaches_duckdb(tmp_path):
    """`--threads` is how a worker shares the CPUs; it has to arrive at DuckDB,
    not stop at argparse."""
    from zamboni.cli import _apply_profile, _build_parser, _session_from
    from zamboni.settings import Profile

    (tmp_path / "wh").mkdir()
    args = _build_parser().parse_args(
        ["maintenance", "--local-warehouse", str(tmp_path / "wh"), "--threads", "2"]
    )
    _apply_profile(args, Profile())
    session = _session_from(args)
    try:
        assert session.con.execute("select current_setting('threads')").fetchone()[0] == 2
    finally:
        session.close()


# -- DuckDB's memory, per worker (ZMBNI-147) -----------------------------------------


def test_each_workers_duckdb_gets_its_share_of_the_plan():
    """The plan is enforced where memory is allocated: N workers' DuckDB limits
    plus their baselines fit inside the headroom of the limit."""
    limit, budget = 8192 * MiB, 256 * MiB
    size = size_pool(cpus=16, memory_limit=limit, memory_budget_bytes=budget)
    assert size.duckdb_memory_bytes is not None
    total = size.workers * (size.duckdb_memory_bytes + WORKER_BASELINE_BYTES)
    assert total <= limit * MEMORY_HEADROOM
    # ...and uses it: no worker's share is left more than a byte per worker unused.
    assert limit * MEMORY_HEADROOM - total < size.workers
    assert size.duckdb_memory_bytes >= budget, "room for the group the budget allows"


def test_fewer_workers_get_more_each():
    one = size_pool(cpus=8, memory_limit=4096 * MiB, memory_budget_bytes=0, requested=1)
    four = size_pool(cpus=8, memory_limit=4096 * MiB, memory_budget_bytes=0, requested=4)
    assert one.duckdb_memory_bytes > four.duckdb_memory_bytes


def test_no_memory_figure_leaves_duckdb_its_default():
    size = size_pool(cpus=4, memory_limit=None, memory_budget_bytes=256 * MiB)
    assert size.duckdb_memory_bytes is None


def test_a_plan_that_does_not_fit_floors_duckdb_and_says_so(caplog):
    from zamboni.pool import DUCKDB_MIN_MEMORY_BYTES

    with caplog.at_level("WARNING", logger="zamboni.pool"):
        size = size_pool(cpus=1, memory_limit=128 * MiB, memory_budget_bytes=256 * MiB)
    assert size.workers == 1
    assert size.duckdb_memory_bytes == DUCKDB_MIN_MEMORY_BYTES
    assert "does not fit" in caplog.text


def test_with_no_cgroup_the_plan_uses_host_memory(monkeypatch):
    """Declining to plan would leave every worker's DuckDB assuming 80% of the host."""
    from zamboni import pool

    monkeypatch.setattr(pool, "cgroup_memory_limit", lambda: None)
    monkeypatch.setattr(pool, "physical_memory", lambda: 16384 * MiB)
    monkeypatch.setattr(pool, "available_cpus", lambda: 4)
    size = pool.plan_pool(memory_budget_bytes=256 * MiB)
    assert size.duckdb_memory_bytes is not None
    assert size.workers * size.duckdb_memory_bytes < 16384 * MiB * MEMORY_HEADROOM


def test_the_cgroup_limit_wins_over_host_memory(monkeypatch):
    from zamboni import pool

    monkeypatch.setattr(pool, "cgroup_memory_limit", lambda: 2048 * MiB)
    monkeypatch.setattr(pool, "physical_memory", lambda: 65536 * MiB)
    monkeypatch.setattr(pool, "available_cpus", lambda: 4)
    size = pool.plan_pool(memory_budget_bytes=256 * MiB)
    assert size.workers * (size.duckdb_memory_bytes + WORKER_BASELINE_BYTES) <= (
        2048 * MiB * MEMORY_HEADROOM
    )


def test_a_worker_config_carries_the_plan_and_the_worker_passes_it(tmp_path):
    """No second literal: the numbers the sizing chose are the numbers on the
    worker's command line, and the CLI's own parser reads them back."""
    from zamboni.cli import _build_parser

    size = size_pool(cpus=8, memory_limit=4096 * MiB, memory_budget_bytes=256 * MiB)
    config = WorkerConfig.from_pool(size, workdir=tmp_path)
    assert (config.threads, config.duckdb_memory_bytes) == (
        size.threads_per_worker,
        size.duckdb_memory_bytes,
    )
    args = _build_parser().parse_args(
        worker_argv(item(), config, tmp_path / "t.json", tmp_path / "r.jsonl")
    )
    assert args.duckdb_memory_limit_bytes == size.duckdb_memory_bytes
    assert args.threads == size.threads_per_worker


def test_without_a_plan_a_worker_passes_no_memory_flag(tmp_path):
    argv = worker_argv(item(), WorkerConfig(workdir=tmp_path), tmp_path / "t", tmp_path / "r")
    assert "--duckdb-memory-limit-bytes" not in argv


def session_from(tmp_path, *flags: str):
    from zamboni.cli import _apply_profile, _build_parser, _session_from
    from zamboni.settings import Profile

    (tmp_path / "wh").mkdir(exist_ok=True)
    args = _build_parser().parse_args(
        ["maintenance", "--local-warehouse", str(tmp_path / "wh"), *flags]
    )
    _apply_profile(args, Profile())
    return _session_from(args)


def duckdb_setting(session, name: str):
    return session.con.execute(f"select current_setting('{name}')").fetchone()[0]


def test_the_memory_flag_reaches_duckdb(tmp_path):
    session = session_from(tmp_path, "--duckdb-memory-limit-bytes", str(512 * MiB))
    try:
        assert duckdb_setting(session, "memory_limit") == "512.0 MiB"
        assert session.memory_limit_bytes == 512 * MiB
    finally:
        session.close()


def test_without_the_flag_duckdb_keeps_its_own_default(tmp_path):
    """A cron or CLI run is unchanged: what DuckDB picks for itself."""
    import duckdb

    session = session_from(tmp_path)
    try:
        expected = duckdb.connect().execute("select current_setting('memory_limit')").fetchone()[0]
        assert duckdb_setting(session, "memory_limit") == expected
        assert session.memory_limit_bytes is None
    finally:
        session.close()


@pytest.mark.parametrize("value", ["0", "-1"])
def test_a_non_positive_memory_limit_is_a_usage_error(tmp_path, value):
    with pytest.raises(SystemExit) as exc:
        session_from(tmp_path, "--duckdb-memory-limit-bytes", value)
    assert exc.value.code == 2
