# SPDX-License-Identifier: Apache-2.0
"""docs/maintenance-runtime.md §7.1: each failure made to happen (#170).

Every test here makes its row of the table happen to a real spawned worker and
asserts what the runtime did and what it reported. Two rows are tested where
they were built: a worker that dies at startup, in
tests/test_pool.py::test_a_worker_dying_at_startup_costs_one_table_and_the_slot_recovers
(#155), and the pipe errors it generalises, in the same file's death tests.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import re
import signal
import subprocess
import sys
import textwrap
import time
from datetime import timedelta
from pathlib import Path

import psutil
import pytest

from tests.test_pool import item, run_items, table_config
from zamboni.pool import (
    SOUND_EXITS,
    STALL_TIMEOUT,
    TABLE_TIMEOUTS,
    ItemResult,
    PoolSize,
    WorkerConfig,
    WorkerPool,
)
from zamboni.scheduler import EVENT, REASON_OPERATIONS, SCHEDULE, PressureCheck

ROOT = Path(__file__).resolve().parent.parent

#: A table of `tests.pool_entries._cli`'s that ends with each sound exit.
ENDS_WITH = {0: "raw.a", 2: "raw.usage", 3: "raw.blocked", 4: "raw.aborted"}


def gone(pid: int) -> bool:
    """Exited: no such process, or a zombie nobody has reaped yet -- which is
    what an orphan becomes where the container's init does not reap."""
    try:
        return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True


def wait_for(predicate, *, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


# -- the entry raises: the worker is retired -----------------------------------------


def test_a_worker_whose_entry_raised_is_retired(tmp_path):
    """A process that just raised is not trusted with the next table."""
    before, boom, after = run_items(tmp_path, [item("raw.a"), item("raw.boom"), item("raw.b")])

    assert (boom.exit_code, boom.died) == (1, None), "a failure of the table, not a death"
    assert "RuntimeError: boom" in boom.output, "with its traceback"
    assert (after.exit_code, after.died) == (0, None)
    assert after.record["pid"] != before.record["pid"], "the next table got a new worker"


def test_the_sound_exits_are_the_contracts_own():
    assert set(ENDS_WITH) == SOUND_EXITS


@pytest.mark.parametrize("code", sorted(set(ENDS_WITH) - {0}))
def test_a_refusal_or_an_abort_keeps_its_worker(tmp_path, code):
    """Exits 2, 3 and 4 are the contract's answers, not the process failing."""
    before, ended, after = run_items(
        tmp_path, [item("raw.a"), item(ENDS_WITH[code]), item("raw.b")]
    )

    assert ended.exit_code == code
    assert after.record["pid"] == before.record["pid"], "the same worker ran the next table"


# -- CPU contention and out of memory: the worker yields --------------------------------


def test_a_worker_runs_niced_and_first_in_line_for_the_oom_killer(tmp_path):
    config = WorkerConfig()
    ours = os.getpriority(os.PRIO_PROCESS, 0)

    (result,) = run_items(tmp_path, [item("raw.a")])

    assert result.record["nice"] == max(config.nice, ours)
    if result.record["oom_score_adj"] is not None:  # Linux
        ours_adj = int(Path("/proc/self/oom_score_adj").read_text(encoding="ascii"))
        assert result.record["oom_score_adj"] == max(config.oom_score_adj, ours_adj)
    assert os.getpriority(os.PRIO_PROCESS, 0) == ours, "the runtime's own process is untouched"


def test_a_worker_killed_as_an_oom_would_be_is_reported_died(tmp_path):
    """SIGKILLed from outside mid-table, which is all an OOM kill looks like
    from here; the next table runs on a new worker."""
    pid_file = tmp_path / "worker.pid"
    config = WorkerConfig(
        entry="tests.pool_entries:sleeps",
        workdir=tmp_path,
        base_environ={"PATH": "/usr/bin:/bin", "ZAMBONI_TEST_PID_FILE": str(pid_file)},
    )

    async def scenario():
        pool = WorkerPool(PoolSize(1, 1, "test"), config)
        try:
            running = asyncio.ensure_future(pool.run(item("raw.a")))
            while not pid_file.exists():
                await asyncio.sleep(0.05)
            killed = int(pid_file.read_text(encoding="ascii"))
            os.kill(killed, signal.SIGKILL)
            return killed, await asyncio.wait_for(running, timeout=30)
        finally:
            pool.kill()  # as in `run_one`: fail, never hang
            await pool.close()

    killed, result = asyncio.run(scenario())
    assert result.exit_code == 1
    assert result.died == "worker died (killed by signal 9)"
    assert result.record is None
    assert gone(killed)


# -- the runtime's process dies: the worker exits at once -------------------------------

PARENT = textwrap.dedent(
    """
    import asyncio, sys
    from pathlib import Path
    from tests.test_pool import item
    from zamboni.pool import PoolSize, WorkerConfig, WorkerPool

    workdir, pid_file = Path(sys.argv[1]), sys.argv[2]
    config = WorkerConfig(
        entry="tests.pool_entries:sleeps",
        workdir=workdir,
        base_environ={"PATH": "/usr/bin:/bin", "ZAMBONI_TEST_PID_FILE": pid_file},
    )
    asyncio.run(WorkerPool(PoolSize(1, 1, "test"), config).run(item("raw.a")))
    """
)


def test_a_worker_exits_when_the_runtimes_process_is_killed(tmp_path):
    """Before the watcher, a worker outlived its parent's SIGKILL and kept
    maintaining a table whose claim nobody renewed (§7.1, verified 2026-10-07)."""
    pid_file = tmp_path / "worker.pid"
    parent = subprocess.Popen(
        [sys.executable, "-c", PARENT, str(tmp_path), str(pid_file)], cwd=ROOT
    )
    try:
        assert wait_for(pid_file.exists, seconds=60), "the worker never started its table"
        worker = int(pid_file.read_text(encoding="ascii"))
        assert not gone(worker), "the worker is running its table"

        parent.kill()
        parent.wait(timeout=10)

        assert wait_for(lambda: gone(worker), seconds=5), "the worker outlived its parent"
    finally:
        parent.kill()
        parent.wait(timeout=10)
        if pid_file.exists() and not gone(pid := int(pid_file.read_text(encoding="ascii"))):
            os.kill(pid, signal.SIGKILL)  # never leave the sleeper behind


# -- a table runs too long, or stops reporting: SIGKILL ---------------------------------

SECOND = timedelta(seconds=1)


def limited(tmp_path, entry: str, *, every: timedelta | None, stall: timedelta | None, **env):
    """A config whose every reason has the limit ``every``."""
    return WorkerConfig(
        entry=f"tests.pool_entries:{entry}",
        workdir=tmp_path,
        base_environ={"PATH": "/usr/bin:/bin", **env},
        table_timeout=dict.fromkeys(TABLE_TIMEOUTS, every),
        stall_timeout=stall,
    )


def run_one(config: WorkerConfig, job) -> ItemResult:
    async def scenario():
        pool = WorkerPool(PoolSize(1, 1, "test"), config)
        try:
            return await asyncio.wait_for(pool.run(job), timeout=60)
        finally:
            # Should a limit regress, the worker sleeps on; killing it frees
            # the pool's thread, so the test fails instead of hanging.
            pool.kill()
            await pool.close()

    return asyncio.run(scenario())


def test_a_table_that_runs_too_long_is_killed_and_reported_timed_out(tmp_path):
    """Reporting progress all the while: the wall clock is a limit of its own."""
    config = limited(
        tmp_path, "keeps_talking", every=SECOND, stall=None, ZAMBONI_TEST_TALK_SECONDS="30"
    )

    started = time.monotonic()
    result = run_one(config, item("raw.a"))

    assert result.exit_code == 1
    assert result.died == f"timed out after {SECOND} (schedule)"
    assert result.progress == "still rewriting", "what it was doing when killed"
    assert time.monotonic() - started < 15, "killed at its limit, not at the end of its work"


def test_a_table_that_stops_reporting_is_killed_and_reported_stalled(tmp_path):
    config = limited(tmp_path, "goes_quiet", every=None, stall=SECOND)

    result = run_one(config, item("raw.a"))

    assert result.exit_code == 1
    assert result.died == f"stalled: no progress report for {SECOND}"
    assert (result.progress, result.progress_reports) == ("starting, then silence", 1)


def test_a_table_that_keeps_reporting_outlives_the_stall_timeout(tmp_path):
    """Each report restarts the stall clock: six seconds of work, reported every
    0.1 s, under a stall limit of two. The margin is for a loaded machine, where
    a worker at nice 10 can go unscheduled for a moment; half a second flaked
    at a load average of 10."""
    stall = timedelta(seconds=2)
    config = limited(
        tmp_path, "keeps_talking", every=None, stall=stall, ZAMBONI_TEST_TALK_SECONDS="6"
    )

    result = run_one(config, item("raw.a"))

    assert (result.exit_code, result.died) == (0, None)
    assert result.seconds > 2 * stall.total_seconds(), "it ran well past one stall limit"


def test_a_pressure_check_reports_nothing_so_the_stall_timeout_is_its_limit(tmp_path):
    config = dataclasses.replace(
        limited(tmp_path, "record", every=None, stall=SECOND),
        health_entry="tests.pool_entries:health_that_hangs",
    )

    result = run_one(config, PressureCheck("acme", table_config("a")))

    assert result.exit_code == 1
    assert result.died == f"stalled: no progress report for {SECOND}"


def test_a_killed_tables_slot_runs_the_next_table_on_a_new_worker(tmp_path):
    config = limited(tmp_path, "goes_quiet", every=None, stall=SECOND)

    async def scenario():
        pool = WorkerPool(PoolSize(1, 1, "test"), config)
        try:
            killed = await asyncio.wait_for(pool.run(item("raw.a")), timeout=60)
            pool.config = dataclasses.replace(config, entry="tests.pool_entries:record")
            return killed, await asyncio.wait_for(pool.run(item("raw.b")), timeout=60)
        finally:
            pool.kill()  # as in `run_one`: fail, never hang
            await pool.close()

    killed, after = asyncio.run(scenario())
    assert killed.died is not None and killed.died.startswith("stalled")
    assert (after.exit_code, after.died) == (0, None)


# -- which limit a job gets -------------------------------------------------------------


def design_default(setting: str) -> str:
    """The Default cell of ``setting``'s row in docs/maintenance-runtime.md §3.5."""
    design = (ROOT / "docs" / "maintenance-runtime.md").read_text(encoding="utf-8")
    (row,) = re.findall(rf"^\| `{setting}` \| (.+?) \|", design, re.MULTILINE)
    return row


def hours(text: str) -> timedelta:
    (number,) = re.fullmatch(r"\s*(\d+) h\s*", text).groups()
    return timedelta(hours=int(number))


def test_each_reason_has_the_limit_the_design_gives_it():
    """Read from §3.5's table, so the code and the design cannot disagree:
    "`schedule`, `manual`, `reclaim`: 6 h; `event`: 1 h; ...", every reason named."""
    designed = {}
    for part in design_default("table_timeout").split(";"):
        reasons, _, limit = part.partition(":")
        for reason in re.findall(r"`(\w+)`", reasons):
            designed[reason] = hours(limit)

    assert set(designed) == set(REASON_OPERATIONS), "the design names every reason"
    assert dict(TABLE_TIMEOUTS) == designed
    assert hours(design_default("stall_timeout")) == STALL_TIMEOUT


def test_a_merged_item_gets_the_longest_of_its_reasons_limits():
    """It runs every merged reason's operations, so the shortest would cut one off."""
    config = WorkerConfig()
    merged = item("raw.a", also=frozenset({EVENT}))  # a schedule with an event merged in
    alone = dataclasses.replace(merged, reason=EVENT, also=frozenset())

    assert config.time_limit(merged) == max(TABLE_TIMEOUTS[SCHEDULE], TABLE_TIMEOUTS[EVENT])
    assert config.time_limit(alone) == TABLE_TIMEOUTS[EVENT]


def test_a_reason_without_a_limit_runs_unlimited_and_says_so_by_name():
    unlimited = WorkerConfig(table_timeout={**TABLE_TIMEOUTS, SCHEDULE: None})
    assert unlimited.time_limit(item("raw.a")) is None


def test_a_limit_table_that_leaves_a_reason_out_is_refused():
    """Or a reason added later would run without a limit by omission."""
    partial = {k: v for k, v in TABLE_TIMEOUTS.items() if k != EVENT}
    with pytest.raises(ValueError, match="table_timeout must name every reason"):
        WorkerConfig(table_timeout=partial)


# -- capability detection runs in a process of its own ----------------------------------

SERVE_PARENT = textwrap.dedent(
    """
    import json, sys
    from pathlib import Path
    import zamboni.cli  # serve's own process has the whole CLI imported
    from zamboni import capabilities
    from zamboni.pool import PoolSize, WorkerConfig
    from zamboni.service import Service

    workdir = Path(sys.argv[1])
    service = Service(
        fleet_path=workdir / "fleet.yaml",
        size=PoolSize(1, 1, "test"),
        worker_config=WorkerConfig(workdir=workdir),
        state_path=workdir / "state.json",
    )
    reason = service.capability_check()  # serve's default, as `run` calls it
    print(json.dumps({
        "reason": reason,
        "probed_here": capabilities.detect.cache_info().currsize,
        "cache_state": capabilities.cache_status(),
        "probe_module_loaded": "pyiceberg.catalog.sql" in sys.modules,
    }))
    """
)


def test_the_runtimes_process_never_runs_the_capability_probe(tmp_path):
    """The probe performs a real overwrite on a scratch table, which the process
    hosting the runtime must not do (§7.1): `serve`'s check is answered by a
    spawned process, and the parent's own `detect` is never called."""
    import json

    from zamboni import capabilities

    done = subprocess.run(
        [sys.executable, "-c", SERVE_PARENT, str(tmp_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    seen = json.loads(done.stdout.strip().splitlines()[-1])

    assert seen["reason"] == capabilities.detect().unsupported_reason(), "the answer arrived"
    assert seen["probed_here"] == 0, "detect() never ran in the parent"
    assert seen["cache_state"] == "not yet probed"
    # The probe's scratch catalog; the CLI alone does not import it.
    assert not seen["probe_module_loaded"], "the probe's modules never loaded in the parent"
