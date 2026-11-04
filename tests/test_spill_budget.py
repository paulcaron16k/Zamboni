# SPDX-License-Identifier: Apache-2.0
"""`zamboni serve --spill-budget-bytes`: the pod's spill, divided between workers (#205).

Each worker is a DuckDB of its own spilling into the same directory, so the
per-worker cap #186 added had to be divided by hand. The pool divides it now,
as it divides memory. Expectations are derived from the config and the plan,
never written as byte counts.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.test_env_flags import settled, workdir  # noqa: F401 - fixture
from tests.test_pool import item, table_config
from zamboni import cli
from zamboni.pool import PoolSize, WorkerConfig, WorkerPool, worker_argv
from zamboni.scheduler import PressureCheck

GiB = 1 << 30
BUDGET = 18 * GiB + 7  # deliberately not a multiple of any worker count below
PER_WORKER = "ZAMBONI_MAX_TEMP_DIRECTORY_BYTES"
VARIABLE = "ZAMBONI_SPILL_BUDGET_BYTES"


def pool(workers: int) -> PoolSize:
    return PoolSize(workers, 1, "test")


@pytest.mark.parametrize("workers", [1, 3, 4])
def test_each_worker_gets_an_equal_share_of_the_budget(workers):
    config = WorkerConfig.from_pool(pool(workers), spill_budget_bytes=BUDGET)

    assert config.max_temp_directory_bytes == BUDGET // workers
    assert config.max_temp_directory_bytes * workers <= BUDGET, "never more than the budget"
    assert config.context().max_temp_directory_bytes == config.max_temp_directory_bytes


def test_without_a_budget_nothing_is_capped():
    config = WorkerConfig.from_pool(pool(4))
    assert config.max_temp_directory_bytes is None
    argv = worker_argv(item("raw.a"), config.context(), config.workdir / "t", config.workdir / "r")
    assert "--max-temp-directory-bytes" not in argv


def test_a_budget_that_cannot_give_each_worker_a_byte_is_refused():
    with pytest.raises(ValueError, match="cannot give each of 4 worker"):
        WorkerConfig.from_pool(pool(4), spill_budget_bytes=3)
    assert WorkerConfig.from_pool(pool(4), spill_budget_bytes=4).max_temp_directory_bytes == 1


def test_a_table_is_run_with_its_share_and_a_pressure_check_without(tmp_path):
    """Read back through the CLI's own parser, as the worker's `main` will."""
    config = WorkerConfig.from_pool(pool(3), spill_budget_bytes=BUDGET, workdir=tmp_path)
    context = config.context()
    parser = cli._build_parser()

    table = parser.parse_args(worker_argv(item("raw.a"), context, tmp_path / "t", tmp_path / "r"))
    check = worker_argv(
        PressureCheck("acme", table_config("a")), context, tmp_path / "t", tmp_path / "r"
    )

    assert table.max_temp_directory_bytes == config.max_temp_directory_bytes
    assert "--max-temp-directory-bytes" not in check, "a check loads metadata; it never spills"


def test_a_real_worker_receives_its_share(tmp_path):
    config = WorkerConfig.from_pool(
        pool(2),
        spill_budget_bytes=BUDGET,
        entry="tests.pool_entries:record",
        workdir=tmp_path,
        base_environ={"PATH": "/usr/bin:/bin"},
    )

    async def scenario():
        workers = WorkerPool(pool(2), config)
        try:
            return await workers.run(item("raw.a"))
        finally:
            await workers.close()

    argv = asyncio.run(scenario()).record["argv"]
    flag = argv.index("--max-temp-directory-bytes")
    assert int(argv[flag + 1]) == config.max_temp_directory_bytes


# -- serve ------------------------------------------------------------------------------


@pytest.fixture
def no_spill_settings(workdir, monkeypatch):  # noqa: F811 - the imported fixture
    for name in (PER_WORKER, VARIABLE):
        monkeypatch.delenv(name, raising=False)
    return workdir


def serve(*flags: str):
    return settled(["serve", "--fleet", "fleet.yaml", *flags])


@pytest.mark.parametrize("source", ["flag", "variable"])
def test_serve_divides_its_budget_between_the_workers_it_planned(
    no_spill_settings, monkeypatch, source
):
    if source == "flag":
        args = serve("--spill-budget-bytes", str(BUDGET))
    else:
        monkeypatch.setenv(VARIABLE, str(BUDGET))
        args = serve()

    config = cli._serve_worker_config(args, pool(3), workdir=no_spill_settings)

    assert config.max_temp_directory_bytes == BUDGET // 3


def test_serve_refuses_a_budget_beside_a_per_worker_cap(no_spill_settings, monkeypatch):
    """The worker's flag would beat the variable (#252), ignoring the operator's
    per-worker figure without a word."""
    monkeypatch.setenv(PER_WORKER, str(GiB))
    args = serve("--spill-budget-bytes", str(BUDGET))

    with pytest.raises(ValueError, match=f"{PER_WORKER} is a per-worker cap"):
        cli._serve_worker_config(args, pool(3), workdir=no_spill_settings)


def test_a_per_worker_cap_alone_still_reaches_each_worker_as_before(no_spill_settings, monkeypatch):
    """Without a budget nothing changes: each worker reads the variable itself."""
    monkeypatch.setenv(PER_WORKER, str(GiB))
    config = cli._serve_worker_config(serve(), pool(3), workdir=no_spill_settings)
    assert config.max_temp_directory_bytes is None


@pytest.mark.parametrize(
    ("flags", "environ", "says"),
    [
        (["--spill-budget-bytes", "3"], {}, "cannot give each of 4 worker"),
        (["--spill-budget-bytes", str(BUDGET)], {PER_WORKER: "1024"}, "per-worker cap"),
        (["--spill-budget-bytes", "0"], {}, "must be at least 1"),
    ],
    ids=["too-small", "beside-a-cap", "zero"],
)
def test_serve_refuses_to_start_with_exit_2(
    no_spill_settings, monkeypatch, capsys, flags, environ, says
):
    """At startup, before any worker exists: never a table's failure."""
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(cli, "_plan_serve_pool", lambda args: pool(4))
    tmp = no_spill_settings
    argv = ["serve", "--fleet", str(tmp / "fleet.yaml"), "--state-file", str(tmp / "s.json")]
    argv += ["--pid-file", str(tmp / "serve.pid"), *flags]

    try:
        code = cli.main(argv)
    except SystemExit as exc:  # argparse's own refusal of 0
        code = exc.code
    assert code == 2
    assert says in capsys.readouterr().err
