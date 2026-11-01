# SPDX-License-Identifier: Apache-2.0
"""`zamboni serve --reserved-memory-bytes / --reserved-cpus` (#234).

R5 (#169) gave `plan_pool` the reservations; these hold `serve` to passing them
through. Each expected plan is `plan_pool`'s own answer for the same fake
cgroup, so the tests assert the plumbing, not the arithmetic -- tests/test_pool.py
owns that.
"""

from __future__ import annotations

import logging

import pytest

from tests.test_pool import pod
from zamboni import cli
from zamboni.pool import plan_pool

GiB = 1 << 30
MEMORY = "ZAMBONI_RESERVED_MEMORY_BYTES"
CPUS = "ZAMBONI_RESERVED_CPUS"


@pytest.fixture(autouse=True)
def no_reservation_in_the_environment(monkeypatch):
    monkeypatch.delenv(MEMORY, raising=False)
    monkeypatch.delenv(CPUS, raising=False)


@pytest.fixture
def limits(tmp_path, monkeypatch):
    """A pod both bounds could decide: 8 CPUs, 8 GiB, on a host with more of
    both. The host's own CPU count is affinity, which would otherwise cap the
    pod's quota and make a CPU reservation's effect depend on the test machine."""
    monkeypatch.setattr("os.sched_getaffinity", lambda pid: set(range(64)), raising=False)
    monkeypatch.setattr("os.cpu_count", lambda: 64)
    return pod(tmp_path, cpus=8, memory=8 * GiB)


def serve_args(*flags: str):
    return cli._build_parser().parse_args(["serve", "--fleet", "fleet.yaml", *flags])


def expected(limits, *, memory: int = 0, cpus: int = 0, workers: int | None = None):
    return plan_pool(
        memory_budget_bytes=cli._DEFAULTS.memory_budget_bytes,
        requested=workers,
        reserved_memory_bytes=memory,
        reserved_cpus=cpus,
        **limits,
    )


def test_with_no_reservation_serve_plans_the_pool_it_always_did(limits):
    """Both default to 0: an existing deployment's pool does not move."""
    assert cli._plan_serve_pool(serve_args(), **limits) == expected(limits)


@pytest.mark.parametrize(
    "flags, reserved",
    [
        (["--reserved-cpus", "6"], {"cpus": 6}),
        (["--reserved-memory-bytes", str(6 * GiB)], {"memory": 6 * GiB}),
        (["--reserved-cpus", "2", "--reserved-memory-bytes", str(GiB)], {"cpus": 2, "memory": GiB}),
    ],
    ids=["cpus", "memory", "both"],
)
def test_the_flags_reach_the_plan(limits, flags, reserved):
    planned = cli._plan_serve_pool(serve_args(*flags), **limits)
    assert planned == expected(limits, **reserved)
    assert planned != expected(limits), "the reservation must change this plan"


def test_the_flags_reach_the_plan_under_workers(limits):
    planned = cli._plan_serve_pool(serve_args("--workers", "2", "--reserved-cpus", "4"), **limits)
    assert planned == expected(limits, cpus=4, workers=2)


def test_a_variable_set_after_parsing_still_arrives(limits, monkeypatch):
    """`.env` is loaded after the parser is built, so the variable is read when
    the pool is planned -- not as a flag default, which would miss it (#252)."""
    args = serve_args()
    monkeypatch.setenv(CPUS, "6")
    monkeypatch.setenv(MEMORY, str(GiB))
    assert cli._plan_serve_pool(args, **limits) == expected(limits, cpus=6, memory=GiB)


def test_a_flag_beats_its_variable(limits, monkeypatch):
    monkeypatch.setenv(CPUS, "6")
    planned = cli._plan_serve_pool(serve_args("--reserved-cpus", "1"), **limits)
    assert planned == expected(limits, cpus=1)


@pytest.mark.parametrize("flag", ["--reserved-cpus", "--reserved-memory-bytes"])
def test_a_negative_flag_is_a_usage_error(flag, capsys):
    with pytest.raises(SystemExit) as exc:
        serve_args(flag, "-1")
    assert exc.value.code == 2
    assert "must be 0 or more" in capsys.readouterr().err


@pytest.mark.parametrize("value", ["-1", "lots"])
def test_a_malformed_variable_stops_serve_with_exit_2_naming_it(
    tmp_path, monkeypatch, capsys, value
):
    monkeypatch.chdir(tmp_path)  # so a developer's own zamboni.yml is not read
    monkeypatch.setenv(MEMORY, value)
    code = cli.main(
        [
            "serve",
            "--fleet",
            str(tmp_path / "fleet.yaml"),
            "--state-file",
            str(tmp_path / "state.json"),
            "--pid-file",
            str(tmp_path / "serve.pid"),
        ]
    )
    assert code == 2
    assert f"{MEMORY}={value!r}" in capsys.readouterr().err


def test_serve_says_what_it_reserved(limits, caplog):
    """In its own line: the plan's reason names a reservation only when that
    bound decided the worker count, and here the CPU bound decides."""
    with caplog.at_level(logging.INFO, logger="zamboni.cli"):
        cli._plan_serve_pool(serve_args("--reserved-memory-bytes", "1024"), **limits)
    assert [r.getMessage() for r in caplog.records] == [
        "keeping 1024 byte(s) of memory and 0 CPU(s) for this process before sizing the pool"
    ]


def test_serve_says_nothing_when_it_reserves_nothing(limits, caplog):
    with caplog.at_level(logging.INFO, logger="zamboni.cli"):
        cli._plan_serve_pool(serve_args(), **limits)
    assert not caplog.records
