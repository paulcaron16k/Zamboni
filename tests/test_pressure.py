# SPDX-License-Identifier: Apache-2.0
"""Delete pressure (#175): which tables are offered between schedules, and why.

The decision is a function of a health record, so it is tested on records: the
three thresholds, the tables it must never offer (copy-on-write, equality
deletes, unreported counters), and the settings that tune it.
"""

from __future__ import annotations

import dataclasses
import json
import subprocess
import sys

import pytest

from zamboni.pressure import (
    DEFAULT_CHECK_EVERY_MINUTES,
    DEFAULT_DELETE_FILES,
    DEFAULT_DELETE_RATIO,
    DEFAULT_DELETED_ROWS_RATIO,
    HEALTH_FIELDS,
    PressureSettings,
)

#: A merge-on-read table under every default threshold.
QUIET = {
    "data_files": 100,
    "delete_files": 2,
    "position_deletes": 10,
    "equality_deletes": 0,
    "records": 10_000,
}


def health(**changes) -> dict:
    return QUIET | changes


def test_the_defaults_are_the_ones_decided():
    """Chosen starting values, confirmed on #175 (2026-10-08). A changed
    default changes what a fleet compacts, so it is pinned."""
    s = PressureSettings()
    assert (
        (s.delete_files, s.delete_ratio, s.deleted_rows_ratio, s.check_every_minutes)
        == (
            DEFAULT_DELETE_FILES,
            DEFAULT_DELETE_RATIO,
            DEFAULT_DELETED_ROWS_RATIO,
            DEFAULT_CHECK_EVERY_MINUTES,
        )
        == (10, 0.10, 0.15, 15)
    )


def test_a_table_under_every_threshold_is_not_offered():
    assert PressureSettings().triggered(QUIET) is None


@pytest.mark.parametrize(
    ("changes", "named"),
    [
        ({"delete_files": 10}, "10 delete files"),
        ({"data_files": 20, "delete_files": 3}, "0.15 (threshold 0.1)"),
        ({"position_deletes": 1_500}, "1500 of 10000 rows"),
    ],
)
def test_any_one_threshold_met_triggers_and_says_which(changes, named):
    reason = PressureSettings().triggered(health(**changes))
    assert reason is not None and named in reason


def test_a_threshold_is_met_at_its_value_not_only_above_it():
    s = PressureSettings(delete_files=None, deleted_rows_ratio=None, delete_ratio=0.5)
    assert s.triggered(health(data_files=4, delete_files=2)) is not None
    assert s.triggered(health(data_files=5, delete_files=2)) is None


def test_a_copy_on_write_table_is_never_offered():
    """No delete files at all, however the ratios are set."""
    eager = PressureSettings(delete_files=1, delete_ratio=0.0001, deleted_rows_ratio=0.0001)
    assert eager.triggered(health(delete_files=0, position_deletes=0)) is None


def test_equality_deletes_make_a_table_ineligible_unless_the_engine_clears_them():
    """Where compaction refuses them (ZMBNI-11), or applies them and keeps their
    files (Spark, on a table whose only spec is unpartitioned), the next check
    finds the table as it was and offers it again, forever; where compaction
    applies and removes them, the table is over like any other (#300). Not
    saying is the old rule."""
    over = health(delete_files=50, equality_deletes=3)
    assert PressureSettings().triggered(over) is None
    assert PressureSettings().triggered(over, equality_deletes_eligible=False) is None
    assert PressureSettings().triggered(over, equality_deletes_eligible=True)


@pytest.mark.parametrize("engine", ["local", "trino", "spark"])
@pytest.mark.parametrize(("specs", "partitioned"), [(1, False), (1, True), (2, False)])
def test_each_engine_is_offered_equality_delete_tables_as_it_declares(engine, specs, partitioned):
    """The outcome is derived from the engine's declarations and the table's
    partitioning through the record the worker builds, so a declaration that
    changes moves the decision with it and no second copy of the fact exists
    to fall behind (#300, #301)."""
    from zamboni import maintainers
    from zamboni.health import health_record

    table = {"partition_specs": specs, "partitioned": partitioned}
    record = health_record(
        "acme", {"db.cdc": health(delete_files=50, equality_deletes=3, **table)}, engine=engine
    )
    assert record["engine"] == engine
    caps = maintainers.get(engine).capabilities()
    eligible = caps.compacts_equality_deletes and caps.clears_equality_deletes.applies_to(**table)
    assert record["tables"]["db.cdc"]["equality_deletes_eligible"] is eligible
    offered = pressure_offers_(warehouse(), record)["over"]
    assert ("db.cdc" in offered) is eligible


def test_spark_is_offered_a_partitioned_equality_delete_table_and_not_an_unpartitioned_one():
    """The case #301 exists for, spelled out rather than derived: one record,
    two tables, one engine, two answers."""
    from zamboni.health import health_record

    cdc = health(delete_files=50, equality_deletes=3, partition_specs=1)
    record = health_record(
        "acme",
        {"db.flat": cdc | {"partitioned": False}, "db.daily": cdc | {"partitioned": True}},
        engine="spark",
    )
    assert list(pressure_offers_(warehouse(), record)["over"]) == ["db.daily"]


@pytest.mark.parametrize("unmeasured", ["partition_specs", "partitioned"])
def test_a_table_whose_partitioning_was_not_measured_is_not_offered(unmeasured):
    """No answer is written without both facts, even for an engine that clears
    every table, and no answer is "not eligible"."""
    from zamboni.health import health_record

    signals = health(delete_files=50, equality_deletes=3, partition_specs=1, partitioned=True)
    signals[unmeasured] = None
    record = health_record("acme", {"db.cdc": signals}, engine="trino")
    assert "equality_deletes_eligible" not in record["tables"]["db.cdc"]
    assert pressure_offers_(warehouse(), record)["over"] == {}


def test_an_unreported_equality_count_is_ineligible_whatever_the_engine():
    """Unknown is not zero: the table might carry deletes some engine refuses."""
    unknown = health(delete_files=50, equality_deletes=None)
    assert PressureSettings().triggered(unknown, equality_deletes_eligible=True) is None


@pytest.mark.parametrize("said", [None, False, "true", 1])
def test_only_a_table_entry_that_says_true_lifts_the_equality_rule(said):
    """An application's own entry may omit the key or write something else; only
    the JSON boolean the worker writes, in the table's own entry, counts."""
    signals = health(delete_files=50, equality_deletes=3)
    record = {"tables": {"db.cdc": signals}}
    if said is not None:
        signals["equality_deletes_eligible"] = said
    assert pressure_offers_(warehouse(), record)["over"] == {}
    record["equality_deletes_eligible"] = True  # #300's place for it, beside the tables
    assert pressure_offers_(warehouse(), record)["over"] == {}
    signals["equality_deletes_eligible"] = True
    assert list(pressure_offers_(warehouse(), record)["over"]) == ["db.cdc"]


def test_unreported_counters_meet_no_threshold():
    """ "Not reported" is not zero, and it is not over either."""
    s = PressureSettings()
    assert s.triggered(health(delete_files=None)) is None
    assert s.triggered(health(delete_files=50, equality_deletes=None)) is None, (
        "equality deletes might be there"
    )
    rows_only = PressureSettings(delete_files=None, delete_ratio=None)
    assert rows_only.triggered(health(position_deletes=5_000, records=None)) is None


def test_a_switched_off_threshold_does_not_trigger():
    s = PressureSettings(delete_files=None)
    assert s.triggered(health(delete_files=12, data_files=1_000)) is None


# -- settings ---------------------------------------------------------------------------------


def test_a_block_overrides_only_the_keys_it_names():
    base = PressureSettings(delete_files=40)
    s = PressureSettings.from_value({"check_every_minutes": 5}, base)
    assert (s.delete_files, s.check_every_minutes) == (40, 5)


def test_false_is_off_and_true_is_the_base_or_the_defaults():
    base = PressureSettings(delete_files=40)
    assert PressureSettings.from_value(False, base) is None
    assert PressureSettings.from_value(True, base) == base
    assert PressureSettings.from_value(True, None) == PressureSettings(), "on again: the defaults"
    assert PressureSettings.from_value({"delete_files": 3}, None).delete_files == 3


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({"delete_file": 3}, "unknown key"),
        ({"delete_files": 0}, "above 0"),
        ({"delete_files": 2.5}, "whole number"),
        ({"delete_files": True}, "whole number"),
        ({"delete_ratio": 0}, "above 0 and at most 1"),
        ({"deleted_rows_ratio": 1.5}, "above 0 and at most 1"),
        ({"check_every_minutes": 0}, "above 0"),
        ({"delete_files": None, "delete_ratio": None, "deleted_rows_ratio": None}, "false"),
        ("sometimes", "true, false or a block"),
    ],
)
def test_settings_that_cannot_mean_what_they_say_are_refused(value, message):
    with pytest.raises(ValueError, match=message):
        PressureSettings.from_value(value, None)


# -- agreement and weight ---------------------------------------------------------------------


def test_the_fields_read_are_table_healths():
    from zamboni.health import TableHealth

    assert set(HEALTH_FIELDS) <= {f.name for f in dataclasses.fields(TableHealth)}


def test_the_module_imports_no_engine():
    """The service evaluates pressure in its own process, which stays light (#154)."""
    heavy = ("pyiceberg", "duckdb", "pyarrow")
    code = (
        "import sys, json; import zamboni.pressure, zamboni.fleet; "
        f"print(json.dumps(sorted(m for m in {heavy!r} if m in sys.modules)))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert json.loads(out.stdout) == []


# -- the check, in a worker ------------------------------------------------------------------


def warehouse(pressure=None, *tables: str):
    from tests.test_pool import table_config
    from zamboni.fleet import CronSchedule, FleetWarehouse

    config = table_config(*(tables or ("mor",)), namespace="db")
    kw = {} if pressure is None else {"pressure": pressure}
    return FleetWarehouse("acme", CronSchedule("0 2 1 1 *"), config, **kw)


def test_a_pressure_check_is_health_in_a_worker_and_never_commits(tmp_path):
    from zamboni.pool import WorkerConfig, worker_argv
    from zamboni.scheduler import PressureCheck

    wh = warehouse()
    config = WorkerConfig(workdir=tmp_path, commit=True, profile_path="/p.yml")
    argv = worker_argv(
        PressureCheck("acme", wh.table_config), config.context(), tmp_path / "t", tmp_path / "r"
    )
    assert argv[0] == "health"
    assert "--yes" not in argv, "read-only, whatever the service's --yes"
    assert "--operations" not in argv and "maintenance" not in argv
    assert argv[argv.index("--json") + 1] == str(tmp_path / "r")
    assert argv[argv.index("--profile") + 1] == "/p.yml"


def test_a_real_worker_measures_a_merge_on_read_table_over_the_default_thresholds(
    tmp_path, monkeypatch, mor_table
):
    """End to end through a spawned worker: `health --json` on a local catalog,
    read back by the service's decision. The fixture's 1 delete file to 4 data
    files is a ratio of 0.25, over the default 0.10 -- and agreeing field names
    between health's record and the decision are what this proves."""
    import asyncio
    import os

    from zamboni.pool import PoolSize, WorkerConfig, WorkerPool
    from zamboni.scheduler import PressureCheck
    from zamboni.service import pressure_offers

    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)  # no developer zamboni.yml or .env
    environ = {
        "PATH": os.environ.get("PATH", ""),
        "ZAMBONI_LOCAL_WAREHOUSE": str(tmp_path / "warehouse"),
    }
    wh = warehouse(None, "mor", "absent")

    async def scenario():
        pool = WorkerPool(
            PoolSize(1, 1, "test"), WorkerConfig(workdir=workdir, base_environ=environ)
        )
        try:
            return await pool.run(PressureCheck("acme", wh.table_config))
        finally:
            await pool.close()

    result = asyncio.run(scenario())
    assert result.exit_code == 1, "a table it could not read makes the exit 1"
    signals = result.record["tables"]["db.mor"]
    assert (signals["delete_files"], signals["data_files"], signals["equality_deletes"]) == (
        1,
        4,
        0,
    )
    found = pressure_offers(wh, result.record)
    assert list(found["over"]) == ["db.mor"] and "0.25" in found["over"]["db.mor"]
    assert list(found["errors"]) == ["db.absent"], "the rest are still checked"
    from zamboni.maintainers import offers_equality_deletes_to_pressure

    assert result.record["engine"] == "local", "no profile or variable names another"
    assert (signals["partition_specs"], signals["partitioned"]) == (1, False)
    assert signals["equality_deletes_eligible"] is offers_equality_deletes_to_pressure(
        "local", partition_specs=1, partitioned=False
    )


@pytest.mark.parametrize(("engine", "eligible"), [("trino", True), ("spark", False)])
def test_the_deployments_engine_reaches_the_record_the_decision_reads(
    tmp_path, monkeypatch, mor_table, engine, eligible
):
    """#300: a `serve` worker picks its engine the way a cron line does, from
    the profile or ZAMBONI_ENGINE, and the pressure check's record must carry
    *that* engine's answer -- not the local engine's, which is what a check
    that ignored the deployment would report. db.mor has one unpartitioned
    spec, the one shape Spark leaves its equality delete files on (#301)."""
    import os

    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)  # no developer zamboni.yml or .env
    environ = {
        "PATH": os.environ.get("PATH", ""),
        "ZAMBONI_LOCAL_WAREHOUSE": str(tmp_path / "warehouse"),
        "ZAMBONI_ENGINE": engine,
    }
    result = run_check(workdir, environ, warehouse(None, "mor"))
    assert result.exit_code == 0, result.output
    said = result.record["tables"]["db.mor"]["equality_deletes_eligible"]
    assert (result.record["engine"], said) == (engine, eligible)


def run_check(workdir, environ, wh, **config):
    """One pressure check of ``wh`` through a real pool, one spawned worker."""
    import asyncio

    from zamboni.pool import PoolSize, WorkerConfig, WorkerPool
    from zamboni.scheduler import PressureCheck

    async def scenario():
        pool = WorkerPool(
            PoolSize(1, 1, "test"), WorkerConfig(workdir=workdir, base_environ=environ, **config)
        )
        try:
            return await pool.run(PressureCheck("acme", wh.table_config))
        finally:
            await pool.close()

    return asyncio.run(scenario())


def test_a_health_entry_with_its_own_session_drives_pressure_as_the_cli_does(
    tmp_path, monkeypatch, mor_table
):
    """#233: the session built the entry's own way -- here from a variable no
    profile or flag reads -- and a record the decision reads exactly as it
    reads `health --json`'s, because both come from `check_tables`' pieces."""
    import os

    from zamboni.service import pressure_offers

    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)  # no developer zamboni.yml or .env
    wh = warehouse(None, "mor", "absent")
    path = os.environ.get("PATH", "")
    own = {"PATH": path, "TEST_OWN_WAREHOUSE": str(tmp_path / "warehouse")}

    custom = run_check(workdir, own, wh, health_entry="tests.pool_entries:own_session_health")
    assert custom.exit_code == 1, "a table it could not read makes the exit 1, as health's does"
    found = pressure_offers(wh, custom.record)
    assert list(found["over"]) == ["db.mor"] and "0.25" in found["over"]["db.mor"]
    assert list(found["errors"]) == ["db.absent"], "the rest are still checked"

    cli = run_check(
        workdir, {"PATH": path, "ZAMBONI_LOCAL_WAREHOUSE": own["TEST_OWN_WAREHOUSE"]}, wh
    )
    assert custom.record["tables"] == cli.record["tables"]
    assert pressure_offers(wh, custom.record) == pressure_offers(wh, cli.record)

    blind = run_check(workdir, own, wh)
    assert blind.record is None, "the default entry cannot read a catalog only the entry knows"


@pytest.mark.parametrize(
    "entry, code, said",
    [
        ("health_that_raises", 1, "RuntimeError: the catalog is down"),
        ("health_that_exits", 5, ""),
        ("health_without_tables", 1, "no health record with tables"),
    ],
)
def test_a_broken_health_entry_is_a_failed_check_never_a_reading(tmp_path, entry, code, said):
    """A check that read nothing must not read as "no table over"."""
    from zamboni.service import pressure_offers

    result = run_check(
        tmp_path, {"PATH": "/usr/bin:/bin"}, warehouse(), health_entry=f"tests.pool_entries:{entry}"
    )
    assert (result.exit_code, result.record, result.died) == (code, None, None)
    assert said in result.output
    assert pressure_offers(warehouse(), result.record)["errors"] == {
        "*": "the check wrote no record"
    }


def test_a_table_never_goes_to_the_health_entry(tmp_path):
    """The two entries are routed by job, so a broken health entry costs the
    pressure checks and nothing else."""
    import asyncio

    from tests.test_pool import item
    from zamboni.pool import PoolSize, WorkerConfig, WorkerPool

    config = WorkerConfig(
        entry="tests.pool_entries:record",
        health_entry="tests.pool_entries:health_that_raises",
        workdir=tmp_path,
        base_environ={"PATH": "/usr/bin:/bin"},
    )

    async def scenario():
        pool = WorkerPool(PoolSize(1, 1, "test"), config)
        try:
            return await pool.run(item("raw.a"))
        finally:
            await pool.close()

    result = asyncio.run(scenario())
    assert (result.exit_code, result.record["argv"][:2]) == (0, ["maintenance", "raw.a"])


# -- what the service does with a check ------------------------------------------------------


def test_pressure_offers_reads_a_record_and_a_dead_worker():
    wh = warehouse()
    record = {"tables": {"db.mor": health(delete_files=12), "db.calm": QUIET}}
    assert pressure_offers_(wh, record)["over"].keys() == {"db.mor"}
    dead = pressure_offers_(wh, None)
    assert (dead["over"], dead["checked"]) == ({}, 0) and dead["errors"]


def pressure_offers_(wh, record):
    from zamboni.service import pressure_offers

    return pressure_offers(wh, record)


class Result:
    def __init__(self, record, exit_code=0):
        self.record, self.exit_code = record, exit_code


class CheckPool:
    def __init__(self, record) -> None:
        self.record = record
        self.checked: list[str] = []

    async def run(self, item):
        self.checked.append(item.warehouse)
        return Result(self.record)


def run_loop(fleet, pool, *, minutes: int):
    """Drive run_pressure_checks over ``minutes`` of a clock that advances one
    minute per wait, then stop."""
    import asyncio
    from datetime import UTC, datetime, timedelta

    from zamboni.service import run_pressure_checks

    now = [datetime(2026, 10, 13, tzinfo=UTC)]
    end = now[0] + timedelta(minutes=minutes)
    offered, checks = [], []

    async def scenario():
        stop = asyncio.Event()

        async def wait(stop_event, seconds):
            now[0] += timedelta(minutes=1)
            if now[0] >= end:
                stop_event.set()
            await asyncio.sleep(0)

        await run_pressure_checks(
            lambda: fleet,
            pool,
            lambda c: offered.append(c) or True,
            stop,
            clock=lambda: now[0],
            on_check=lambda name, found: checks.append((name, found)),
            wait=wait,
        )

    asyncio.run(scenario())
    return offered, checks


def test_each_warehouse_is_checked_every_interval_starting_one_interval_in():
    from zamboni.fleet import FleetConfig
    from zamboni.scheduler import PRESSURE, Candidate

    fleet = FleetConfig(warehouses=(warehouse(PressureSettings(check_every_minutes=10)),))
    pool = CheckPool({"tables": {"db.mor": health(delete_files=12)}})
    offered, checks = run_loop(fleet, pool, minutes=35)
    assert pool.checked == ["acme"] * 3, "at 10, 20 and 30 minutes; none at start"
    assert offered == [Candidate("acme", "db.mor", PRESSURE)] * 3
    assert checks[0][1]["offered"] == 1


def test_a_warehouse_with_pressure_off_is_never_checked():
    from zamboni.fleet import FleetConfig

    off = warehouse(None)
    off = type(off)(off.name, off.schedule, off.table_config, pressure=None)
    pool = CheckPool({"tables": {}})
    run_loop(FleetConfig(warehouses=(off,)), pool, minutes=60)
    assert pool.checked == []


def test_the_service_process_imports_no_engine_to_decide():
    """The check runs in a worker; the decision runs here, on its record."""
    code = (
        "import sys, json\n"
        "import zamboni.service\n"
        "from zamboni.fleet import FleetWarehouse, CronSchedule\n"
        "from zamboni.tableconfig import TableConfig\n"
        "wh = FleetWarehouse('acme', CronSchedule('0 2 * * *'), TableConfig(warehouse='acme'))\n"
        "rec = {'tables': {'db.t': {'delete_files': 50, 'data_files': 10, 'equality_deletes': 0,"
        " 'position_deletes': 1, 'records': 100}}}\n"
        "assert zamboni.service.pressure_offers(wh, rec)['over']\n"
        "heavy = ('pyiceberg', 'duckdb', 'pyarrow')\n"
        "print(json.dumps(sorted(m for m in heavy if m in sys.modules)))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout) == []
