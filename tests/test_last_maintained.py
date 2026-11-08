# SPDX-License-Identifier: Apache-2.0
"""Where the runtime learns when each table was last maintained (#165).

The low class is taken oldest watermark first (docs/maintenance-runtime.md
§3.4), but the runtime process never loads table metadata. So the records the
workers already return carry it: a run's ``maintained_at`` and a health
record's per table. Expectations are read back from the table's own
watermark, never written as times.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta

import pytest

from tests.test_maintenance import config, warehouse  # noqa: F401 - fixture
from tests.test_priorities import START, TABLES, wide_fleet
from tests.test_runtime import QUIET, eventually
from zamboni import maintain
from zamboni.health import check_tables, maintenance_watermark
from zamboni.pool import ItemResult, PoolSize, WorkerConfig
from zamboni.runtime import (
    LastMaintained,
    MaintenanceRuntime,
    StaticFleet,
    maintained_times,
    run_pressure_checks,
    start_in_thread,
)
from zamboni.scheduler import EVENT, MANUAL, SCHEDULE

TABLE = "db.events"


def watermark_time(session) -> datetime | None:
    return maintenance_watermark(session.table(TABLE)).maintained_at


# -- the run record -------------------------------------------------------------------


def test_a_run_records_a_table_never_maintained_as_never(warehouse, tmp_path):  # noqa: F811
    report = maintain(warehouse, table_config=config(tmp_path))

    assert watermark_time(warehouse) is None, "the premise: never stamped"
    assert report.maintained_at == {TABLE: None}
    assert report.as_dict()["maintained_at"] == {TABLE: None}


def test_a_run_that_commits_records_the_watermark_it_left(warehouse, tmp_path):  # noqa: F811
    """Not the one it read before the table, which its own commit made stale."""
    report = maintain(warehouse, table_config=config(tmp_path), commit=True)

    after = watermark_time(warehouse)
    assert after is not None, "the premise: compaction stamped the table"
    assert report.maintained_at == {TABLE: after}
    assert datetime.fromisoformat(report.as_dict()["maintained_at"][TABLE]) == after.replace(
        microsecond=0
    ), "serialised to the second, as every time in a run record"


def test_the_watermark_is_read_again_only_after_a_stamping_commit(
    warehouse,  # noqa: F811
    tmp_path,
    monkeypatch,
):
    """One `load_table` per table, and a second only when the first went stale."""
    from zamboni import maintenance as maintenance_module

    real = maintenance_module._watermark
    reads: list[str] = []

    def counted(session, table):
        reads.append(table)
        return real(session, table)

    monkeypatch.setattr(maintenance_module, "_watermark", counted)

    maintain(warehouse, table_config=config(tmp_path))
    assert reads == [TABLE], "a preview commits nothing, so the first read stands"

    reads.clear()
    maintain(warehouse, table_config=config(tmp_path), commit=True)
    assert reads == [TABLE, TABLE]

    reads.clear()
    report = maintain(warehouse, table_config=config(tmp_path), commit=True)
    assert reads == [TABLE], "nothing written since, so nothing stamped"
    assert report.maintained_at == {TABLE: watermark_time(warehouse)}


def test_a_table_whose_watermark_cannot_be_read_is_left_out(warehouse, tmp_path, monkeypatch):  # noqa: F811
    """Unknown is not never: the runtime would put a never-maintained table first."""

    def unreadable(*args, **kwargs):
        raise RuntimeError("catalog is having a day")

    monkeypatch.setattr("zamboni.health.maintenance_watermark", unreadable)
    assert maintain(warehouse, table_config=config(tmp_path)).maintained_at == {}


# -- the health record ----------------------------------------------------------------


def test_a_health_record_carries_each_tables_last_maintenance(warehouse, tmp_path):  # noqa: F811
    def recorded():
        record = check_tables(warehouse.catalog, [TABLE], warehouse=None)
        return maintained_times({TABLE: record["tables"][TABLE]["maintained_at"]})[TABLE]

    assert recorded() is None
    maintain(warehouse, table_config=config(tmp_path), commit=True)
    assert recorded() == watermark_time(warehouse) is not None


# -- reading them ---------------------------------------------------------------------


def test_reading_the_times_keeps_never_and_leaves_out_what_is_not_a_time():
    at = START - timedelta(days=2)
    found = maintained_times(
        {
            "raw.a": at.isoformat(),
            "raw.b": None,
            "raw.c": "yesterday",
            "raw.d": 1730000000,
            "raw.e": at.replace(tzinfo=None).isoformat(),  # naive: whose clock?
            "raw.f": "2026-11-03T02:00:00Z",
        }
    )
    assert found == {
        "raw.a": at,
        "raw.b": None,
        "raw.f": datetime(2026, 11, 3, 2, tzinfo=UTC),
    }


@pytest.mark.parametrize("times", [None, "x", ["raw.a"], 3])
def test_a_record_without_times_teaches_nothing(times):
    assert maintained_times(times) == {}


def test_a_table_the_fleet_no_longer_has_is_forgotten():
    known = LastMaintained()
    known.learned("acme", {"raw.t000": START, "raw.gone": START})
    known.learned("globex", {"raw.t000": START})

    known.adopted(wide_fleet())

    assert (known("acme", "raw.t000"), known("acme", "raw.gone")) == (START, None)
    assert known("globex", "raw.t000") is None


def test_a_pressure_check_reports_the_times_its_record_carried():
    from tests.test_pressure import health
    from tests.test_pressure import warehouse as pressure_warehouse
    from zamboni.fleet import FleetConfig
    from zamboni.pressure import PressureSettings

    at = START - timedelta(hours=7)
    tables = {
        "db.mor": health(delete_files=0) | {"maintained_at": at.isoformat()},
        "db.new": health(delete_files=0) | {"maintained_at": None},
        "db.gone": {"error": "NoSuchTableError: db.gone"},
    }
    fleet = FleetConfig(warehouses=(pressure_warehouse(PressureSettings(check_every_minutes=1)),))
    found: list[dict] = []

    class Pool:
        async def run(self, check):
            return ItemResult(check, 0, {"tables": tables}, "", None, 0.0)

    async def scenario():
        stop = asyncio.Event()
        now = [START]

        async def wait(stop_event, seconds):
            now[0] += timedelta(minutes=1)
            if found:
                stop_event.set()
            await asyncio.sleep(0)

        await run_pressure_checks(
            lambda: fleet,
            Pool(),
            lambda c: True,
            stop,
            clock=lambda: now[0],
            on_check=lambda name, f: found.append(f),
            wait=wait,
        )

    asyncio.run(scenario())
    assert found[0]["maintained_at"] == {"db.mor": at, "db.new": None}


def test_the_runtime_learns_from_a_pressure_check(tmp_path):
    runtime = MaintenanceRuntime(
        fleet=StaticFleet(wide_fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: QUIET,
    )
    at = START - timedelta(days=1)
    found = {"checked": 1, "over": {}, "errors": {}, "offered": 0, "exit_code": 0}

    runtime._pressure_checked("acme", found | {"maintained_at": {"raw.t000": at}})

    assert runtime._last_maintained("acme", "raw.t000") == at


# -- the sweep, in the runtime ----------------------------------------------------------


#: What each table's run reports. raw.t001 was never maintained, and raw.t003
#: has never run here, so the runtime has heard nothing of it.
MARKS = {
    "raw.t000": START - timedelta(hours=1),
    "raw.t001": None,
    "raw.t002": START - timedelta(days=3),
}
SWEEP = ["raw.t000", "raw.t001", "raw.t002", "raw.t003"]
HELD = f"raw.{TABLES[-1]}"


class ReportingPool:
    """Runs each table at once, except ``HELD``, which waits for ``release``;
    every record carries the table's time from ``MARKS``."""

    def __init__(self, size, config) -> None:
        self.size = size
        self.release = threading.Event()
        self.started: list[tuple[str, str]] = []
        self.killed = self.closed = False

    async def run(self, item):
        self.started.append((item.table, item.reason))
        while item.table == HELD and not (self.release.is_set() or self.killed):
            await asyncio.sleep(0.005)
        record: dict = {"exit_code": 0}
        if item.table in MARKS:
            at = MARKS[item.table]
            record["maintained_at"] = {item.table: None if at is None else at.isoformat()}
        return ItemResult(item, 0, record, "", None, 0.0)

    def kill(self) -> None:
        self.killed = True

    async def close(self) -> None:
        self.closed = True


def test_a_sweep_runs_the_tables_maintained_longest_ago_first(tmp_path):
    pools: list[ReportingPool] = []
    handle = start_in_thread(
        MaintenanceRuntime(
            fleet=StaticFleet(wide_fleet()),
            pool=PoolSize(1, 1, "test"),
            worker=WorkerConfig(workdir=tmp_path),
            clock=lambda: QUIET,
            pool_factory=lambda size, config: (
                pools.append(ReportingPool(size, config)) or pools[-1]
            ),
        )
    )
    rt = handle.runtime
    try:
        rt.offer("acme", list(MARKS), reason=EVENT)  # the runs that teach it
        eventually(lambda: len(pools[0].started) == len(MARKS))
        rt.offer("acme", [HELD], reason=MANUAL)  # holds the only worker...
        eventually(lambda: (HELD, MANUAL) in pools[0].started)
        rt.offer("acme", SWEEP, reason=SCHEDULE)  # ...while the sweep is queued
        pools[0].release.set()
        eventually(lambda: sum(r == SCHEDULE for _, r in pools[0].started) == len(SWEEP))
    finally:
        assert handle.stop(grace=timedelta(seconds=10))

    swept = [table for table, reason in pools[0].started if reason == SCHEDULE]
    never = datetime.min.replace(tzinfo=UTC)
    assert swept == sorted(SWEEP, key=lambda t: (MARKS.get(t) or never, SWEEP.index(t)))


def test_the_runtime_forgets_a_table_a_new_fleet_drops(tmp_path):
    from tests.test_scheduler import fleet
    from tests.test_scheduler import warehouse as fleet_warehouse

    runtime = MaintenanceRuntime(
        fleet=StaticFleet(wide_fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: QUIET,
    )
    found = {"checked": 2, "over": {}, "errors": {}, "offered": 0, "exit_code": 0}
    runtime._pressure_checked(
        "acme", found | {"maintained_at": {"raw.t000": START, "raw.t001": START}}
    )

    runtime.update_fleet(fleet(fleet_warehouse("acme", tables=("t000",))))

    assert runtime._last_maintained("acme", "raw.t000") == START
    assert runtime._last_maintained("acme", "raw.t001") is None
