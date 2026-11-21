# SPDX-License-Identifier: Apache-2.0
"""Partition evolution's waiting partitions are released by the clock (#217).

`maintain()` skips compaction -- which is what runs evolution -- whenever
nothing has been written since the newest `zamboni.operation` stamp. A
partition waiting for `older_than_days` is released by the clock, not by a
write, so on a table whose writes had stopped it was never condensed: the
same shape as the held partitions of ZMBNI-196.

#196's marker does not fit. An evolving table nearly always has partitions
still waiting to age, so "left work" would be stamped on every run and the
skip would be lost for good. The stamp carries a *date* instead -- the first
day a waiting partition crosses its cutoff -- and the skip holds until then.
"""

from __future__ import annotations

import datetime as dt
import json
import types

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import NestedField, TimestamptzType

from zamboni import CatalogSession, maintain
from zamboni import evolution as evolution_module
from zamboni import health as health_module
from zamboni import maintenance as maintenance_module
from zamboni.cli import main
from zamboni.evolution import plan_evolution
from zamboni.health import EVOLUTION_RELEASE, OPERATION_STAMP, maintenance_watermark
from zamboni.maintainers import Operation
from zamboni.profile import profile_table
from zamboni.tableconfig import TableConfig

SCHEMA = Schema(NestedField(1, "ts", TimestamptzType(), required=False))
ARROW = pa.schema([pa.field("ts", pa.timestamp("us", tz="UTC"))])
SPEC = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="d"))

#: Short, so the test's own dates stay readable. Nothing depends on the value.
OLDER_THAN_DAYS = 10


@pytest.fixture
def session(tmp_path):
    root = tmp_path / "wh"
    root.mkdir()
    sess = CatalogSession.for_local(warehouse_path=str(root))
    sess.catalog.create_namespace("db")
    yield sess
    sess.close()


def config(tmp_path, *, evolution: bool = True):
    path = tmp_path / "table-config.json"
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "defaults": {
                    "partition_evolution": {
                        "enabled": evolution,
                        "rules": [
                            {"from": "day", "to": "month", "older_than_days": OLDER_THAN_DAYS}
                        ],
                    }
                },
                "namespaces": {"db": {"tables": {"quiet": {}}}},
            }
        )
    )
    return path


def quiet_table(session, days_ago: int, files: int = 3):
    """`files` one-row appends into the day `days_ago`, then no more writes."""
    tbl = session.catalog.create_table(
        "db.quiet", schema=SCHEMA, partition_spec=SPEC, properties={"format-version": "2"}
    )
    at = dt.datetime.now(dt.UTC) - dt.timedelta(days=days_ago)
    for _ in range(files):
        tbl.append(pa.table({"ts": [at]}, schema=ARROW))
    return tbl


def release_of(days_ago: int) -> dt.date:
    """When a day `days_ago` crosses the cutoff: its window end plus the rule's age."""
    today = dt.datetime.now(dt.UTC).date()
    return today - dt.timedelta(days=days_ago) + dt.timedelta(days=1 + OLDER_THAN_DAYS)


def days_later(monkeypatch, days: int) -> None:
    """Move every clock the due-check and the plan read forward, and only those."""

    class Later(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime.now(tz) + dt.timedelta(days=days)

    shifted = types.SimpleNamespace(**{**vars(dt), "datetime": Later})
    monkeypatch.setattr(evolution_module, "dt", shifted)
    monkeypatch.setattr(health_module, "dt", shifted)
    monkeypatch.setattr(maintenance_module, "datetime", Later)


def outcome(report, operation):
    return next(o for o in report.outcomes if o.operation is operation)


def specs_of_live_files(session) -> set[str]:
    tbl = session.catalog.load_table("db.quiet")
    specs = tbl.metadata.specs()
    return {str(specs[f.spec_id].fields[0].transform) for f in profile_table(tbl).live_files}


def newest_stamp(session) -> dict[str, str]:
    tbl = session.catalog.load_table("db.quiet")
    snapshot = tbl.snapshot_by_id(maintenance_watermark(tbl).snapshot_id)
    return dict(snapshot.summary.additional_properties)


# -- the plan names the date ---------------------------------------------------


def test_the_plan_names_the_first_day_a_waiting_partition_ages(session, tmp_path):
    quiet_table(session, days_ago=3)
    tbl = session.catalog.load_table("db.quiet")
    tbl.append(pa.table({"ts": [dt.datetime.now(dt.UTC) - dt.timedelta(days=6)]}, schema=ARROW))
    tbl = session.catalog.load_table("db.quiet")
    settings = TableConfig.load(config(tmp_path)).for_table("db.quiet")

    plan = plan_evolution(tbl, settings, profile_table(tbl).live_files)

    assert plan.is_empty, "precondition: nothing has aged yet"
    assert plan.release_at == release_of(6), "the older of the two waiting days ages first"


def test_nothing_waiting_names_no_date(session, tmp_path):
    quiet_table(session, days_ago=OLDER_THAN_DAYS + 5)
    tbl = session.catalog.load_table("db.quiet")
    settings = TableConfig.load(config(tmp_path)).for_table("db.quiet")

    plan = plan_evolution(tbl, settings, profile_table(tbl).live_files)

    assert not plan.is_empty, "precondition: the one day has aged"
    assert plan.release_at is None


# -- end to end: the skip holds until the date, then lets compaction run ---------


def test_a_quiet_table_is_condensed_once_its_waiting_partition_ages(session, tmp_path, monkeypatch):
    """The issue's reproduction: writes stop, a partition ages, nothing runs it."""
    quiet_table(session, days_ago=3)
    path = config(tmp_path)

    first = maintain(session, table_config=path, commit=True)
    assert outcome(first, Operation.COMPACT).ok
    assert specs_of_live_files(session) == {"day"}, "precondition: nothing aged yet"
    assert newest_stamp(session).get(EVOLUTION_RELEASE) == release_of(3).isoformat(), (
        "the stamp the next run reads says when the waiting partition ages"
    )

    days_later(monkeypatch, (release_of(3) - dt.datetime.now(dt.UTC).date()).days)
    released = maintain(session, table_config=path, commit=True)

    assert "nothing written since" not in (outcome(released, Operation.COMPACT).detail or ""), (
        "compaction was skipped as if the stamp meant nothing could have changed"
    )
    assert specs_of_live_files(session) == {"month"}, "the aged partition was never condensed"


def test_the_skip_holds_before_the_date(session, tmp_path, monkeypatch):
    """The cheap skip survives on a quiet evolving table between cutoffs."""
    quiet_table(session, days_ago=3)
    path = config(tmp_path)
    maintain(session, table_config=path, commit=True)

    days_later(monkeypatch, (release_of(3) - dt.datetime.now(dt.UTC).date()).days - 1)
    again = maintain(session, table_config=path, commit=True)

    assert (outcome(again, Operation.COMPACT).detail or "").startswith(
        "nothing to do -- nothing written since"
    )


def test_the_date_rides_on_a_stamp_compaction_did_not_write(session, tmp_path):
    """Compaction committed nothing; rewrite-manifests' stamp must carry the date.

    The next run reads the *newest* stamp, as with #196's marker.
    """
    quiet_table(session, days_ago=3, files=1)
    tbl = session.catalog.load_table("db.quiet")
    for _ in range(2):  # more one-file days: manifests to rewrite, nothing to compact
        tbl.append(
            pa.table({"ts": [dt.datetime.now(dt.UTC) - dt.timedelta(days=4 + _)]}, schema=ARROW)
        )
        tbl = session.catalog.load_table("db.quiet")

    report = maintain(session, table_config=config(tmp_path), commit=True)

    assert not outcome(report, Operation.COMPACT).result.evolved, "precondition"
    stamp = newest_stamp(session)
    assert stamp[OPERATION_STAMP] == "rewrite-manifests", "precondition: not compaction's stamp"
    assert stamp.get(EVOLUTION_RELEASE) == release_of(5).isoformat()


def test_no_evolution_stamps_no_date(session, tmp_path):
    """A table with nothing waiting reads exactly as before #217."""
    quiet_table(session, days_ago=3)
    maintain(session, table_config=config(tmp_path, evolution=False), commit=True)

    assert EVOLUTION_RELEASE not in newest_stamp(session)


def test_a_stamp_from_before_the_date_existed_still_skips(session, tmp_path, monkeypatch):
    """No forced runs on upgrade: a stamp without the key is read as today."""
    quiet_table(session, days_ago=OLDER_THAN_DAYS + 5)
    path = config(tmp_path, evolution=False)
    maintain(session, table_config=path, commit=True)
    assert EVOLUTION_RELEASE not in newest_stamp(session), "precondition: an old-style stamp"

    days_later(monkeypatch, 400)
    again = maintain(session, table_config=path, commit=True)

    assert (outcome(again, Operation.COMPACT).detail or "").startswith(
        "nothing to do -- nothing written since"
    )


def test_an_unreadable_date_lets_compaction_run(session):
    """Any doubt skips nothing (`_unchanged_since_maintenance`)."""
    mark = health_module.Watermark(
        identifier="db.quiet", snapshot_id=1, operation="compact", release_at="not-a-date"
    )
    assert mark.compaction_left(dt.date(2026, 1, 1))


def test_health_reports_a_table_due_once_its_waiting_partition_ages(
    session, tmp_path, monkeypatch, capsys
):
    quiet_table(session, days_ago=3)
    maintain(session, table_config=config(tmp_path), commit=True)
    args = ["health", "db.quiet", "--local-warehouse", str(tmp_path / "wh"), "--exit-code"]
    capsys.readouterr()

    assert main(args) == 1, "not due before the date"
    days_later(monkeypatch, (release_of(3) - dt.datetime.now(dt.UTC).date()).days)
    assert main(args) == 0, "due once it passes"

    out = capsys.readouterr().out
    assert "db.quiet: due -- a waiting partition aged past its evolution cutoff" in out
