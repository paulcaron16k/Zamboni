# SPDX-License-Identifier: Apache-2.0
"""A compaction that held partitions is not done with them (ZMBNI-196).

`maintain()` skips the write-driven operations when nothing has been written
since the newest `zamboni.operation` stamp. That reads a stamp as "compaction
had nothing left", and a held partition breaks it: the hold
(`skip_partitions_newer_than_windows`) lifts with the clock, not with a write.
Two shapes, both run end to end:

* compaction holds everything and commits nothing, while rewrite-manifests
  commits -- the newest stamp is not compaction's at all;
* compaction commits the partitions it may and holds the rest -- its own stamp
  is the newest, and says nothing about what it left.

Either way, every later run skipped compaction until someone wrote again, and
on a table whose writes have stopped -- a closed period, a finished backfill --
that is never.
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
from zamboni import planner as planner_module
from zamboni.changed import changed_partitions
from zamboni.cli import main
from zamboni.compactor import CompactionBlocked, TableCompactor
from zamboni.config import CompactionConfig
from zamboni.deletes import DanglingDeleteCleaner
from zamboni.health import COMPACT_INCOMPLETE, COMPACT_STAMP, OPERATION_STAMP, commit_order
from zamboni.maintainers import Operation
from zamboni.manifests import ManifestRewriter

SCHEMA = Schema(NestedField(1, "ts", TimestamptzType(), required=False))
ARROW = pa.schema([pa.field("ts", pa.timestamp("us", tz="UTC"))])
SPEC = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="d"))


@pytest.fixture
def session(tmp_path):
    root = tmp_path / "wh"
    root.mkdir()
    sess = CatalogSession.for_local(warehouse_path=str(root))
    sess.catalog.create_namespace("db")
    yield sess
    sess.close()


def daily_table(session, files: dict[int, int]):
    """One day partition per key, `days ago -> how many one-row files`."""
    tbl = session.catalog.create_table(
        "db.daily", schema=SCHEMA, partition_spec=SPEC, properties={"format-version": "2"}
    )
    now = dt.datetime.now(dt.UTC)
    for offset, count in files.items():
        for _ in range(count):
            tbl.append(pa.table({"ts": [now - dt.timedelta(days=offset)]}, schema=ARROW))
    return tbl


def config(tmp_path):
    """The hold set explicitly: table-config does not apply it by default."""
    path = tmp_path / "table-config.json"
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "namespaces": {
                    "db": {"tables": {"daily": {"skip_partitions_newer_than_windows": 1}}}
                },
            }
        )
    )
    return path


def days_later(monkeypatch, days: int) -> None:
    """Move the planner's clock, and only the planner's, forward."""

    class Later(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime.now(tz) + dt.timedelta(days=days)

    monkeypatch.setattr(
        planner_module, "dt", types.SimpleNamespace(**{**vars(dt), "datetime": Later})
    )


def outcome(report, operation):
    return next(o for o in report.outcomes if o.operation is operation)


def data_files(session) -> int:
    return len(list(session.catalog.load_table("db.daily").scan().plan_files()))


def test_a_hold_with_nothing_committed_is_not_read_as_maintained(session, tmp_path, monkeypatch):
    """Compaction held everything; rewrite-manifests stamped; the hold lifted."""
    daily_table(session, {0: 3})
    path = config(tmp_path)

    held = maintain(session, table_config=path, commit=True)
    assert data_files(session) == 3, "precondition: today's partition was held"
    assert outcome(held, Operation.REWRITE_MANIFESTS).result is not None
    stamped = session.catalog.load_table("db.daily").current_snapshot()
    assert stamped.summary.additional_properties.get("zamboni.operation") == "rewrite-manifests", (
        "precondition: the newest stamp is rewrite-manifests', not compaction's"
    )
    assert stamped.summary.additional_properties.get(COMPACT_INCOMPLETE) == (
        "1 partition(s) held by skip_partitions_newer_than_windows"
    ), "the stamp the next run reads says what compaction left"

    days_later(monkeypatch, 3)
    lifted = maintain(session, table_config=path, commit=True)

    assert "nothing" not in (outcome(lifted, Operation.COMPACT).detail or ""), (
        "compaction was skipped as if the rewrite-manifests stamp meant it had finished"
    )
    assert data_files(session) == 1, "the partition held on the first run was never compacted"


def test_a_partial_compaction_is_not_read_as_finished(session, tmp_path, monkeypatch):
    """Compaction committed what it could and held the rest; the hold lifted."""
    daily_table(session, {0: 3, 5: 3})
    path = config(tmp_path)

    maintain(session, table_config=path, commit=True)
    assert data_files(session) == 4, "precondition: day -5 compacted to 1, today's 3 held"

    days_later(monkeypatch, 3)
    lifted = maintain(session, table_config=path, commit=True)

    assert "nothing" not in (outcome(lifted, Operation.COMPACT).detail or ""), (
        "compaction was skipped as if its own stamp meant it had left nothing"
    )
    assert data_files(session) == 2, "the partition held on the first run was never compacted"


# -- what the stamp says, and what must keep skipping --------------------------


def compaction_stamps(session) -> list[dict[str, str]]:
    """Compaction's stamped summaries, oldest commit first."""
    tbl = session.catalog.load_table("db.daily")
    return [
        dict(s.summary.additional_properties)
        for s in commit_order(tbl.metadata.snapshots)
        if s.summary and s.summary.additional_properties.get(OPERATION_STAMP) == COMPACT_STAMP
    ]


def test_a_finished_compaction_still_skips_the_next_run(session, tmp_path):
    """The cheap skip survives: nothing held, nothing written, nothing to do."""
    daily_table(session, {5: 3})
    path = config(tmp_path)

    maintain(session, table_config=path, commit=True)
    assert data_files(session) == 1, "precondition: day -5 was compacted"
    assert COMPACT_INCOMPLETE not in compaction_stamps(session)[-1]

    again = maintain(session, table_config=path, commit=True)

    assert (outcome(again, Operation.COMPACT).detail or "").startswith(
        "nothing to do -- nothing written since"
    )


def test_a_held_partition_with_nothing_to_merge_is_not_left_work(session, tmp_path):
    """Holding a one-file partition defers nothing: its stamp reads finished.

    Counting it would make every later run re-plan a quiet table for a partition
    that has nothing to merge when the hold lifts.
    """
    daily_table(session, {0: 1, 5: 3})
    path = config(tmp_path)

    first = maintain(session, table_config=path, commit=True)
    held = [reason for _, reason in outcome(first, Operation.COMPACT).result.skipped]
    assert any("skip_partitions_newer_than_windows" in r for r in held), (
        "precondition: today's partition was held"
    )
    assert COMPACT_INCOMPLETE not in compaction_stamps(session)[-1]

    again = maintain(session, table_config=path, commit=True)
    assert "nothing to do" in outcome(again, Operation.COMPACT).detail


def test_a_held_partition_is_named_on_the_stamp(session, tmp_path):
    daily_table(session, {0: 3, 5: 3})

    maintain(session, table_config=config(tmp_path), commit=True)

    assert compaction_stamps(session)[-1][COMPACT_INCOMPLETE] == (
        "1 partition(s) held by skip_partitions_newer_than_windows"
    )


def test_committing_group_by_group_marks_every_commit_but_the_last(session, tmp_path):
    daily_table(session, {5: 3, 6: 3})

    maintain(
        session,
        table_config=config(tmp_path),
        commit=True,
        base_config=CompactionConfig(partial_progress=True),
    )

    stamps = compaction_stamps(session)
    assert [s.get(COMPACT_INCOMPLETE) for s in stamps] == ["1 group(s) still to commit", None]


def test_a_run_that_dies_part_way_is_not_read_as_finished(session, tmp_path, monkeypatch):
    """The group it never reached is compacted by the next run, with no new write."""
    daily_table(session, {5: 3, 6: 3})
    path = config(tmp_path)
    progressive = CompactionConfig(partial_progress=True)

    real = TableCompactor._rewrite_group
    calls = []

    def dies_on_the_second(self, tbl, group):
        calls.append(group)
        if len(calls) == 2:
            raise RuntimeError("the pod was evicted")
        return real(self, tbl, group)

    monkeypatch.setattr(TableCompactor, "_rewrite_group", dies_on_the_second)
    with pytest.raises(RuntimeError, match="evicted"):
        maintain(session, table_config=path, commit=True, base_config=progressive)
    assert data_files(session) == 4, "precondition: one group committed, one not"

    monkeypatch.setattr(TableCompactor, "_rewrite_group", real)
    resumed = maintain(session, table_config=path, commit=True, base_config=progressive)

    assert "nothing" not in (outcome(resumed, Operation.COMPACT).detail or "")
    assert data_files(session) == 2


def test_a_refused_compaction_is_tried_again_without_a_write(session, tmp_path, monkeypatch):
    """A refusal can clear with no write -- a PyIceberg build fixed by an upgrade."""
    daily_table(session, {5: 3})
    path = config(tmp_path)

    def refuses(self, *, dry_run):
        raise CompactionBlocked("cannot compact db.daily: this build is unsafe")

    real = TableCompactor.execute
    monkeypatch.setattr(TableCompactor, "execute", refuses)
    refused = maintain(session, table_config=path, commit=True)
    assert outcome(refused, Operation.COMPACT).exit_code == 3, "precondition: refused"
    newest = session.catalog.load_table("db.daily").current_snapshot().summary
    assert newest.additional_properties.get(OPERATION_STAMP) == "rewrite-manifests", (
        "precondition: a later operation stamped after the refusal"
    )
    assert "did not finish (exit 3)" in newest.additional_properties[COMPACT_INCOMPLETE]

    monkeypatch.setattr(TableCompactor, "execute", real)
    fixed = maintain(session, table_config=path, commit=True)

    assert outcome(fixed, Operation.COMPACT).result is not None
    assert data_files(session) == 1


def test_a_carried_stamp_cannot_relabel_rewrite_manifests(session):
    """What a run carries is written beside an operation's stamp, never over it."""
    tbl = daily_table(session, {5: 3})

    ManifestRewriter(
        min_input_manifests=2, stamp={OPERATION_STAMP: COMPACT_STAMP, COMPACT_INCOMPLETE: "held"}
    ).run(tbl)

    summary = session.catalog.load_table("db.daily").current_snapshot().summary
    assert summary.additional_properties[OPERATION_STAMP] == "rewrite-manifests"
    assert summary.additional_properties[COMPACT_INCOMPLETE] == "held"


def test_a_carried_stamp_cannot_relabel_remove_dangling_deletes(session, mor_table):
    TableCompactor(session, "db.mor", CompactionConfig()).execute()

    DanglingDeleteCleaner(stamp={OPERATION_STAMP: COMPACT_STAMP, COMPACT_INCOMPLETE: "held"}).run(
        session.table("db.mor")
    )

    summary = session.table("db.mor").current_snapshot().summary
    assert summary.additional_properties[OPERATION_STAMP] == "remove-dangling-deletes"
    assert summary.additional_properties[COMPACT_INCOMPLETE] == "held"


# -- the other two readers of the watermark ------------------------------------


def test_a_run_that_left_work_widens_the_changed_partitions(session, tmp_path):
    """The held partition was written before the stamp, so writes since cannot name it."""
    daily_table(session, {0: 3, 5: 3})
    maintain(session, table_config=config(tmp_path), commit=True)

    changed = changed_partitions(session.catalog.load_table("db.daily"))

    assert not changed.known, "narrowed to writes since, which skips the held partition"
    assert "left compaction work" in changed.source


def test_health_reports_a_table_due_while_compaction_has_work_left(session, tmp_path, capsys):
    daily_table(session, {0: 3, 5: 3})
    maintain(session, table_config=config(tmp_path), commit=True)
    capsys.readouterr()

    args = ["health", "db.daily", "--local-warehouse", str(tmp_path / "wh"), "--exit-code"]
    assert main(args) == 0, "--exit-code answers 0 when a table is due"

    out = capsys.readouterr().out
    assert "db.daily: due -- the last run left compaction work" in out
    assert "left for later" in out
