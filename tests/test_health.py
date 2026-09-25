# SPDX-License-Identifier: Apache-2.0
"""Health from metadata: what it reads, and what it refuses to guess.

The point of this tier is that it is cheap enough to ask about every table in a
fleet before doing any work. A test that let it read a manifest would not fail
loudly -- it would just be slow, at a scale no test runs at -- so the cheapness
is asserted directly rather than trusted.
"""

from __future__ import annotations

import pytest

from zamboni.health import UNSEEN_FROM_METADATA, TableHealth, table_health


def test_reads_the_signals_from_the_snapshot_summary(unpartitioned):
    health = table_health(unpartitioned)

    assert health.identifier == "db.unpartitioned"
    assert health.snapshot_id == unpartitioned.metadata.current_snapshot_id
    assert health.data_files == 6, "six appends, six live data files"
    assert health.total_bytes and health.total_bytes > 0
    assert health.snapshots == 6


def test_no_manifest_is_read(unpartitioned, monkeypatch):
    """The whole argument for this tier is that it costs one metadata load.

    Reading a manifest would still return the right answer, just ~16x slower --
    a regression nothing else here would catch, because the suite's tables are
    small enough that slow and fast look identical.
    """
    from pyiceberg.table.snapshots import Snapshot

    def refuse(*args, **kwargs):
        raise AssertionError("table_health read a manifest; it must not")

    monkeypatch.setattr(Snapshot, "manifests", refuse)

    assert table_health(unpartitioned).data_files == 6


def test_it_names_what_it_cannot_see(unpartitioned):
    """ "No problems found" and "no problems visible from here" are different
    claims, and only the second one is true at this tier.

    Three of the six signals in the runbook need the manifests and one needs a
    storage listing. Omitting them silently would let a caller read a clean
    health result as a full assessment.
    """
    unseen = table_health(unpartitioned).unseen

    assert unseen == UNSEEN_FROM_METADATA
    assert len(unseen) == 4
    for signal in ("per partition", "manifests per data file", "metadata bytes", "unreferenced"):
        assert any(signal in row for row in unseen), f"{signal} is not named as unseen"
    # Each one says which tier answers it, so the reader is not left guessing.
    assert all("needs" in row for row in unseen)


@pytest.mark.parametrize(
    ("field", "expected"),
    [("data_files", None), ("total_bytes", None), ("delete_files", None), ("records", None)],
)
def test_a_missing_counter_is_none_and_not_zero(field, expected):
    """A writer may omit any summary key, and "not reported" leads to the
    opposite decision from "zero": zero data files means an empty table, absent
    means we do not know, and only one of those is safe to act on."""
    assert getattr(TableHealth(identifier="db.t", snapshot_id=1), field) is expected


def test_derived_signals_refuse_to_divide_by_nothing():
    """A table with no data files is not a table with small files."""
    empty = TableHealth(identifier="db.t", snapshot_id=1, data_files=0, total_bytes=0)
    unreported = TableHealth(identifier="db.t", snapshot_id=1, data_files=10)

    assert empty.average_file_bytes is None
    assert empty.delete_ratio is None
    assert unreported.average_file_bytes is None, "no total bytes reported, so no average"


def test_the_derived_signals_are_the_compaction_and_mor_questions():
    """Average file size against target is the primary compaction signal; delete
    files per data file is the merge-on-read one. Both are ratios of counters
    Iceberg already publishes."""
    health = TableHealth(
        identifier="db.t",
        snapshot_id=1,
        data_files=4,
        total_bytes=4096,
        delete_files=2,
    )

    assert health.average_file_bytes == 1024
    assert health.delete_ratio == 0.5


def test_a_table_with_no_snapshots_reports_rather_than_raises(session):
    """A table nothing has written to yet is a normal thing for a fleet sweep to
    meet, and it is precisely the case where there is nothing to do."""
    from tests.conftest import SCHEMA

    empty = session.catalog.create_table("db.never_written", schema=SCHEMA)

    health = table_health(empty)

    assert health.snapshot_id is None
    assert health.data_files is None
    assert health.snapshots == 0


# -- the watermark (ZMBNI-114) --------------------------------------------


def test_a_table_nobody_maintained_is_entirely_unreviewed(unpartitioned):
    """Never maintained reads as six unreviewed snapshots, not as "up to date".

    The `None` watermark and a zero count would be indistinguishable to a caller
    deciding whether to work, and they mean opposite things.
    """
    from zamboni.health import maintenance_watermark

    mark = maintenance_watermark(unpartitioned)

    assert mark.maintained is False
    assert mark.snapshots_since == 6, "every snapshot is unreviewed"
    assert mark.written_since is True


def test_a_real_compaction_leaves_a_readable_watermark(session, unpartitioned):
    """End to end: compact, then read the mark back out of the table.

    This is the property the whole design rests on -- that maintenance history
    lives in the table rather than in a store the service has to keep. Asserted
    against a real commit rather than a hand-built summary, because what is
    being tested is that the stamp Zamboni writes is the stamp this reads.
    """
    from zamboni.compactor import TableCompactor
    from zamboni.config import CompactionConfig
    from zamboni.health import maintenance_watermark

    TableCompactor(session, "db.unpartitioned", CompactionConfig(min_input_files=2)).execute()
    tbl = session.catalog.load_table("db.unpartitioned")

    mark = maintenance_watermark(tbl)

    assert mark.maintained is True
    assert mark.operation == "compaction"
    assert mark.snapshot_id == tbl.metadata.current_snapshot_id
    assert mark.written_since is False, "nothing has written since we compacted"


def test_a_write_after_maintenance_makes_the_table_due_again(session, unpartitioned):
    """The due-check in one property: compact, append, and the table is due."""
    from zamboni.compactor import TableCompactor
    from zamboni.config import CompactionConfig
    from zamboni.health import maintenance_watermark

    from .conftest import batch

    TableCompactor(session, "db.unpartitioned", CompactionConfig(min_input_files=2)).execute()
    tbl = session.catalog.load_table("db.unpartitioned")
    tbl.append(batch(900, 5))

    mark = maintenance_watermark(session.catalog.load_table("db.unpartitioned"))

    assert mark.maintained is True
    assert mark.snapshots_since == 1, "one append since the compaction"
    assert mark.written_since is True


def test_the_newest_stamp_wins_not_the_first(session, unpartitioned):
    """Two maintenance runs, and the watermark is the later one.

    Taking the first would make a table look stale for as long as its oldest
    maintenance snapshot survived.
    """
    from zamboni.compactor import TableCompactor
    from zamboni.config import CompactionConfig
    from zamboni.health import maintenance_watermark
    from zamboni.manifests import ManifestRewriter

    from .conftest import batch

    config = CompactionConfig(min_input_files=2)
    TableCompactor(session, "db.unpartitioned", config).execute()
    first = maintenance_watermark(session.catalog.load_table("db.unpartitioned"))

    tbl = session.catalog.load_table("db.unpartitioned")
    tbl.append(batch(900, 5))
    ManifestRewriter(min_input_manifests=2).run(session.catalog.load_table("db.unpartitioned"))

    later = maintenance_watermark(session.catalog.load_table("db.unpartitioned"))

    assert later.snapshot_id != first.snapshot_id, "the watermark did not advance"
    assert later.written_since is False, "the rewrite is the newest snapshot"
