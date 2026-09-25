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
