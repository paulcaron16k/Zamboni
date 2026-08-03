"""Manifest rewriting.

The operation moves no data, so "it still reads correctly" is a weak signal --
almost any mistake here still reads correctly *today* and goes wrong later, when
a delete file's applicability is decided by a sequence number we quietly
reassigned. The tests below check the entry metadata, not just the rows.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.manifest import ManifestContent

from zamboni.manifests import (
    ManifestRewriter,
    manifest_partition_spread,
    plan_rewrite,
)
from zamboni.testing import add_position_deletes

from .conftest import ARROW_SCHEMA, batch


@pytest.fixture
def scattered(session, partitioned):
    """Manifests that each span both partitions -- what streaming ingest makes.

    The `partitioned` fixture appends both categories per commit, so every
    manifest touches every partition and no partition predicate can prune one.
    """
    return session.table("db.partitioned")


def entry_metadata(tbl) -> dict[str, tuple]:
    """file_path -> the entry fields a rewrite must not change."""
    found = {}
    for manifest in tbl.current_snapshot().manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            found[entry.data_file.file_path] = (
                entry.snapshot_id,
                entry.sequence_number,
                entry.file_sequence_number,
                entry.data_file.record_count,
            )
    return found


def manifest_count(tbl) -> int:
    return len(tbl.current_snapshot().manifests(io=tbl.io))


# -- planning ------------------------------------------------------------


def test_the_fixture_really_is_scattered(scattered):
    """Without this, the whole operation would be measuring nothing."""
    spread = manifest_partition_spread(scattered)
    assert len(spread) == 4
    assert all(n == 2 for n in spread.values()), (
        f"every manifest should span both partitions, got {spread}"
    )


def test_a_single_manifest_is_skipped(session):
    """One commit, one manifest: nothing to regroup."""
    from .conftest import SCHEMA

    tbl = session.catalog.create_table(
        "db.one_manifest", schema=SCHEMA, properties={"format-version": "2"}
    )
    tbl.append(batch(0, 10))

    plan = plan_rewrite(session.table("db.one_manifest"))
    assert plan.skipped_reason and "min_input_manifests" in plan.skipped_reason


def test_tombstone_manifests_are_dropped(session, unpartitioned):
    """Compaction leaves a manifest holding only DELETED entries.

    Those tombstones served the snapshot that made the deletion; a new snapshot
    listing current state has no use for them, and upstream's own merge manager
    keeps DELETED entries only when the *current* snapshot made them. So the
    manifest goes, and one live entry is left in one manifest.
    """
    from zamboni import CompactionConfig, TableCompactor

    TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()
    tbl = session.table("db.unpartitioned")
    before = sorted(tbl.scan().to_arrow()["id"].to_pylist())
    assert manifest_count(tbl) == 2

    result = ManifestRewriter().run(tbl)

    assert result.manifests_after == 1
    tbl = session.table("db.unpartitioned")
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == before


def test_an_empty_table_is_skipped(session):
    from .conftest import SCHEMA

    tbl = session.catalog.create_table(
        "db.empty_manifests", schema=SCHEMA, properties={"format-version": "2"}
    )
    plan = plan_rewrite(tbl)
    assert plan.skipped_reason == "table has no snapshot"


def test_planning_groups_entries_by_partition(scattered):
    plan = plan_rewrite(scattered)

    assert len(plan.replaced) == 4
    assert plan.entries == 8  # 4 commits x 2 partitions
    # Both partitions fit well under the 8 MiB target, so they pack into one
    # manifest -- but each partition's entries stay contiguous within it.
    assert plan.bins
    assert sum(len(m.entries) for m in plan.bins) == plan.entries


# -- the rewrite ---------------------------------------------------------


def test_rewrite_reduces_manifests_without_touching_data(session, scattered):
    before_rows = sorted(scattered.scan().to_arrow()["id"].to_pylist())
    before_manifests = manifest_count(scattered)
    before_entries = entry_metadata(scattered)

    result = ManifestRewriter().run(scattered)

    tbl = session.table("db.partitioned")
    assert result.manifests_after < before_manifests
    assert manifest_count(tbl) == result.manifests_after
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == before_rows
    assert entry_metadata(tbl) == before_entries, "a rewrite changed entry metadata"


def test_sequence_numbers_survive_the_rewrite(session, scattered):
    """The property that makes this safe rather than merely fast.

    A delete file applies to data files with a lower sequence number. Reassign
    one and the table still reads correctly today, then silently applies the
    wrong deletes the next time one is written.
    """
    before = {path: meta[1] for path, meta in entry_metadata(scattered).items()}
    assert len(set(before.values())) > 1, "the fixture has only one sequence number"

    ManifestRewriter().run(scattered)

    after = {
        path: meta[1] for path, meta in entry_metadata(session.table("db.partitioned")).items()
    }
    assert after == before


def test_entries_are_written_as_existing(session, scattered):
    """A rewrite adds no file, so nothing may be marked ADDED.

    Marking them added would make an incremental consumer reprocess the whole
    table every time manifests are tidied.
    """
    from pyiceberg.manifest import ManifestEntryStatus

    ManifestRewriter().run(scattered)
    tbl = session.table("db.partitioned")

    statuses = set()
    for manifest in tbl.current_snapshot().manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=False):
            statuses.add(entry.status)
    assert statuses == {ManifestEntryStatus.EXISTING}


def test_the_snapshot_says_replace_and_moves_no_files(session, scattered):
    ManifestRewriter().run(scattered)
    summary = session.table("db.partitioned").current_snapshot().summary
    # `in` on a pydantic model iterates (key, value) pairs rather than keys, so
    # membership is asserted against the plain dict it carries.
    props = summary.additional_properties

    assert summary.operation.value == "replace"
    assert props["zamboni.operation"] == "rewrite-manifests"
    # No data file was added or removed, so neither counter may appear.
    assert "added-data-files" not in props
    assert "removed-data-files" not in props
    assert props["total-data-files"] == "8"
    assert props["total-records"] == "80"


def test_dry_run_changes_nothing(session, scattered):
    before = scattered.current_snapshot().snapshot_id
    result = ManifestRewriter(dry_run=True).run(scattered)

    assert result.dry_run and result.manifests_after < result.manifests_before
    assert session.table("db.partitioned").current_snapshot().snapshot_id == before


def test_running_twice_is_a_no_op(session, scattered):
    ManifestRewriter().run(scattered)
    tbl = session.table("db.partitioned")
    snapshot = tbl.current_snapshot().snapshot_id

    result = ManifestRewriter().run(tbl)

    assert not result.plan.worth_doing
    assert session.table("db.partitioned").current_snapshot().snapshot_id == snapshot


def test_partition_spread_improves(session, scattered):
    """The metric the operation exists to move."""
    before = manifest_partition_spread(scattered)
    assert sum(before.values()) == 8  # 4 manifests x 2 partitions each

    ManifestRewriter().run(scattered)

    after = manifest_partition_spread(session.table("db.partitioned"))
    assert sum(after.values()) < sum(before.values()), (
        f"partition spread did not improve: {before} -> {after}"
    )


def test_a_target_size_of_one_byte_splits_every_partition(session, scattered):
    """The bin packer is driven by the table property, not a constant."""
    tbl = (
        scattered.transaction()
        .set_properties(**{"write.manifest.target-size-bytes": "1"})
        .commit_transaction()
    )
    plan = plan_rewrite(session.table("db.partitioned"))
    # One bin per partition group rather than one for everything.
    assert len(plan.bins) == 2
    assert all(len(m.partitions) == 1 for m in plan.bins)
    assert tbl is not None


def test_a_bad_target_size_property_is_rejected(session, scattered):
    from zamboni.manifests import ManifestRewriteError

    scattered.transaction().set_properties(
        **{"write.manifest.target-size-bytes": "eight megs"}
    ).commit_transaction()

    with pytest.raises(ManifestRewriteError, match="not an integer"):
        plan_rewrite(session.table("db.partitioned"))


# -- delete manifests ----------------------------------------------------


def test_delete_manifests_are_carried_over_untouched(session, partitioned):
    """PyIceberg cannot write one, so they must pass through by reference."""
    target = sorted(t.file.file_path for t in partitioned.scan().plan_files())[0]
    add_position_deletes(partitioned, target, [0])
    tbl = session.table("db.partitioned")

    before = {
        m.manifest_path
        for m in tbl.current_snapshot().manifests(io=tbl.io)
        if m.content == ManifestContent.DELETES
    }
    assert before, "the fixture wrote no delete manifest"
    before_rows = sorted(tbl.scan().to_arrow()["id"].to_pylist())

    ManifestRewriter().run(tbl)

    tbl = session.table("db.partitioned")
    after = {
        m.manifest_path
        for m in tbl.current_snapshot().manifests(io=tbl.io)
        if m.content == ManifestContent.DELETES
    }
    assert after == before, "a delete manifest was rewritten or dropped"
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == before_rows


def test_deletes_still_apply_after_a_rewrite(session, partitioned):
    """The failure this operation could plausibly cause, asserted directly."""
    target = sorted(t.file.file_path for t in partitioned.scan().plan_files())[0]
    add_position_deletes(partitioned, target, [0, 1])
    tbl = session.table("db.partitioned")
    before = sorted(tbl.scan().to_arrow()["id"].to_pylist())

    ManifestRewriter().run(tbl)

    after = sorted(session.table("db.partitioned").scan().to_arrow()["id"].to_pylist())
    assert after == before, "the rewrite changed which rows the deletes removed"


def test_multi_spec_tables_keep_one_manifest_per_spec(session):
    """A manifest belongs to exactly one partition spec."""
    import datetime as dt

    from zamboni import CompactionConfig, TableCompactor
    from zamboni.tableconfig import EvolutionRule, PartitionEvolution, TableConfig, TableSettings

    from .test_evolution import DAY_SPEC, TS_ARROW, TS_SCHEMA

    tbl = session.catalog.create_table(
        "db.evolved_manifests",
        schema=TS_SCHEMA,
        partition_spec=DAY_SPEC,
        properties={"format-version": "2"},
    )
    for d in range(1, 6):
        tbl.append(pa.table({"id": [d], "ts": [dt.datetime(2026, 1, d)]}, schema=TS_ARROW))

    config = TableConfig(
        defaults=TableSettings(
            partition_evolution=PartitionEvolution(
                enabled=True, rules=(EvolutionRule("day", "month", 90),)
            )
        ),
        tables={},
    )
    TableCompactor.from_table_config(
        session, "db.evolved_manifests", config, base=CompactionConfig()
    ).execute()

    tbl = session.table("db.evolved_manifests")
    plan = plan_rewrite(tbl)
    for manifest_bin in plan.bins:
        specs = {
            e.data_file.spec_id for e in manifest_bin.entries if e.data_file.spec_id is not None
        }
        assert len(specs) <= 1, f"a bin mixes specs: {specs}"


def test_rows_survive_on_an_unpartitioned_table(session):
    """Grouping by partition on an unpartitioned table means one group."""
    from .conftest import SCHEMA

    tbl = session.catalog.create_table(
        "db.flat_manifests", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(5):
        tbl.append(batch(i * 10, 10))
    tbl = session.table("db.flat_manifests")
    before = sorted(tbl.scan().to_arrow()["id"].to_pylist())

    result = ManifestRewriter().run(tbl)

    assert result.manifests_after == 1
    tbl = session.table("db.flat_manifests")
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == before
    assert pa.Table.from_batches([], schema=ARROW_SCHEMA) is not None


def test_partitions_are_packed_in_value_order_not_name_order(session):
    """Integer partitions sorted as strings give 1, 10, 2 -- scattered bounds.

    A manifest records lower/upper partition bounds, so a contiguous range of
    partitions still prunes. Sorting by name breaks that for exactly the values
    most likely to be partition keys.
    """
    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import IntegerType, NestedField

    schema = Schema(NestedField(1, "bucket_no", IntegerType(), required=False))
    arrow = pa.schema([pa.field("bucket_no", pa.int32(), nullable=True)])
    spec = PartitionSpec(
        IcebergPartitionField(
            source_id=1, field_id=1000, transform=IdentityTransform(), name="bucket_no"
        )
    )
    tbl = session.catalog.create_table(
        "db.intparts", schema=schema, partition_spec=spec, properties={"format-version": "2"}
    )
    # Two commits so there is something to regroup; values chosen so string
    # order (1, 10, 2, 9) differs from numeric order (1, 2, 9, 10).
    for values in ([1, 10], [2, 9]):
        tbl.append(pa.table({"bucket_no": pa.array(values, type=pa.int32())}, schema=arrow))

    tbl = session.table("db.intparts")
    tbl = (
        tbl.transaction()
        .set_properties(
            **{
                "write.manifest.target-size-bytes": "1"  # one bin per partition group
            }
        )
        .commit_transaction()
    )

    plan = plan_rewrite(session.table("db.intparts"))
    order = [next(iter(m.partitions))[0] for m in plan.bins]
    assert order == sorted(order), f"bins are not in partition-value order: {order}"
    assert order == [1, 2, 9, 10]


def test_clustering_alone_is_worth_doing(session):
    """Same manifest count, far better pruning -- the case count-only judging skipped.

    Two commits, each touching two partitions, packed one partition per
    manifest: two in, two out. Judging on manifest count calls that "no
    reduction" and skips it, even though it is precisely the transformation the
    operation exists to perform.
    """
    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import IntegerType, NestedField

    schema = Schema(NestedField(1, "part", IntegerType(), required=False))
    arrow = pa.schema([pa.field("part", pa.int32(), nullable=True)])
    spec = PartitionSpec(
        IcebergPartitionField(
            source_id=1, field_id=1000, transform=IdentityTransform(), name="part"
        )
    )
    tbl = session.catalog.create_table(
        "db.cluster", schema=schema, partition_spec=spec, properties={"format-version": "2"}
    )
    for _ in range(2):
        tbl.append(pa.table({"part": pa.array([1, 2], type=pa.int32())}, schema=arrow))

    tbl = session.table("db.cluster")
    tbl = (
        tbl.transaction()
        .set_properties(**{"write.manifest.target-size-bytes": "1"})
        .commit_transaction()
    )
    tbl = session.table("db.cluster")

    plan = plan_rewrite(tbl)
    assert len(plan.replaced) == 2
    assert len(plan.bins) == 2, "expected one manifest per partition"
    assert plan.spread_before == 4, "each input manifest should span both partitions"
    assert plan.spread_after == 2
    assert plan.worth_doing, "a pure clustering improvement must not be skipped"

    before = sorted(tbl.scan().to_arrow()["part"].to_pylist())
    result = ManifestRewriter().run(tbl)
    assert result.snapshot_id is not None, "the rewrite did not commit"

    tbl = session.table("db.cluster")
    assert sorted(tbl.scan().to_arrow()["part"].to_pylist()) == before
    after = manifest_partition_spread(tbl)
    assert all(n == 1 for n in after.values()), f"manifests still span partitions: {after}"


def test_an_already_clustered_table_is_skipped(session):
    """The other direction: no reduction in either measure means no commit.

    Uses a table that stays at two manifests after clustering, so the skip is
    decided by the count/spread test rather than by min_input_manifests.
    """
    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import IntegerType, NestedField

    schema = Schema(NestedField(1, "part", IntegerType(), required=False))
    arrow = pa.schema([pa.field("part", pa.int32(), nullable=True)])
    spec = PartitionSpec(
        IcebergPartitionField(
            source_id=1, field_id=1000, transform=IdentityTransform(), name="part"
        )
    )
    tbl = session.catalog.create_table(
        "db.clustered_twice", schema=schema, partition_spec=spec, properties={"format-version": "2"}
    )
    for _ in range(2):
        tbl.append(pa.table({"part": pa.array([1, 2], type=pa.int32())}, schema=arrow))
    tbl = session.table("db.clustered_twice")
    tbl.transaction().set_properties(
        **{"write.manifest.target-size-bytes": "1"}
    ).commit_transaction()

    first = ManifestRewriter().run(session.table("db.clustered_twice"))
    assert first.snapshot_id is not None
    tbl = session.table("db.clustered_twice")
    assert len(tbl.current_snapshot().manifests(io=tbl.io)) == 2

    result = ManifestRewriter().run(tbl)

    assert not result.plan.worth_doing
    assert "neither improves" in result.plan.skipped_reason
    assert result.snapshot_id is None, "a no-op rewrite committed a snapshot"
