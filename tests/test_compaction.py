"""End-to-end compaction behaviour on a local SQL catalog."""

from __future__ import annotations

import pytest

from icemaint import CompactionConfig, MemoryMode, TableCompactor
from icemaint.profile import profile_table


def live_paths(tbl) -> set[str]:
    from pyiceberg.manifest import DataFileContent

    tbl.refresh()
    snapshot = tbl.current_snapshot()
    paths: list[str] = []
    for manifest in snapshot.manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            if entry.data_file.content == DataFileContent.DATA:
                paths.append(entry.data_file.file_path)
    # A file appearing twice in the live set is the signature of a manifest
    # that was kept verbatim when it should have been rewritten.
    assert len(paths) == len(set(paths)), "a data file is live more than once"
    return set(paths)


def rows(tbl) -> list[tuple]:
    tbl.refresh()
    table = tbl.scan().to_arrow().sort_by("id")
    return list(
        zip(
            table["id"].to_pylist(),
            table["category"].to_pylist(),
            table["payload"].to_pylist(),
            strict=True,
        )
    )


@pytest.mark.parametrize("mode", [MemoryMode.IN_MEMORY, MemoryMode.CHUNKED])
def test_unpartitioned_compaction_preserves_data(session, unpartitioned, mode):
    before_rows = rows(unpartitioned)
    before_files = live_paths(unpartitioned)
    assert len(before_files) == 6

    compactor = TableCompactor(
        session,
        "db.unpartitioned",
        CompactionConfig(memory_mode=mode, target_file_size_bytes=64 * 1024 * 1024),
    )
    result = compactor.execute()

    assert result.rewritten_data_files == 6
    assert result.added_data_files >= 1

    tbl = session.table("db.unpartitioned")
    assert rows(tbl) == before_rows
    after_files = live_paths(tbl)
    assert len(after_files) < len(before_files)
    assert not (after_files & before_files), "compacted output reused a source path"


def test_commit_uses_replace_operation(session, unpartitioned):
    TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()

    tbl = session.table("db.unpartitioned")
    snapshot = tbl.current_snapshot()
    assert snapshot.summary.operation.value == "replace"
    assert snapshot.summary["icemaint.operation"] == "compaction"


def test_partitioned_compaction_is_partition_scoped(session, partitioned):
    before_rows = rows(partitioned)
    profile = profile_table(partitioned)
    assert len(profile.live_files) == 8  # 4 appends x 2 partitions

    result = TableCompactor(session, "db.partitioned", CompactionConfig()).execute()
    assert result.rewritten_data_files == 8
    assert len(result.groups) == 2  # one per partition

    tbl = session.table("db.partitioned")
    assert rows(tbl) == before_rows

    after = profile_table(tbl)
    assert len(after.live_files) == 2
    # Each output file must carry exactly one partition value.
    assert {tuple(f.partition) for f in after.live_files} == {("a",), ("b",)}


def test_bucket_partitioned_table_compacts(session, bucketed):
    """The transform that defeats PyIceberg's add_files-based partition inference.

    We go through _dataframe_to_data_files, which derives the partition key from
    the data rather than from column statistics, so bucket works.
    """
    before_rows = rows(bucketed)
    before = profile_table(bucketed)
    # 4 appends x 2 buckets: every append straddles both partitions.
    assert len(before.live_files) == 8
    assert {tuple(f.partition) for f in before.live_files} == {(0,), (1,)}

    result = TableCompactor(session, "db.bucketed", CompactionConfig()).execute()
    assert result.rewritten_data_files == 8
    assert len(result.groups) == 2  # one per bucket

    tbl = session.table("db.bucketed")
    assert rows(tbl) == before_rows

    after = profile_table(tbl)
    assert len(after.live_files) == 2
    assert {tuple(f.partition) for f in after.live_files} == {(0,), (1,)}


def test_sort_expression_orders_output_but_never_stamps_sort_order(session, unpartitioned):
    """An arbitrary order is applied, but not claimed as the table's order.

    Stamping the table's sort_order_id here would assert the data satisfies an
    order it was never sorted by -- the same defect duckdb-iceberg's native
    compactor has.
    """
    config = CompactionConfig(sort_expression="id DESC", memory_mode=MemoryMode.IN_MEMORY)
    TableCompactor(session, "db.unpartitioned", config).execute()

    tbl = session.table("db.unpartitioned")
    after = profile_table(tbl)
    assert len(after.live_files) == 1

    ids = tbl.scan().to_arrow()["id"].to_pylist()
    assert ids == sorted(ids, reverse=True)
    assert after.live_files[0].data_file.sort_order_id is None


def test_sort_by_table_order_applies_and_stamps_the_declared_order(session, sorted_table):
    config = CompactionConfig(sort_by_table_order=True, memory_mode=MemoryMode.IN_MEMORY)
    TableCompactor(session, "db.sorted", config).execute()

    tbl = session.table("db.sorted")
    after = profile_table(tbl)
    assert len(after.live_files) == 1

    # Declared order is: id DESC NULLS LAST.
    ids = tbl.scan().to_arrow()["id"].to_pylist()
    assert ids == sorted(ids, reverse=True)

    declared = tbl.sort_order()
    assert declared.order_id != 0
    assert after.live_files[0].data_file.sort_order_id == declared.order_id


def test_sort_by_table_order_refuses_an_unsorted_table(session, unpartitioned):
    config = CompactionConfig(sort_by_table_order=True)
    with pytest.raises(ValueError, match="declares no sort order"):
        TableCompactor(session, "db.unpartitioned", config).execute()


def test_sort_options_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        CompactionConfig(sort_by_table_order=True, sort_expression="id")


def test_chunked_mode_splits_output_by_target_size(session, partitioned):
    """A tiny target must produce several output files per partition.

    This is the bin-packing that bounds memory: without it, CHUNKED would
    accumulate the whole partition before writing.
    """
    before_rows = rows(partitioned)
    config = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        target_file_size_bytes=200,  # bytes, so each chunk holds a few rows
        rewrite_all=True,
    )
    TableCompactor(session, "db.partitioned", config).execute()

    tbl = session.table("db.partitioned")
    assert rows(tbl) == before_rows

    after = profile_table(tbl)
    assert len(after.live_files) > 2, "chunking did not split the output"
    assert {tuple(f.partition) for f in after.live_files} == {("a",), ("b",)}


def test_chunked_mode_with_sort_routes_through_duckdb(session, tmp_path, partitioned):
    config = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        sort_expression="id DESC",
        temp_directory=str(tmp_path / "spill"),
    )
    TableCompactor(session, "db.partitioned", config).execute()

    tbl = session.table("db.partitioned")
    for category in ("a", "b"):
        ids = tbl.scan(row_filter=f"category == '{category}'").to_arrow()["id"].to_pylist()
        assert ids == sorted(ids, reverse=True)


def test_no_sort_expression_leaves_sort_order_unset(session, unpartitioned):
    TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()
    after = profile_table(session.table("db.unpartitioned"))
    assert all(f.data_file.sort_order_id is None for f in after.live_files)
