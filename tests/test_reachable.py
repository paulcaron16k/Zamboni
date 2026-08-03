"""The referenced-file set.

Everything destructive subtracts this set from something else, so the tests
that matter are the ones proving it is *complete*: a category silently dropped
here means live data deleted.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyarrow.fs import FileSelector
from pyiceberg.io.pyarrow import PyArrowFileIO

from zamboni.reachable import Category, canonical, reachable_files
from zamboni.testing import add_position_deletes

from .conftest import SCHEMA, batch


def list_storage(tbl) -> set[str]:
    """Every file physically under the table location."""
    scheme, netloc, path = PyArrowFileIO.parse_location(tbl.location())
    fs = tbl.io._initialize_fs(scheme, netloc)
    return {f.path for f in fs.get_file_info(FileSelector(path, recursive=True)) if f.is_file}


@pytest.fixture
def busy_table(session):
    """A table exercising as many reference categories as we can construct.

    Several snapshots (so there are multiple manifest lists), a rewritten file
    (so there are superseded data files), and a position delete file.
    """
    tbl = session.catalog.create_table("db.busy", schema=SCHEMA, properties={"format-version": "2"})
    for i in range(4):
        tbl.append(batch(i * 10, 10))
    tbl = session.catalog.load_table("db.busy")

    target = sorted(t.file.file_path for t in tbl.scan().plan_files())[0]
    add_position_deletes(tbl, target, [0, 1])
    return session.catalog.load_table("db.busy")


def test_canonical_matches_the_listing_form():
    """Metadata carries a scheme; a filesystem listing does not."""
    assert canonical("file:///w/t/data/a.parquet") == "/w/t/data/a.parquet"
    assert canonical("/w/t/data/a.parquet") == "/w/t/data/a.parquet"
    # s3 and s3a name the same object and must not produce two keys.
    assert canonical("s3://b/k/a.parquet") == canonical("s3a://b/k/a.parquet")


def test_every_referenced_file_exists_on_disk(busy_table):
    """The invariant orphan removal refuses to run without.

    A referenced file that is not on disk means our view of storage is wrong,
    which is the one condition under which the deletion diff is untrustworthy.
    """
    reachable = reachable_files(busy_table)
    missing = reachable.paths - list_storage(busy_table)
    assert not missing, (
        f"{len(missing)} referenced file(s) absent from storage: {sorted(missing)[:3]}"
    )


def test_the_categories_that_apply_are_all_populated(busy_table):
    """A category that silently returns nothing is how live data gets deleted."""
    reachable = reachable_files(busy_table)
    for category in (
        Category.DATA,
        Category.MANIFEST,
        Category.MANIFEST_LIST,
        Category.METADATA,
    ):
        assert reachable.by_category[category], f"{category.value} is empty"

    # These two exist in the schema but PyIceberg writes neither, so they are
    # legitimately empty here. Asserting the keys exist is what stops a future
    # refactor from dropping them unnoticed.
    assert Category.STATISTICS in reachable.by_category
    assert Category.PARTITION_STATISTICS in reachable.by_category


def test_delete_files_are_reachable(busy_table):
    """Position deletes are data-category references, not something separate."""
    reachable = reachable_files(busy_table)
    delete_paths = [p for p in reachable.by_category[Category.DATA] if "position-deletes" in p]
    assert delete_paths, "the position delete file is not in the reachable set"


def test_current_metadata_is_always_reachable(busy_table):
    assert busy_table.metadata_location in reachable_files(busy_table)


def test_superseded_files_stay_reachable_while_their_snapshot_lives(session, busy_table):
    """Nothing is reclaimable until a snapshot is actually expired.

    Every append leaves the previous snapshot intact, so its files remain
    referenced -- which is why compaction alone frees no storage.
    """
    reachable = reachable_files(busy_table)
    on_disk = list_storage(busy_table)
    # Everything written so far is still referenced by some live snapshot.
    assert on_disk - reachable.paths == set()


def test_category_of_identifies_a_file(busy_table):
    reachable = reachable_files(busy_table)
    assert reachable.category_of(busy_table.metadata_location) is Category.METADATA

    data_file = next(iter(reachable.by_category[Category.DATA]))
    assert reachable.category_of(data_file) is Category.DATA
    assert reachable.category_of("/nowhere/at/all.parquet") is None


def test_empty_table_still_reports_its_metadata(session):
    """A table with no snapshots has no data, but its metadata.json exists."""
    tbl = session.catalog.create_table(
        "db.empty_reach", schema=SCHEMA, properties={"format-version": "2"}
    )
    reachable = reachable_files(tbl)
    assert len(reachable.by_category[Category.DATA]) == 0
    assert len(reachable.by_category[Category.METADATA]) >= 1
    assert tbl.metadata_location in reachable


def test_describe_lists_every_category(busy_table):
    text = reachable_files(busy_table).describe()
    for category in Category:
        assert category.value in text
    assert "total" in text


def test_reachable_covers_a_partitioned_multi_spec_table(session):
    """Evolution leaves files under two specs; both must stay reachable."""
    import datetime as dt

    from zamboni import CompactionConfig, TableCompactor
    from zamboni.tableconfig import (
        EvolutionRule,
        PartitionEvolution,
        TableConfig,
        TableSettings,
    )

    from .test_evolution import DAY_SPEC, TS_ARROW, TS_SCHEMA

    tbl = session.catalog.create_table(
        "db.evolved_reach",
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
        session, "db.evolved_reach", config, base=CompactionConfig()
    ).execute()

    tbl = session.table("db.evolved_reach")
    reachable = reachable_files(tbl)
    assert not reachable.paths - list_storage(tbl)
    # Both the superseded day files and the new month file are still referenced.
    assert len(reachable.by_category[Category.DATA]) >= 6


def test_canonical_is_not_idempotent_for_object_keys():
    """The trap that made orphan removal unusable on S3.

    `canonical` maps a *metadata* location to the form a listing returns. A
    listing of object storage yields `bucket/key` with no scheme, and feeding
    that back through resolves it as a local path against the working
    directory. Applying it twice therefore does not converge -- so `list_storage`
    must key by the listed path as-is, never re-canonicalise it.

    Invisible locally, because local listings are already absolute paths and the
    function is idempotent on those. It took a run against MinIO to surface.
    """
    from_metadata = canonical("s3://bucket/prefix/data/a.parquet")
    assert from_metadata == "bucket/prefix/data/a.parquet"

    # The listing form fed back in: no scheme, so it is treated as a local path.
    assert canonical(from_metadata) != from_metadata
    assert canonical(from_metadata).endswith(from_metadata)
    assert canonical(from_metadata).startswith("/")

    # Local paths are idempotent, which is why every local test passed.
    assert canonical("/w/t/data/a.parquet") == "/w/t/data/a.parquet"


def test_listing_keys_match_reachable_keys(busy_table):
    """The property the bug broke, asserted on the two sets directly."""
    from zamboni.orphans import list_storage, storage_roots

    storage = set(list_storage(busy_table, storage_roots(busy_table)))
    referenced = reachable_files(busy_table).paths

    assert referenced <= storage, (
        f"{len(referenced - storage)} referenced file(s) absent from the listing -- "
        "the two sides are keyed differently"
    )
