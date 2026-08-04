"""Orphan-file removal.

The tests that matter most are the ones asserting it **refuses to run**. This
operation deletes files based on a computed set, and it is enabled by default,
so the invariants are the whole justification for that default.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pytest

from zamboni.orphans import (
    OrphanCleaner,
    OrphanCleanupAborted,
    list_storage,
    storage_roots,
)
from zamboni.reachable import Category, ReachableSet, reachable_files

from .conftest import SCHEMA, batch

DAY = dt.timedelta(days=1)


@pytest.fixture
def table(session):
    tbl = session.catalog.create_table("db.orph", schema=SCHEMA, properties={"format-version": "2"})
    for i in range(3):
        tbl.append(batch(i * 10, 10))
    return session.catalog.load_table("db.orph")


def plant(tbl, name: str, *, age_days: float) -> Path:
    """Write a file nothing references, aged by setting its mtime."""
    path = Path(tbl.location().replace("file://", "")) / "data" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not a real parquet file")
    when = (dt.datetime.now(dt.UTC) - dt.timedelta(days=age_days)).timestamp()
    os.utime(path, (when, when))
    return path


# -- finding orphans -----------------------------------------------------


def test_a_clean_table_has_no_orphans(table):
    result = OrphanCleaner(dry_run=True).run(table)
    assert result.orphans == 0
    assert result.scanned == result.referenced


def test_an_old_unreferenced_file_is_deleted(table):
    planted = plant(table, "stray-old.parquet", age_days=10)
    assert planted.exists()

    result = OrphanCleaner(older_than_days=3).run(table)

    assert result.orphans == 1
    assert result.deleted == 1
    assert not planted.exists()


def test_a_recent_unreferenced_file_survives_the_guard(table):
    """The guard's whole job: a writer's in-flight output must not be deleted."""
    planted = plant(table, "stray-new.parquet", age_days=0.5)

    result = OrphanCleaner(older_than_days=3).run(table)

    assert result.orphans == 1
    assert result.too_young == 1
    assert result.deleted == 0
    assert planted.exists(), "an in-flight file was deleted"


def test_zero_day_guard_deletes_recent_orphans(table):
    """What `--reclaim-now` does, and why it is not the default."""
    planted = plant(table, "stray-now.parquet", age_days=0)
    result = OrphanCleaner(older_than_days=0).run(table)
    assert result.deleted == 1
    assert not planted.exists()


def test_dry_run_deletes_nothing(table):
    planted = plant(table, "stray-dry.parquet", age_days=10)
    result = OrphanCleaner(older_than_days=3, dry_run=True).run(table)

    assert result.dry_run and result.deleted == 1
    assert planted.exists(), "a dry run removed a file"


def test_live_data_is_never_touched(table):
    before = reachable_files(table).paths
    plant(table, "stray.parquet", age_days=10)

    OrphanCleaner(older_than_days=0).run(table)

    tbl = table
    tbl.refresh()
    after = reachable_files(tbl)
    assert after.paths == before
    assert tbl.scan().to_arrow().num_rows == 30
    # Everything still referenced must still exist.
    storage = list_storage(tbl, storage_roots(tbl))
    assert not after.paths - set(storage)


def test_current_metadata_is_never_a_candidate(table, monkeypatch):
    """Even if reachability somehow omits it, the live pointer stays."""
    real = reachable_files

    def without_metadata(tbl):
        full = real(tbl)
        trimmed = dict(full.by_category)
        trimmed[Category.METADATA] = frozenset(
            p for p in trimmed[Category.METADATA] if "metadata.json" not in p
        )
        return ReachableSet(by_category=trimmed)

    monkeypatch.setattr("zamboni.orphans.reachable_files", without_metadata)

    current = Path(table.metadata_location.replace("file://", ""))
    assert current.exists()
    # The metadata category is now empty, so the invariant should abort first.
    with pytest.raises(OrphanCleanupAborted, match="no reachable table metadata"):
        OrphanCleaner(older_than_days=0).run(table)
    assert current.exists()


# -- the invariants ------------------------------------------------------


@pytest.mark.parametrize(
    "category,message",
    [
        (Category.DATA, "no reachable data and delete files"),
        (Category.METADATA, "no reachable table metadata"),
        (Category.MANIFEST, "no reachable manifests"),
        (Category.MANIFEST_LIST, "no reachable manifest lists"),
    ],
)
def test_an_empty_reachable_category_aborts(table, monkeypatch, category, message):
    """A silently-dropped category must stop the run, not delete everything."""
    real = reachable_files

    def crippled(tbl):
        full = real(tbl)
        trimmed = dict(full.by_category)
        trimmed[category] = frozenset()
        return ReachableSet(by_category=trimmed)

    monkeypatch.setattr("zamboni.orphans.reachable_files", crippled)

    before = set(list_storage(table, storage_roots(table)))
    with pytest.raises(OrphanCleanupAborted, match=message):
        OrphanCleaner(older_than_days=0).run(table)
    assert set(list_storage(table, storage_roots(table))) == before, "files were deleted anyway"


def test_a_missing_referenced_file_aborts(table):
    """A partial listing makes the complement meaningless."""
    live = sorted(reachable_files(table).by_category[Category.DATA])[0]
    Path(live).unlink()

    with pytest.raises(OrphanCleanupAborted, match="missing from the listing"):
        OrphanCleaner(older_than_days=0).run(table)


def test_negative_guard_is_rejected():
    with pytest.raises(ValueError, match="older_than_days must be >= 0"):
        OrphanCleaner(older_than_days=-1)


# -- scope ---------------------------------------------------------------


def test_roots_are_scoped_to_the_table(table):
    roots = storage_roots(table)
    assert roots == [table.location().rstrip("/")]
    # Never the warehouse: a sibling table's files must be unreachable.
    assert "hims" not in roots[0] or table.location() in roots[0]


def test_roots_include_redirected_paths(session):
    tbl = session.catalog.create_table(
        "db.redirected",
        schema=SCHEMA,
        properties={"format-version": "2", "write.data.path": "file:///tmp/elsewhere/data"},
    )
    roots = storage_roots(tbl)
    assert any("elsewhere" in r for r in roots)


def test_nested_roots_are_not_scanned_twice(session):
    tbl = session.catalog.create_table(
        "db.nested",
        schema=SCHEMA,
        properties={"format-version": "2"},
    )
    location = tbl.location().rstrip("/")
    tbl = (
        tbl.transaction()
        .set_properties(**{"write.data.path": f"{location}/data"})
        .commit_transaction()
    )
    roots = storage_roots(tbl)
    assert roots == [location], f"the nested data path should collapse into the table root: {roots}"


# -- listing against a non-pyarrow FileIO --------------------------------


class _RecordingFsspecIO:
    """The FileIO shape Lakekeeper forces: fsspec, no `_initialize_fs`."""

    def __init__(self, files: dict):
        self.asked_for: list[str] = []
        self._files = files

    def get_fs(self, scheme: str):
        outer = self

        class _FS:
            def find(self, path, detail=False):
                outer.asked_for.append(path)
                return outer._files

        return _FS()


def test_fsspec_listing_uses_the_bucket_qualified_prefix_once(table, monkeypatch):
    """`parse_location` already puts the bucket in the path.

    Prefixing the netloc again asks S3 for `bucket/bucket/key` and lists
    nothing -- and because the unreferenced set is the complement of the
    listing, "nothing" would look like "everything is referenced". The live
    Lakekeeper run hid this behind the signer refusing the request outright.
    """
    io = _RecordingFsspecIO(
        {
            "warehouse/acme/tbl/data/a.parquet": {
                "type": "file",
                "size": 10,
                "LastModified": dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
            }
        }
    )
    monkeypatch.setattr(table, "io", io, raising=False)

    found = list_storage(table, ["s3://warehouse/acme/tbl"])

    assert io.asked_for == ["warehouse/acme/tbl"]
    assert list(found) == ["warehouse/acme/tbl/data/a.parquet"]


def test_fsspec_entries_carry_a_deletable_location(table, monkeypatch):
    """The key and the delete target differ on object storage."""
    io = _RecordingFsspecIO(
        {"warehouse/acme/tbl/data/a.parquet": {"type": "file", "size": 10, "LastModified": None}}
    )
    monkeypatch.setattr(table, "io", io, raising=False)

    entry = next(iter(list_storage(table, ["s3://warehouse/acme/tbl"]).values()))

    assert entry.path == "warehouse/acme/tbl/data/a.parquet"
    assert entry.location == "s3://warehouse/acme/tbl/data/a.parquet"


def test_an_io_that_can_neither_list_nor_be_understood_aborts(table, monkeypatch):
    monkeypatch.setattr(table, "io", object(), raising=False)

    with pytest.raises(OrphanCleanupAborted, match="neither a pyarrow nor an fsspec"):
        list_storage(table, ["s3://warehouse/acme/tbl"])


# -- ZMBNI-507: another table's files inside this table's location -------


def _rename_then_recreate(session):
    """Two live tables, different names, one location -- via ordinary calls.

    No misconfiguration and no explicit `location=` argument. A table's default
    location is derived from its name *at creation time*; `rename_table` rewrites
    the catalog entry and moves no files; creating the freed name derives the
    same default location again. Four calls, and the warehouse now has two
    tables pointing at one directory.
    """
    old = session.catalog.create_table(
        "db.orders", schema=SCHEMA, properties={"format-version": "2"}
    )
    old.append(batch(0, 5))
    old.append(batch(5, 5))
    session.catalog.rename_table("db.orders", "db.orders_v2")
    old = session.catalog.load_table("db.orders_v2")

    new = session.catalog.create_table(
        "db.orders", schema=SCHEMA, properties={"format-version": "2"}
    )
    new.append(batch(100, 5))
    return old, new


def test_rename_then_recreate_really_does_collide(session):
    """Pins the premise. If a catalog ever stops doing this, the guard below
    still holds but this test explains why it was written."""
    old, new = _rename_then_recreate(session)

    assert old.location() == new.location()
    assert old.name() != new.name()


def test_maintaining_one_table_refuses_to_delete_a_colocated_table(session):
    """The defect: without the guard this deleted every file of `db.orders_v2`,
    including its current metadata, leaving it unreadable."""
    old, new = _rename_then_recreate(session)
    victim_files = {f["file_path"] for f in old.inspect.files().to_pylist()}
    assert victim_files

    with pytest.raises(OrphanCleanupAborted, match=r"db\.orders_v2"):
        OrphanCleaner(older_than_days=0).run(new)

    for path in victim_files:
        assert Path(path.replace("file://", "")).exists(), f"deleted a live file: {path}"
    assert old.scan().to_arrow().num_rows == 10


def test_the_colocation_guard_names_what_to_do(session):
    _, new = _rename_then_recreate(session)

    with pytest.raises(OrphanCleanupAborted) as exc:
        OrphanCleaner(older_than_days=0).run(new)

    message = str(exc.value)
    assert "shares" in message or "share" in message
    assert "db.orders_v2" in message


def test_a_table_alone_in_its_location_is_unaffected(table):
    """The guard must not fire on the ordinary case, which is every other test
    in this file -- asserted once explicitly so a too-broad guard is obvious."""
    result = OrphanCleaner(older_than_days=0, dry_run=True).run(table)

    assert result.scanned > 0
