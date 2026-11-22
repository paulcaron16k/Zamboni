"""Merge-on-read compaction.

The defining property of a MoR table is that a data file's manifest
``record_count`` counts physical rows, including ones a delete file removes. Any
row-preservation check written against ``record_count`` will therefore reject
every correct MoR rewrite -- which is what this module exists to prevent
regressing.
"""

from __future__ import annotations

import pytest

from zamboni import CompactionConfig, MemoryMode, TableCompactor
from zamboni.compactor import CompactionBlocked
from zamboni.profile import profile_table


def live_ids(tbl) -> list[int]:
    tbl.refresh()
    return sorted(tbl.scan().to_arrow()["id"].to_pylist())


def test_fixture_really_is_merge_on_read(mor_table):
    profile = profile_table(mor_table.table)
    assert profile.position_delete_files == 1
    # The whole hazard in one assertion: physical != live.
    assert sum(f.record_count for f in profile.live_files) == 40
    assert mor_table.table.scan().count() == 37


@pytest.mark.parametrize("mode", [MemoryMode.IN_MEMORY, MemoryMode.CHUNKED])
def test_compaction_preserves_live_rows_not_physical_rows(session, mor_table, mode):
    before = live_ids(mor_table.table)
    assert len(before) == 37
    assert set(mor_table.deleted_ids).isdisjoint(before)

    result = TableCompactor(session, "db.mor", CompactionConfig(memory_mode=mode)).execute()

    assert result.rewritten_data_files == 4
    tbl = session.table("db.mor")
    assert live_ids(tbl) == before

    after = profile_table(tbl)
    assert len(after.live_files) == 1
    # Deletes are materialised into the rewritten file, so its physical count
    # is now the live count.
    assert after.live_files[0].record_count == 37


def test_deleted_rows_do_not_come_back(session, mor_table):
    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    ids = live_ids(session.table("db.mor"))
    assert set(mor_table.deleted_ids).isdisjoint(ids)
    assert len(ids) == 37


def test_delete_files_are_reported_as_dangling(session, mor_table):
    result = TableCompactor(session, "db.mor", CompactionConfig()).execute()
    assert result.dangling_delete_files == 1
    assert any("dangling-delete-files" in w for w in result.warnings)


def test_block_policy_refuses_a_mor_table(session, mor_table):
    config = CompactionConfig(dangling_delete_policy="block")
    with pytest.raises(CompactionBlocked, match="dangling-delete-files"):
        TableCompactor(session, "db.mor", config).execute()


# -- compaction counts only what it left dangling (#289) -----------------------


@pytest.fixture
def partly_compacted(session, partitioned):
    """Partitions a and b compact (4 files each); c is one file and is skipped.

    One position delete on a file in each. After compaction the deletes on a
    and b no longer apply to anything; c's still applies to its file.
    """
    from tests.conftest import add_position_deletes, batch

    partitioned.append(batch(900, 10, "c"))
    tbl = session.catalog.load_table("db.partitioned")
    for category in ("a", "b", "c"):
        target = sorted(
            t.file.file_path for t in tbl.scan(row_filter=f"category == '{category}'").plan_files()
        )[0]
        add_position_deletes(tbl, target, [0])
        tbl = session.catalog.load_table("db.partitioned")
    assert profile_table(tbl).position_delete_files == 3
    return tbl


@pytest.mark.parametrize("dry_run", [False, True], ids=["commit", "dry-run"])
def test_compaction_counts_only_the_deletes_it_left_dangling(session, partly_compacted, dry_run):
    """The count agrees with what remove-dangling-deletes then finds.

    Before #289 it was every position delete file in the table, read before
    anything was rewritten, so a delete still applying to a partition
    compaction skipped was reported as dangling.
    """
    from zamboni.deletes import find_dangling

    result = TableCompactor(session, "db.partitioned", CompactionConfig()).execute(dry_run=dry_run)

    assert result.rewritten_data_files == 8, "precondition: a and b compacted, c left alone"
    if dry_run:
        # What a commit would leave: the same table compacted for real.
        TableCompactor(session, "db.partitioned", CompactionConfig()).execute()
    expected = find_dangling(session.table("db.partitioned")).dangling_files
    assert expected == 2, "precondition: a's and b's deletes dangle, c's still applies"
    assert result.dangling_delete_files == expected
    assert f"{expected} delete file(s) now dangling" in result.describe()
