"""Merge-on-read compaction.

The defining property of a MoR table is that a data file's manifest
``record_count`` counts physical rows, including ones a delete file removes. Any
row-preservation check written against ``record_count`` will therefore reject
every correct MoR rewrite -- which is what this module exists to prevent
regressing.
"""

from __future__ import annotations

import pytest

from icemaint import CompactionConfig, MemoryMode, TableCompactor
from icemaint.compactor import CompactionBlocked
from icemaint.profile import profile_table


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
