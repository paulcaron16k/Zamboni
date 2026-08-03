"""Dangling delete-file removal.

The operation is metadata-only, which makes it easy to get subtly wrong: an
untracked removal reads fine until an incremental consumer disagrees with the
snapshot summary. The tests that matter are the ones checking what the *new
snapshot* says, not just what a scan returns.
"""

from __future__ import annotations

import pytest
from pyiceberg.manifest import DataFileContent, ManifestContent

from zamboni import CompactionConfig, TableCompactor
from zamboni.deletes import (
    DanglingDeleteCleaner,
    DanglingDeleteError,
    find_dangling,
)
from zamboni.profile import profile_table


def delete_files(tbl) -> list:
    """Live delete files, straight off the current snapshot's manifests."""
    snapshot = tbl.current_snapshot()
    found = []
    for manifest in snapshot.manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            if entry.data_file.content != DataFileContent.DATA:
                found.append(entry.data_file)
    return found


def live_ids(tbl) -> list[int]:
    tbl.refresh()
    return sorted(tbl.scan().to_arrow()["id"].to_pylist())


# -- detection -----------------------------------------------------------


def test_a_delete_that_still_applies_is_not_dangling(mor_table):
    """The most important negative: nothing has been compacted yet."""
    report = find_dangling(mor_table.table)

    assert report.delete_files == 1
    assert report.dangling_files == 0
    assert report.removable == []


def test_a_table_with_no_deletes_reports_nothing(unpartitioned):
    report = find_dangling(unpartitioned)
    assert report.delete_files == 0
    assert report.removable == []
    assert "no delete files" in report.describe()


def test_compaction_makes_the_delete_dangling(session, mor_table):
    """Compaction applies the deletes and supersedes the data file they name.

    That is exactly when a delete file stops applying -- and until now, exactly
    when it became permanently referenced dead weight.
    """
    assert find_dangling(mor_table.table).dangling_files == 0

    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    tbl = session.table("db.mor")

    report = find_dangling(tbl)
    assert report.delete_files == 1
    assert report.dangling_files == 1
    assert len(report.removable) == 1


# -- removal -------------------------------------------------------------


def test_removal_drops_the_delete_file_from_the_table(session, mor_table):
    before = live_ids(mor_table.table)
    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    tbl = session.table("db.mor")
    assert delete_files(tbl), "nothing to remove; the fixture changed"

    result = DanglingDeleteCleaner().run(tbl)

    assert result.removed == 1
    tbl = session.table("db.mor")
    assert delete_files(tbl) == []
    assert profile_table(tbl).position_delete_files == 0
    assert live_ids(tbl) == before, "removing a dangling delete changed the data"


def test_removal_is_recorded_in_the_snapshot_summary(session, mor_table):
    """Upstream's ``_deleted_entries`` filters to DATA, so without the override
    the file would vanish from the manifest list with no snapshot saying so."""
    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    tbl = session.table("db.mor")

    DanglingDeleteCleaner().run(tbl)

    summary = session.table("db.mor").current_snapshot().summary
    # The two counters mean different things: one delete *file* was removed, and
    # it carried three delete *rows*. Conflating them is an easy way to write a
    # summary that quietly disagrees with the manifests.
    assert summary["removed-delete-files"] == "1"
    assert summary["removed-position-deletes"] == "3"
    assert summary.operation.value == "replace"
    assert summary["zamboni.operation"] == "remove-dangling-deletes"


def test_removal_leaves_the_file_on_disk_for_time_travel(session, mor_table):
    """Metadata only -- older snapshots still reference it, as with compaction."""
    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    tbl = session.table("db.mor")
    path = delete_files(tbl)[0].file_path

    DanglingDeleteCleaner().run(tbl)

    assert tbl.io.new_input(path).exists(), "a metadata-only operation deleted a file"


def test_dry_run_changes_nothing(session, mor_table):
    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    tbl = session.table("db.mor")
    before = tbl.current_snapshot().snapshot_id

    result = DanglingDeleteCleaner(dry_run=True).run(tbl)

    assert result.dry_run and result.removed == 1
    tbl = session.table("db.mor")
    assert tbl.current_snapshot().snapshot_id == before
    assert delete_files(tbl), "a dry run removed the delete file"


def test_a_delete_that_still_applies_is_never_removed(session, mor_table):
    """The safety property: removing a live delete resurrects deleted rows."""
    tbl = mor_table.table
    before = live_ids(tbl)
    assert len(before) == 37

    result = DanglingDeleteCleaner().run(tbl)

    assert result.removed == 0
    tbl = session.table("db.mor")
    assert live_ids(tbl) == before
    assert set(mor_table.deleted_ids).isdisjoint(live_ids(tbl))


def test_running_twice_is_a_no_op(session, mor_table):
    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    tbl = session.table("db.mor")
    DanglingDeleteCleaner().run(tbl)
    snapshot = session.table("db.mor").current_snapshot().snapshot_id

    result = DanglingDeleteCleaner().run(session.table("db.mor"))

    assert result.removed == 0
    assert session.table("db.mor").current_snapshot().snapshot_id == snapshot


# -- the manifest-content limitation -------------------------------------


def test_a_partially_dangling_manifest_is_retained_not_rewritten(session, mor_table, monkeypatch):
    """PyIceberg cannot write a delete manifest, so a mixed one must be left.

    Rewriting it through ``write_manifest`` would stamp ``content: data`` and a
    reader would then treat position deletes as rows. Reclaiming less is the
    only safe answer available.
    """
    from zamboni.deletes import DeleteManifest

    TableCompactor(session, "db.mor", CompactionConfig()).execute()
    tbl = session.table("db.mor")

    # Pretend the manifest also holds a delete file that still applies.
    real = DeleteManifest.fully_dangling.fget
    monkeypatch.setattr(
        DeleteManifest,
        "fully_dangling",
        property(lambda self: False if self.dangling else real(self)),
    )

    report = find_dangling(tbl)
    assert report.dangling_files == 1
    assert report.removable == []
    assert len(report.stuck) == 1
    assert "cannot write a delete manifest" in report.describe()

    result = DanglingDeleteCleaner().run(tbl)
    assert result.removed == 0
    assert delete_files(session.table("db.mor")), "a stuck delete file was removed anyway"


def test_the_producer_refuses_to_rewrite_a_delete_manifest(session, partitioned):
    """The guard behind the planning contract, exercised directly.

    Planning only ever hands whole manifests to the producer, so this path
    should be unreachable -- which is precisely why it is asserted rather than
    trusted. Two partitions give one delete manifest holding two entries;
    removing one of them is the partial rewrite that would corrupt it.
    """
    from pyiceberg.table.snapshots import Operation

    from zamboni.deletes import _RemoveDeleteFiles
    from zamboni.testing import write_position_deletes

    by_partition: dict = {}
    for task in partitioned.scan().plan_files():
        by_partition.setdefault(str(task.file.partition), task.file.file_path)
    assert len(by_partition) == 2, "the fixture no longer has two partitions"

    assert write_position_deletes(partitioned, {p: [0] for p in by_partition.values()}) == 2
    tbl = session.table("db.partitioned")

    manifests = [
        m
        for m in tbl.current_snapshot().manifests(io=tbl.io)
        if m.content == ManifestContent.DELETES
    ]
    assert len(manifests) == 1, "expected both delete files in one delete manifest"
    entries = manifests[0].fetch_manifest_entry(io=tbl.io, discard_deleted=True)
    assert len(entries) == 2

    with pytest.raises(DanglingDeleteError, match="partially-emptied"), tbl.transaction() as txn:
        producer = _RemoveDeleteFiles(
            operation=Operation.REPLACE, transaction=txn, io=tbl.io, branch="main"
        )
        # One of two entries removed: exactly the case upstream would rewrite
        # through write_manifest and label a data manifest.
        producer._deleted_data_files = {entries[0].data_file}
        producer._existing_manifests()


def test_delete_files_land_in_a_delete_manifest(mor_table):
    """The simulation has to be faithful at the manifest level too.

    Appending through PyIceberg's own producer puts delete files in a manifest
    labelled ``content: data``. PyIceberg and DuckDB both dispatch on the
    entry's content and cope, so nothing here would have failed -- an engine
    that prunes on manifest content would silently apply no deletes at all.
    """
    tbl = mor_table.table
    contents = {m.content for m in tbl.current_snapshot().manifests(io=tbl.io)}
    assert ManifestContent.DELETES in contents

    for manifest in tbl.current_snapshot().manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            is_delete = entry.data_file.content != DataFileContent.DATA
            assert is_delete == (manifest.content == ManifestContent.DELETES), (
                f"{manifest.manifest_path} content={manifest.content} holds "
                f"a file with content={entry.data_file.content}"
            )
