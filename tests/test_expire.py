"""Snapshot expiry.

The retention algorithm is tested as a pure function against the Iceberg spec's
five steps, because PyIceberg implements almost none of them and a mistake here
either deletes history that should have been kept or keeps everything forever.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import Operation, Snapshot, Summary

from zamboni.expire import (
    DEFAULT_MAX_SNAPSHOT_AGE_MS,
    RetentionPolicy,
    SnapshotExpirer,
    decide_retention,
    files_of_snapshots,
)
from zamboni.reachable import reachable_files

from .conftest import SCHEMA, batch
from .test_reachable import list_storage

NOW = dt.datetime(2026, 8, 2, 12, 0, tzinfo=dt.UTC)
DAY_MS = 24 * 60 * 60 * 1000


def snap(snapshot_id: int, parent: int | None, days_ago: float) -> Snapshot:
    return Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent,
        manifest_list=f"file:///w/t/metadata/snap-{snapshot_id}.avro",
        sequence_number=snapshot_id,
        timestamp_ms=int(NOW.timestamp() * 1000) - int(days_ago * DAY_MS),
        summary=Summary(operation=Operation.APPEND),
        schema_id=0,
    )


class FakeMetadata:
    """Just enough metadata for the retention algorithm.

    `ancestors_of` walks parents via `snapshot_by_id`, so the double has to
    provide it as well as the two attributes the algorithm reads directly.
    """

    def __init__(self, snapshots, refs):
        self.snapshots = snapshots
        self.refs = refs

    def snapshot_by_id(self, snapshot_id):
        return next((s for s in self.snapshots if s.snapshot_id == snapshot_id), None)


def chain(ages: list[float]) -> list[Snapshot]:
    """A linear branch, oldest first; ``ages`` are days before NOW."""
    out = []
    for i, age in enumerate(ages, start=1):
        out.append(snap(i, i - 1 if i > 1 else None, age))
    return out


def main_ref(snapshot_id: int, **kwargs) -> dict:
    return {
        MAIN_BRANCH: SnapshotRef(
            snapshot_id=snapshot_id, snapshot_ref_type=SnapshotRefType.BRANCH, **kwargs
        )
    }


# -- the retention algorithm --------------------------------------------


def test_recent_snapshots_are_all_retained():
    snaps = chain([3, 2, 1, 0])
    meta = FakeMetadata(snaps, main_ref(4))
    decision = decide_retention(meta, RetentionPolicy(max_snapshot_age_ms=5 * DAY_MS), now=NOW)
    assert decision.expire == frozenset()
    assert len(decision.retain) == 4


def test_snapshots_older_than_max_age_are_expired():
    snaps = chain([30, 20, 10, 0])
    meta = FakeMetadata(snaps, main_ref(4))
    decision = decide_retention(
        meta, RetentionPolicy(max_snapshot_age_ms=15 * DAY_MS, min_snapshots_to_keep=1), now=NOW
    )
    # Walking back from the head: 0d and 10d are within 15 days; 20d is older
    # and beyond min_snapshots_to_keep, so the walk stops there.
    assert decision.retain == frozenset({4, 3})
    assert decision.expire == frozenset({1, 2})


def test_min_snapshots_to_keep_beats_age():
    """Step 4.2: an old snapshot survives while it is within the minimum."""
    snaps = chain([100, 90, 80, 70])
    meta = FakeMetadata(snaps, main_ref(4))
    decision = decide_retention(
        meta, RetentionPolicy(max_snapshot_age_ms=1 * DAY_MS, min_snapshots_to_keep=3), now=NOW
    )
    assert decision.retain == frozenset({4, 3, 2}), "the three newest must survive despite age"
    assert decision.expire == frozenset({1})


def test_the_head_always_survives_however_old():
    snaps = chain([365])
    meta = FakeMetadata(snaps, main_ref(1))
    decision = decide_retention(
        meta, RetentionPolicy(max_snapshot_age_ms=1 * DAY_MS, min_snapshots_to_keep=1), now=NOW
    )
    assert decision.expire == frozenset()


def test_tag_and_branch_heads_are_retained():
    """Step 3: every ref head is retained even if nothing else points at it."""
    snaps = chain([50, 40, 30, 0])
    refs = main_ref(4)
    refs["v1"] = SnapshotRef(snapshot_id=1, snapshot_ref_type=SnapshotRefType.TAG)
    meta = FakeMetadata(snaps, refs)
    decision = decide_retention(
        meta, RetentionPolicy(max_snapshot_age_ms=1 * DAY_MS, min_snapshots_to_keep=1), now=NOW
    )
    assert 1 in decision.retain, "a tagged snapshot must not be expired"


def test_per_ref_overrides_beat_the_table_policy():
    snaps = chain([100, 90, 80, 70])
    meta = FakeMetadata(snaps, main_ref(4, min_snapshots_to_keep=4))
    decision = decide_retention(
        meta, RetentionPolicy(max_snapshot_age_ms=1 * DAY_MS, min_snapshots_to_keep=1), now=NOW
    )
    assert decision.expire == frozenset(), "the ref's own minimum should win"


def test_stale_refs_are_reported_but_never_main():
    snaps = chain([100, 0])
    refs = main_ref(2)
    refs["old_branch"] = SnapshotRef(snapshot_id=1, snapshot_ref_type=SnapshotRefType.BRANCH)
    meta = FakeMetadata(snaps, refs)
    decision = decide_retention(
        meta,
        RetentionPolicy(max_snapshot_age_ms=5 * DAY_MS, max_ref_age_ms=10 * DAY_MS),
        now=NOW,
    )
    assert decision.stale_refs == frozenset({"old_branch"})
    # Reported, not dropped -- so its snapshot is still retained.
    assert 1 in decision.retain


def test_main_is_never_stale():
    snaps = chain([500])
    meta = FakeMetadata(snaps, main_ref(1))
    decision = decide_retention(meta, RetentionPolicy(max_ref_age_ms=1 * DAY_MS), now=NOW)
    assert decision.stale_refs == frozenset()


# -- policy resolution ---------------------------------------------------


def test_policy_resolution_order():
    props = {
        "history.expire.max-snapshot-age-ms": str(9 * DAY_MS),
        "history.expire.min-snapshots-to-keep": "7",
    }

    from_props = RetentionPolicy.resolve(props)
    assert from_props.max_snapshot_age_ms == 9 * DAY_MS
    assert from_props.min_snapshots_to_keep == 7

    from_config = RetentionPolicy.resolve(props, max_snapshot_age_days=2, min_snapshots_to_keep=3)
    assert from_config.max_snapshot_age_ms == 2 * DAY_MS, "config must beat the table property"
    assert from_config.min_snapshots_to_keep == 3

    from_default = RetentionPolicy.resolve({})
    assert from_default.max_snapshot_age_ms == DEFAULT_MAX_SNAPSHOT_AGE_MS


def test_bad_table_property_is_rejected():
    with pytest.raises(ValueError, match="not an integer"):
        RetentionPolicy.resolve({"history.expire.max-snapshot-age-ms": "soon"})


# -- against a real table ------------------------------------------------


@pytest.fixture
def aged(session):
    """Six appends; the table's own snapshot timestamps are all 'now'."""
    tbl = session.catalog.create_table("db.aged", schema=SCHEMA, properties={"format-version": "2"})
    for i in range(6):
        tbl.append(batch(i * 10, 10))
    return session.catalog.load_table("db.aged")


def test_expiry_deletes_only_what_it_orphans(session, aged):
    before_rows = aged.scan().to_arrow().num_rows
    before_disk = list_storage(aged)

    # Age is measured from snapshot timestamps, which are all "now" here, so
    # drive it with min_snapshots_to_keep instead: keep 2, expire the rest.
    policy = RetentionPolicy(max_snapshot_age_ms=0, min_snapshots_to_keep=2)
    result = SnapshotExpirer(policy).run(aged)

    assert result.expired_snapshots == 4
    assert result.deleted_files > 0
    assert result.failed_deletes == 0

    tbl = session.table("db.aged")
    assert tbl.scan().to_arrow().num_rows == before_rows, "expiry changed the data"
    assert len(tbl.metadata.snapshots) == 2

    after_disk = list_storage(tbl)
    assert len(after_disk) < len(before_disk), "nothing was reclaimed"
    # Whatever survived must still be fully referenced.
    assert not reachable_files(tbl).paths - after_disk


def test_expiry_never_deletes_a_file_a_surviving_snapshot_needs(session, aged):
    policy = RetentionPolicy(max_snapshot_age_ms=0, min_snapshots_to_keep=2)
    SnapshotExpirer(policy).run(aged)

    tbl = session.table("db.aged")
    reachable = reachable_files(tbl)
    on_disk = list_storage(tbl)
    missing = reachable.paths - on_disk
    assert not missing, f"expiry deleted {len(missing)} file(s) still referenced"


def test_time_travel_still_works_for_surviving_snapshots(session, aged):
    policy = RetentionPolicy(max_snapshot_age_ms=0, min_snapshots_to_keep=2)
    SnapshotExpirer(policy).run(aged)

    tbl = session.table("db.aged")
    survivor = sorted(tbl.metadata.snapshots, key=lambda s: s.timestamp_ms)[0]
    rows = tbl.scan(snapshot_id=survivor.snapshot_id).to_arrow()
    assert rows.num_rows > 0, "a retained snapshot is no longer readable"


def test_dry_run_changes_nothing(session, aged):
    before_snaps = len(aged.metadata.snapshots)
    before_disk = list_storage(aged)

    policy = RetentionPolicy(max_snapshot_age_ms=0, min_snapshots_to_keep=2)
    result = SnapshotExpirer(policy, dry_run=True).run(aged)

    assert result.dry_run and result.expired_snapshots == 4
    assert result.deleted_files > 0, "a dry run should still report what it would remove"

    tbl = session.table("db.aged")
    assert len(tbl.metadata.snapshots) == before_snaps
    assert list_storage(tbl) == before_disk


def test_nothing_to_expire_is_a_clean_no_op(session, aged):
    policy = RetentionPolicy(max_snapshot_age_ms=365 * DAY_MS, min_snapshots_to_keep=1)
    result = SnapshotExpirer(policy).run(aged)
    assert result.expired_snapshots == 0
    assert result.deleted_files == 0


def test_files_of_snapshots_covers_manifests_and_lists(aged):
    ids = frozenset({s.snapshot_id for s in aged.metadata.snapshots})
    files = files_of_snapshots(aged, ids)
    assert any(p.endswith(".avro") for p in files.values()), "no manifests or manifest lists"
    assert any(p.endswith(".parquet") for p in files.values()), "no data files"
    # Keys are canonical, values are the original locations FileIO.delete wants.
    assert all(not k.startswith("file://") for k in files)
    assert any(v.startswith("file://") for v in files.values())
