"""Grouping and skip decisions."""

from __future__ import annotations

import pytest

from zamboni import CompactionConfig, CompactionPlanner, TableCompactor
from zamboni.config import DEFAULT_TARGET_FILE_SIZE_BYTES, resolve_target_file_size
from zamboni.profile import profile_table


def test_min_input_files_skips_small_partitions(session, unpartitioned):
    planner = CompactionPlanner(CompactionConfig(min_input_files=99))
    plan = planner.plan(unpartitioned, profile_table(unpartitioned))

    assert plan.is_empty
    assert len(plan.skipped) == 1
    # A bounded plan must say what it left out; silent truncation reads as
    # full coverage.
    assert "min_input_files=99" in plan.skipped[0][1]


def test_files_at_target_size_are_left_alone(session, unpartitioned):
    # Every test file is far under 1 byte... so make the target tiny instead.
    planner = CompactionPlanner(CompactionConfig(target_file_size_bytes=100))
    plan = planner.plan(unpartitioned, profile_table(unpartitioned))
    assert plan.is_empty


def test_rewrite_all_ignores_size_and_count_thresholds(session, unpartitioned):
    planner = CompactionPlanner(
        CompactionConfig(target_file_size_bytes=100, min_input_files=99, rewrite_all=True)
    )
    plan = planner.plan(unpartitioned, profile_table(unpartitioned))
    assert len(plan.groups) == 1
    assert len(plan.groups[0].files) == 6


def test_partitioned_plan_has_one_group_per_partition(session, partitioned):
    plan = CompactionPlanner(CompactionConfig()).plan(partitioned, profile_table(partitioned))
    assert len(plan.groups) == 2
    for group in plan.groups:
        assert len({tuple(f.partition) for f in group.files}) == 1
        assert len({f.spec_id for f in group.files}) == 1


def test_dry_run_changes_nothing(session, unpartitioned):
    before = profile_table(unpartitioned).snapshot_id
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute(dry_run=True)

    assert result.rewritten_data_files == 0
    assert profile_table(session.table("db.unpartitioned")).snapshot_id == before


@pytest.mark.parametrize(
    "properties,expected",
    [
        ({}, DEFAULT_TARGET_FILE_SIZE_BYTES),
        ({"write.target-file-size-bytes": "555"}, 555),
        # The parquet-specific property wins over the generic one.
        (
            {"write.parquet.target-file-size-bytes": "777", "write.target-file-size-bytes": "555"},
            777,
        ),
    ],
)
def test_target_size_resolution_order(properties, expected):
    assert resolve_target_file_size(CompactionConfig(), properties) == expected


def test_explicit_config_beats_table_properties():
    config = CompactionConfig(target_file_size_bytes=999)
    assert resolve_target_file_size(config, {"write.target-file-size-bytes": "555"}) == 999


def test_rejects_nonsense_config():
    with pytest.raises(ValueError, match="target_file_size_bytes"):
        CompactionConfig(target_file_size_bytes=1)
    with pytest.raises(ValueError, match="min_input_files"):
        CompactionConfig(min_input_files=0)
    with pytest.raises(ValueError, match="snapshot_operation"):
        CompactionConfig(snapshot_operation="rewrite")


# -- the recency floor (#78) ------------------------------------------------


@pytest.fixture
def daily(session):
    """Day-partitioned, 3 files each in today, yesterday, D-2 and D-3."""
    import datetime as dt

    import pyarrow as pa
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import NestedField, TimestamptzType

    schema = Schema(NestedField(1, "ts", TimestamptzType(), required=False))
    arrow = pa.schema([pa.field("ts", pa.timestamp("us", tz="UTC"))])
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="ts_day")
    )
    tbl = session.catalog.create_table(
        "db.daily", schema=schema, partition_spec=spec, properties={"format-version": "2"}
    )
    now = dt.datetime.now(dt.UTC)
    for offset in (0, 1, 2, 3):
        for _ in range(3):
            tbl.append(pa.table({"ts": [now - dt.timedelta(days=offset)]}, schema=arrow))
    return session.catalog.load_table("db.daily")


def _offsets(plan, today_index):
    """Which day-offsets from today the plan actually compacts."""
    return sorted(today_index - int(next(iter(g.partition))) for g in plan.groups)


def test_the_default_floor_holds_today_and_yesterday(session, daily):
    """The default exists for a daily loader extracting *yesterday* at 02:00.

    That run writes into yesterday's partition, not today's, so holding only the
    open window would compact the partition being actively written. A floor of 1
    counts the most recently closed window as still at risk, which is why the
    default holds two partitions rather than one.
    """
    import datetime as dt

    from zamboni.windows import current_index

    assert CompactionConfig().skip_partitions_newer_than_windows == 1

    now = dt.datetime.now(dt.UTC)
    plan = CompactionPlanner(CompactionConfig()).plan(daily, profile_table(daily), now=now)

    assert _offsets(plan, current_index("day", now)) == [2, 3]


@pytest.mark.parametrize(
    ("floor", "expected"),
    [(None, [0, 1, 2, 3]), (0, [1, 2, 3]), (1, [2, 3]), (2, [3])],
    ids=["disabled", "open-window-only", "default", "two-closed"],
)
def test_each_floor_releases_the_windows_it_says(session, daily, floor, expected):
    import datetime as dt

    from zamboni.windows import current_index

    now = dt.datetime.now(dt.UTC)
    plan = CompactionPlanner(CompactionConfig(skip_partitions_newer_than_windows=floor)).plan(
        daily, profile_table(daily), now=now
    )
    assert _offsets(plan, current_index("day", now)) == expected


def test_the_held_partition_says_why(session, daily):
    plan = CompactionPlanner(CompactionConfig()).plan(daily, profile_table(daily))
    held = [r for _, r in plan.skipped if "floor" in r]
    assert len(held) == 2, "today and yesterday"
    assert any("still open" in r for r in held)
    assert any("closed 0 day(s) ago" in r for r in held)


def test_a_table_with_no_time_partition_is_out_of_scope_not_blocked(session, partitioned):
    """`partitioned` is identity-partitioned on category.

    The control is about time windows, and this table has none. Out of scope is
    not the same as refused: the floor is on by default, so refusing would
    silently stop compacting every non-temporal table in a warehouse, and not
    compacting is itself a harm.
    """
    plan = CompactionPlanner(CompactionConfig()).plan(partitioned, profile_table(partitioned))

    assert plan.groups, "an identity-partitioned table still compacts"
    assert not [r for _, r in plan.skipped if "floor" in r]


def test_evolution_and_compaction_share_one_definition_of_a_window():
    """Not a comment asking them to agree -- the same module, asserted.

    Two implementations of "when did this window close" would drift into meaning
    different things under the same configured number, and the drift would be
    invisible until someone compared a plan against an evolution.
    """
    from zamboni import evolution, planner, windows

    assert evolution._window_end is windows.window_end
    assert planner.windows_since_close is windows.windows_since_close
