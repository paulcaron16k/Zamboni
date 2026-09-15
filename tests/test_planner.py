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
def two_days(session):
    """One closed day partition and one that closed yesterday, 2 files each."""
    import datetime as dt

    import pyarrow as pa
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import IntegerType, NestedField, TimestampType

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "ts", TimestampType(), required=False),
    )
    arrow = pa.schema([pa.field("id", pa.int32()), pa.field("ts", pa.timestamp("us"))])
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day")
    )
    tbl = session.catalog.create_table(
        "db.two_days", schema=schema, partition_spec=spec, properties={"format-version": "2"}
    )
    today = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    for day_offset in (0, 30):
        for i in range(2):
            tbl.append(
                pa.table(
                    {"id": [i], "ts": [today - dt.timedelta(days=day_offset)]},
                    schema=arrow,
                )
            )
    return session.catalog.load_table("db.two_days")


def test_the_floor_is_off_by_default(session, two_days):
    """CONTRIBUTING rule 6: a changed default decides what a nightly run touches.

    An unset floor must compact exactly what it compacted before the setting
    existed, or every installed config quietly changes meaning on upgrade.
    """
    assert CompactionConfig().skip_partitions_newer_than_days is None

    plan = CompactionPlanner(CompactionConfig()).plan(two_days, profile_table(two_days))

    assert len(plan.groups) == 2, "both day partitions compact when no floor is set"
    assert not [r for _, r in plan.skipped if "floor" in r]


def test_the_floor_leaves_the_partition_still_being_written(session, two_days):
    plan = CompactionPlanner(CompactionConfig(skip_partitions_newer_than_days=7)).plan(
        two_days, profile_table(two_days)
    )

    assert len(plan.groups) == 1, "only the 30-day-old partition is eligible"
    held = [r for _, r in plan.skipped if "floor" in r]
    assert len(held) == 1
    # The reason names the date, so an operator can tell when it becomes eligible
    # rather than re-deriving the arithmetic.
    assert "window closes" in held[0] and "7-day floor" in held[0]


def test_the_floor_measures_from_the_end_of_the_window_like_evolution(session, two_days):
    """The same configured number of days must mean the same thing in both.

    Today's partition is *not* zero days old the moment tomorrow begins: rows
    timestamped 23:59 are still arriving, which is the whole reason
    `EvolutionRule.older_than_days` measures from the window end. A floor of 1
    must therefore still hold today's partition.
    """
    import datetime as dt

    today = dt.datetime.now(dt.UTC).date()
    plan = CompactionPlanner(CompactionConfig(skip_partitions_newer_than_days=1)).plan(
        two_days, profile_table(two_days), today=today
    )

    held = [r for _, r in plan.skipped if "floor" in r]
    assert held, "today's partition closes tomorrow, so a 1-day floor still holds it"


def test_evolution_and_compaction_share_one_definition_of_a_closed_window():
    """Not a comment asking them to agree -- the same function, asserted.

    Two implementations of "has this window ended" would drift into meaning
    different things under the same configured number of days, and the drift
    would be invisible until someone compared a plan against an evolution.
    """
    from zamboni import evolution, planner, windows

    assert evolution._window_end is windows.window_end
    assert planner.window_end is windows.window_end


def test_the_floor_refuses_a_partition_it_cannot_date(session, partitioned):
    """`partitioned` is identity-partitioned on category -- no window at all.

    Refusing is the conservative reading of "do not touch partitions still being
    written": where that cannot be established, it is not established. The
    reason lands in the plan, so the refusal is never silent.
    """
    plan = CompactionPlanner(CompactionConfig(skip_partitions_newer_than_days=7)).plan(
        partitioned, profile_table(partitioned)
    )

    assert plan.is_empty
    assert plan.skipped
    assert all("no temporal partition field" in reason for _, reason in plan.skipped)
