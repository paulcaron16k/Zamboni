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
