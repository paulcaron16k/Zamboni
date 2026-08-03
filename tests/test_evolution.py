"""Days-to-months partition evolution.

The hazard this module guards is quiet: committing month-partitioned files
through PyIceberg's stock producer writes them into a manifest declared under
the table's *day* spec. Reads still work -- a partition tuple is only consulted
when pruning -- so the corruption surfaces later, as wrong results from a
predicate. Every test here therefore checks manifest/file spec agreement, not
just row counts.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import IntegerType, NestedField, TimestampType

from icemaint import CompactionConfig, TableCompactor
from icemaint.evolution import plan_evolution
from icemaint.profile import profile_table
from icemaint.tableconfig import EvolutionRule, PartitionEvolution, TableConfig, TableSettings

TS_SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "ts", TimestampType(), required=False),
)
TS_ARROW = pa.schema([pa.field("id", pa.int32()), pa.field("ts", pa.timestamp("us"))])

DAY_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day")
)


@pytest.fixture
def daily(session):
    """Day-partitioned: 8 old days (Jan 2026) and 2 recent days."""
    tbl = session.catalog.create_table(
        "db.daily", schema=TS_SCHEMA, partition_spec=DAY_SPEC, properties={"format-version": "2"}
    )
    today = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    for d in range(1, 9):
        tbl.append(pa.table({"id": [d], "ts": [dt.datetime(2026, 1, d)]}, schema=TS_ARROW))
    for offset in (1, 2):
        tbl.append(
            pa.table(
                {"id": [100 + offset], "ts": [today - dt.timedelta(days=offset)]},
                schema=TS_ARROW,
            )
        )
    return session.catalog.load_table("db.daily")


def settings(enabled: bool = True, older_than_days: int = 90) -> TableSettings:
    return TableSettings(
        partition_evolution=PartitionEvolution(
            enabled=enabled,
            rules=(
                EvolutionRule(
                    from_transform="day", to_transform="month", older_than_days=older_than_days
                ),
            ),
        )
    )


def manifest_spec_agreement(tbl) -> list[tuple[int, int]]:
    """(manifest spec id, file spec id) for every live data file.

    A mismatch is the corruption this feature can silently produce.
    """
    from pyiceberg.manifest import DataFileContent

    tbl.refresh()
    pairs = []
    for manifest in tbl.current_snapshot().manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            if entry.data_file.content == DataFileContent.DATA:
                pairs.append((manifest.partition_spec_id, entry.data_file.spec_id))
    return pairs


def test_plan_selects_only_aged_partitions(session, daily):
    profile = profile_table(daily)
    plan = plan_evolution(daily, settings(), profile.live_files)

    assert len(plan.groups) == 1
    group = plan.groups[0]
    assert group.label.endswith("2026-01")
    # The two recent days must be left alone.
    assert len(group.files) == 8


def test_disabled_evolution_plans_nothing(session, daily):
    plan = plan_evolution(daily, settings(enabled=False), profile_table(daily).live_files)
    assert plan.is_empty
    assert any("enabled is false" in reason for _, reason in plan.skipped)


def test_retention_window_protects_recent_data(session, daily):
    # A 20-year window means nothing has aged out yet.
    plan = plan_evolution(
        daily, settings(older_than_days=365 * 20), profile_table(daily).live_files
    )
    assert plan.is_empty


def test_evolution_condenses_days_into_a_month(session, daily):
    before_ids = sorted(daily.scan().to_arrow()["id"].to_pylist())
    config = TableConfig(defaults=settings(), tables={})

    result = TableCompactor.from_table_config(
        session, "db.daily", config, base=CompactionConfig()
    ).execute()

    assert result.evolved, "no evolution group ran"
    tbl = session.table("db.daily")
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == before_ids

    after = profile_table(tbl)
    # 8 daily files became 1 monthly file; the 2 recent days are untouched.
    assert len(after.live_files) == 3

    specs = {f.spec_id for f in after.live_files}
    assert len(specs) == 2, f"expected day and month specs to coexist, got {specs}"


def test_new_data_still_lands_in_the_daily_spec(session, daily):
    config = TableConfig(defaults=settings(), tables={})
    TableCompactor.from_table_config(session, "db.daily", config).execute()

    tbl = session.table("db.daily")
    default_spec = tbl.spec()
    assert len(default_spec.fields) == 1
    assert str(default_spec.fields[0].transform) == "day", (
        "evolution must add the month spec without making it the default, or new "
        "writes would lose day-level pruning"
    )


def test_manifest_spec_matches_file_spec(session, daily):
    """The corruption check: a month file must live in a month-spec manifest."""
    config = TableConfig(defaults=settings(), tables={})
    TableCompactor.from_table_config(session, "db.daily", config).execute()

    pairs = manifest_spec_agreement(session.table("db.daily"))
    mismatched = [(m, f) for m, f in pairs if m != f]
    assert not mismatched, (
        f"manifest spec id disagrees with file spec id for {len(mismatched)} file(s): "
        f"{mismatched}. Month-partitioned files were written into a day-spec manifest."
    )


def test_partition_values_survive_a_round_trip(session, daily):
    """Predicates must still prune correctly after evolution."""
    config = TableConfig(defaults=settings(), tables={})
    TableCompactor.from_table_config(session, "db.daily", config).execute()

    tbl = session.table("db.daily")
    jan = tbl.scan(
        row_filter="ts >= '2026-01-01T00:00:00' and ts < '2026-02-01T00:00:00'"
    ).to_arrow()
    assert sorted(jan["id"].to_pylist()) == list(range(1, 9))


def test_new_partition_field_gets_a_fresh_id(session, daily):
    """Partition field ids must be unique across specs.

    A manifest's partition struct uses partition field ids as its struct field
    ids, so reusing the day field's id for the month field leaves one id
    meaning two different things -- the exact v1 defect the spec introduced
    `last-partition-id` to prevent ("partition fields with the same ID may
    contain different data types").
    """
    config = TableConfig(defaults=settings(), tables={})
    TableCompactor.from_table_config(session, "db.daily", config).execute()

    tbl = session.table("db.daily")
    seen: dict[int, str] = {}
    for spec_id, spec in tbl.metadata.specs().items():
        for pf in spec.fields:
            clash = seen.get(pf.field_id)
            assert clash is None, (
                f"partition field id {pf.field_id} is used by both {clash!r} and "
                f"{pf.name!r} (spec {spec_id})"
            )
            seen[pf.field_id] = pf.name

    assert len(seen) == 2, f"expected a day field and a distinct month field, got {seen}"
    assert tbl.metadata.last_partition_id == max(seen)


def test_snapshot_summary_labels_use_each_file_s_own_spec(session, capsys):
    """Added files must be summarised under the spec they were written with.

    PyIceberg's `_summary` passes the table default for every added file while
    using each file's own spec for removed ones. Left alone, a month file's
    partition value is rendered through the day spec -- a garbage date under
    the wrong field name, written permanently into snapshot metadata.
    """
    tbl = session.catalog.create_table(
        "db.summarised",
        schema=TS_SCHEMA,
        partition_spec=DAY_SPEC,
        # Partition summaries are off by default; this is what exposes it.
        properties={"format-version": "2", "write.summary.partition-limit": "10"},
    )
    for d in range(1, 6):
        tbl.append(pa.table({"id": [d], "ts": [dt.datetime(2026, 1, d)]}, schema=TS_ARROW))

    config = TableConfig(defaults=settings(), tables={})
    TableCompactor.from_table_config(session, "db.summarised", config).execute()

    summary = session.table("db.summarised").current_snapshot().summary
    labels = [k for k in summary.additional_properties if k.startswith("partitions.")]

    added = [k for k in labels if "ts_month" in k]
    assert added == ["partitions.ts_month=2026-01"], (
        f"the added month file should be labelled under the month spec, got {labels}"
    )
    # Removed day files legitimately keep the day spec; what must not appear is
    # a month value rendered as a day.
    assert not any(k.startswith("partitions.ts_day=19") for k in labels), (
        f"a month partition value was rendered through the day spec: {labels}"
    )
