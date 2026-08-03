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
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType, TimestampType

from zamboni import CompactionConfig, TableCompactor
from zamboni.evolution import plan_evolution
from zamboni.profile import profile_table
from zamboni.tableconfig import EvolutionRule, PartitionEvolution, TableConfig, TableSettings

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


def test_evolution_honours_the_overwrite_escape_hatch(session):
    """`snapshot_operation="overwrite"` must reach an evolved table too.

    MultiSpecReplaceFiles hardcoded REPLACE in its summary, so any evolved table
    silently produced a replace snapshot however it was configured -- defeating
    the option that exists for anyone unwilling to depend on the relabelling
    subclass at all. Latent until an atomic commit routed every table through
    that producer.
    """
    import datetime as dt

    tbl = session.catalog.create_table(
        "db.evolved_overwrite",
        schema=TS_SCHEMA,
        partition_spec=DAY_SPEC,
        properties={"format-version": "2"},
    )
    for d in range(1, 5):
        tbl.append(pa.table({"id": [d], "ts": [dt.datetime(2026, 1, d)]}, schema=TS_ARROW))

    config = TableConfig(
        defaults=TableSettings(
            partition_evolution=PartitionEvolution(
                enabled=True, rules=(EvolutionRule("day", "month", 90),)
            )
        ),
        tables={},
    )
    result = TableCompactor.from_table_config(
        session,
        "db.evolved_overwrite",
        config,
        base=CompactionConfig(snapshot_operation="overwrite"),
    ).execute()
    assert result.evolved, "nothing evolved; the fixture no longer exercises this"

    snapshot = session.table("db.evolved_overwrite").current_snapshot()
    assert snapshot.summary.operation.value == "overwrite"


def test_an_atomic_commit_records_every_evolved_group(session):
    """One snapshot covering several evolved groups must name them all.

    Per-group commits each carried their own `zamboni.evolution` label; a single
    commit that kept only one would lose the record of what it did.
    """
    import datetime as dt

    tbl = session.catalog.create_table(
        "db.evolved_many",
        schema=TS_SCHEMA,
        partition_spec=DAY_SPEC,
        properties={"format-version": "2"},
    )
    # Two distinct months, so evolution produces two groups.
    for month, day in ((1, 3), (1, 4), (2, 5), (2, 6)):
        tbl.append(pa.table({"id": [day], "ts": [dt.datetime(2026, month, day)]}, schema=TS_ARROW))

    config = TableConfig(
        defaults=TableSettings(
            partition_evolution=PartitionEvolution(
                enabled=True, rules=(EvolutionRule("day", "month", 90),)
            )
        ),
        tables={},
    )
    result = TableCompactor.from_table_config(
        session, "db.evolved_many", config, base=CompactionConfig()
    ).execute()

    assert len(result.evolved) == 2, f"expected two month groups, got {result.evolved}"
    assert len({g.snapshot_id for g in result.evolved}) == 1, "not a single commit"

    label = session.table("db.evolved_many").current_snapshot().summary["zamboni.evolution"]
    assert "2026-01" in label and "2026-02" in label, label


# -- compound partition specs --------------------------------------------

COMPOUND_SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "ts", TimestampType(), required=False),
    NestedField(3, "region", StringType(), required=False),
)
COMPOUND_ARROW = pa.schema(
    [
        pa.field("id", pa.int32()),
        pa.field("ts", pa.timestamp("us")),
        pa.field("region", pa.string()),
    ]
)
COMPOUND_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day"),
    PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="region"),
)

EVOLVE_CONFIG = TableConfig(
    defaults=TableSettings(
        partition_evolution=PartitionEvolution(
            enabled=True, rules=(EvolutionRule("day", "month", 90),)
        )
    ),
    tables={},
)


@pytest.fixture
def compound(session):
    """[ts:day, region:identity] with two old days in each of two regions."""
    tbl = session.catalog.create_table(
        "db.compound",
        schema=COMPOUND_SCHEMA,
        partition_spec=COMPOUND_SPEC,
        properties={"format-version": "2"},
    )
    for day in (3, 4):
        for region in ("eu", "us"):
            tbl.append(
                pa.table(
                    {
                        "id": [day],
                        "ts": [dt.datetime(2026, 1, day)],
                        "region": [region],
                    },
                    schema=COMPOUND_ARROW,
                )
            )
    return session.catalog.load_table("db.compound")


def test_a_compound_spec_needs_one_new_spec_not_one_per_combination(compound):
    """The reasoning the old skip rested on was wrong.

    Condensing one field does not require a spec per combination of the others.
    The combinations are partition *values*, which each file carries under a
    single shared spec -- so two regions and one month need exactly one new spec.
    """
    plan = plan_evolution(
        compound, EVOLVE_CONFIG.for_table("db.compound"), profile_table(compound).live_files
    )

    assert len(plan.required_specs) == 1, plan.required_specs
    spec = next(iter(plan.required_specs.values()))
    assert [str(f.transform) for f in spec.fields] == ["month", "identity"], (
        "the aged field must coarsen and the other carry through unchanged"
    )
    assert [f.source_id for f in spec.fields] == [2, 3]


def test_the_carried_field_keeps_its_field_id_and_the_aged_one_gets_a_fresh_one(compound):
    """A changed field must not reuse an id; an unchanged one must not change it.

    A manifest's partition struct uses partition field ids as struct field ids,
    so a reused id would leave one id meaning two things -- and a gratuitously
    renumbered one would break pruning on data already written.
    """
    plan = plan_evolution(
        compound, EVOLVE_CONFIG.for_table("db.compound"), profile_table(compound).live_files
    )
    spec = next(iter(plan.required_specs.values()))

    aged, carried = spec.fields
    assert carried.field_id == 1001, "the untouched field was renumbered"
    assert aged.field_id not in (1000, 1001), f"the aged field reused id {aged.field_id}"


def test_files_are_grouped_by_the_whole_output_partition(compound):
    """Two regions in one month are two output partitions, not one group."""
    plan = plan_evolution(
        compound, EVOLVE_CONFIG.for_table("db.compound"), profile_table(compound).live_files
    )

    assert len(plan.groups) == 2, [g.label for g in plan.groups]
    for group in plan.groups:
        regions = {f.partition[1] for f in group.files}
        assert len(regions) == 1, f"{group.label} mixes regions {regions}"
    labels = sorted(g.label for g in plan.groups)
    assert "region=eu" in labels[0] and "region=us" in labels[1], labels


def test_a_compound_spec_evolves_end_to_end(session, compound):
    """Rows preserved, manifests agreeing with their files, region kept."""
    before = sorted(compound.scan().to_arrow()["id"].to_pylist())
    regions_before = sorted(compound.scan().to_arrow()["region"].to_pylist())

    result = TableCompactor.from_table_config(
        session, "db.compound", EVOLVE_CONFIG, base=CompactionConfig()
    ).execute()
    assert len(result.evolved) == 2, result.evolved

    tbl = session.table("db.compound")
    arrow = tbl.scan().to_arrow()
    assert sorted(arrow["id"].to_pylist()) == before
    assert sorted(arrow["region"].to_pylist()) == regions_before

    # Two output files, one per region, each under the month spec.
    live = profile_table(tbl).live_files
    assert len(live) == 2, [f.path for f in live]
    specs = tbl.metadata.specs()
    for f in live:
        assert [str(x.transform) for x in specs[f.spec_id].fields] == ["month", "identity"]

    # The check this module exists for: every manifest's spec matches its files.
    for manifest in tbl.current_snapshot().manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            assert manifest.partition_spec_id == entry.data_file.spec_id, (
                f"{manifest.manifest_path} declares spec {manifest.partition_spec_id} "
                f"but holds a file of spec {entry.data_file.spec_id}"
            )


def test_two_fields_of_the_same_granularity_are_still_refused(session):
    """The genuinely ambiguous case, which stays skipped.

    `older_than_days` is measured from a window end. Two day fields give two
    answers, so ageing by either would be a guess about which column dates the
    row.
    """
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "created_at", TimestampType(), required=False),
        NestedField(3, "updated_at", TimestampType(), required=False),
    )
    arrow = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("created_at", pa.timestamp("us")),
            pa.field("updated_at", pa.timestamp("us")),
        ]
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="created_day"),
        PartitionField(source_id=3, field_id=1001, transform=DayTransform(), name="updated_day"),
    )
    tbl = session.catalog.create_table(
        "db.two_days", schema=schema, partition_spec=spec, properties={"format-version": "2"}
    )
    for day in (3, 4):
        tbl.append(
            pa.table(
                {
                    "id": [day],
                    "created_at": [dt.datetime(2026, 1, day)],
                    "updated_at": [dt.datetime(2026, 1, day)],
                },
                schema=arrow,
            )
        )
    tbl = session.catalog.load_table("db.two_days")

    plan = plan_evolution(
        tbl, EVOLVE_CONFIG.for_table("db.two_days"), profile_table(tbl).live_files
    )

    assert plan.groups == []
    reasons = [r for _, r in plan.skipped]
    assert any("ambiguous" in r for r in reasons), reasons
    assert any("created_day, updated_day" in r for r in reasons), reasons
