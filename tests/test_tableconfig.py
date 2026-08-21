"""The table-config.json specification: parsing, validation, defaults."""

from __future__ import annotations

import json

import pytest

from zamboni.tableconfig import (
    DEFAULT_PARTITION_EVOLUTION,
    SPEC_VERSION,
    EvolutionRule,
    Ordering,
    PartitionEvolution,
    TableConfig,
    TableConfigError,
    TableSettings,
    ZOrder,
)


def v2(tables: dict, **root) -> dict:
    """A version-2 document from ``{"ns.table": settings}``.

    Tests care about settings, not about the nesting, so they keep writing flat
    identifiers and this puts them where the format wants them. The split here
    is safe because these are fixtures with one-level namespaces -- production
    code never splits, it reads the namespace the file states.
    """
    grouped: dict[str, dict] = {}
    for identifier, settings in tables.items():
        namespace, _, name = identifier.rpartition(".")
        grouped.setdefault(namespace, {})[name] = settings
    doc = {
        "version": SPEC_VERSION,
        "warehouse": "acme",
        "namespaces": {ns: {"tables": t} for ns, t in grouped.items()},
    }
    doc.update(root)
    return doc


MINIMAL = v2(
    {
        "analytics.events": {
            "partition": [{"column": "occurred_at", "transform": "day"}],
            "ordering": {"mode": "zorder", "zorder": {"columns": ["customer_id", "product_id"]}},
        }
    }
)


def test_parses_a_minimal_document():
    config = TableConfig.from_dict(MINIMAL)
    config.validate()

    settings = config.for_table("analytics.events")
    assert settings.partition[0].column == "occurred_at"
    assert settings.ordering.mode == "zorder"
    assert settings.ordering.zorder.columns == ("customer_id", "product_id")


def test_days_to_months_evolution_is_on_by_default():
    """The default asked for by the pipeline team: enabled, day -> month."""
    settings = TableConfig.from_dict(MINIMAL).for_table("analytics.events")
    assert settings.partition_evolution.enabled is True
    rule = settings.partition_evolution.rules[0]
    assert (rule.from_transform, rule.to_transform) == ("day", "month")


def test_a_table_can_opt_out_of_evolution():
    raw = json.loads(json.dumps(MINIMAL))
    raw["namespaces"]["analytics"]["tables"]["events"]["partition_evolution"] = {"enabled": False}
    settings = TableConfig.from_dict(raw).for_table("analytics.events")
    assert settings.partition_evolution.enabled is False


def test_evolution_can_be_disabled_fleet_wide():
    raw = json.loads(json.dumps(MINIMAL))
    raw["defaults"] = {"partition_evolution": {"enabled": False, "rules": []}}
    settings = TableConfig.from_dict(raw).for_table("analytics.events")
    assert settings.partition_evolution.enabled is False


def test_unknown_table_falls_back_to_defaults():
    config = TableConfig.from_dict(MINIMAL)
    assert config.for_table("analytics.nothing").partition_evolution == DEFAULT_PARTITION_EVOLUTION


def test_typos_are_rejected_not_ignored():
    """A silently ignored key would produce the wrong layout with no signal."""
    raw = v2({"a.b": {"partiton": []}})
    with pytest.raises(TableConfigError, match="unknown key"):
        TableConfig.from_dict(raw)


def test_round_trips_through_json(tmp_path):
    config = TableConfig.from_dict(MINIMAL)
    path = tmp_path / "table-config.json"
    config.dump(path)
    reloaded = TableConfig.load(path)
    assert reloaded.to_dict() == config.to_dict()


@pytest.mark.parametrize(
    "block,message",
    [
        ({"partition": [{"column": "x", "transform": "nope"}]}, "unknown partition transform"),
        ({"partition": [{"column": "x", "transform": "bucket"}]}, "requires 'num_buckets'"),
        ({"partition": [{"column": "x", "transform": "truncate"}]}, "requires 'width'"),
        ({"ordering": {"mode": "sort"}}, "requires a non-empty 'sort' list"),
        ({"ordering": {"mode": "zorder"}}, "requires a 'zorder' block"),
        (
            {"ordering": {"mode": "zorder", "zorder": {"columns": ["only_one"]}}},
            "at least 2 columns",
        ),
        (
            {
                "ordering": {
                    "mode": "sort",
                    "sort": [{"column": "a"}],
                    "zorder": {"columns": ["a", "b"]},
                }
            },
            "'zorder' is also set",
        ),
        ({"ordering": {"sort": [{"column": "a"}]}}, "mode is 'none' but an ordering is defined"),
        ({"min_input_files": 0}, "must be >= 1"),
    ],
)
def test_rejects_contradictory_blocks(block, message):
    raw = v2({"a.b": block})
    with pytest.raises(TableConfigError, match=message):
        TableConfig.from_dict(raw).validate()


def test_evolution_must_go_coarser():
    raw = v2(
        {
            "a.b": {
                "partition": [{"column": "ts", "transform": "month"}],
                "partition_evolution": {
                    "rules": [{"from": "month", "to": "day", "older_than_days": 1}]
                },
            }
        }
    )
    with pytest.raises(TableConfigError, match="must move to a coarser granularity"):
        TableConfig.from_dict(raw).validate()


def test_evolution_rule_must_match_a_declared_partition_field():
    """Silently doing nothing is the worst response to a mismatched rule."""
    raw = v2(
        {
            "a.b": {
                "partition": [{"column": "region", "transform": "identity"}],
                "partition_evolution": {
                    "rules": [{"from": "day", "to": "month", "older_than_days": 90}]
                },
            }
        }
    )
    with pytest.raises(TableConfigError, match="no partition field uses transform 'day'"):
        TableConfig.from_dict(raw).validate()


def test_zorder_bit_budget_is_enforced():
    with pytest.raises(TableConfigError, match="exceeds the 62-bit"):
        TableSettings(
            ordering=Ordering(
                mode="zorder", zorder=ZOrder(columns=("a", "b", "c", "d"), precision_bits=32)
            )
        ).validate("t")


def test_a_table_name_cannot_contain_a_dot():
    """The ambiguity version 2 exists to remove.

    In version 1 a key was `namespace.table` and the split was guessed with
    `rpartition` -- so `raw.telemetry.events` was read as namespace
    ('raw','telemetry') by the local engine while Trino quoted a single schema
    named `raw.telemetry`. Putting a dot in the table name would reintroduce
    exactly that, so it is refused where an author can see it.
    """
    raw = {
        "version": SPEC_VERSION,
        "warehouse": "acme",
        "namespaces": {"analytics": {"tables": {"raw.events": {}}}},
    }
    with pytest.raises(TableConfigError, match="cannot contain a dot"):
        TableConfig.from_dict(raw).validate()


def test_a_nested_namespace_is_allowed_and_kept_whole():
    """Discouraged, not forbidden: the dots live in a field that *means*
    namespace, so splitting them is safe rather than a guess."""
    raw = {
        "version": SPEC_VERSION,
        "warehouse": "acme",
        "namespaces": {"raw.telemetry": {"tables": {"events": {}}}},
    }
    config = TableConfig.from_dict(raw)
    config.validate()

    assert "raw.telemetry.events" in config.tables
    assert config.namespace_of("raw.telemetry.events") == ("raw", "telemetry")


def test_the_version_1_shape_is_named_rather_than_puzzled_over():
    """`unknown key(s) ['tables']` is what the generic check would say, and it
    tells nobody what to do about it."""
    with pytest.raises(TableConfigError, match="version 1 shape"):
        TableConfig.from_dict({"version": 1, "tables": {"db.events": {}}}).validate()


def test_the_warehouse_is_required():
    """It asserts rather than selects, and an optional assertion is absent in
    precisely the files nobody thought carefully about."""
    raw = {"version": SPEC_VERSION, "namespaces": {"db": {"tables": {"events": {}}}}}
    with pytest.raises(TableConfigError, match="'warehouse' is required"):
        TableConfig.from_dict(raw).validate()


def test_rejects_a_future_spec_version():
    with pytest.raises(TableConfigError, match="unsupported table-config version"):
        TableConfig(version=99).validate()


def test_duplicate_partition_field_names_rejected():
    raw = v2(
        {
            "a.b": {
                "partition": [
                    {"column": "x", "transform": "identity", "name": "same"},
                    {"column": "y", "transform": "identity", "name": "same"},
                ]
            }
        }
    )
    with pytest.raises(TableConfigError, match="duplicate partition field name"):
        TableConfig.from_dict(raw).validate()


def test_one_evolution_rule_per_source_granularity():
    settings = TableSettings(
        partition_evolution=PartitionEvolution(
            rules=(
                EvolutionRule("day", "month", 30),
                EvolutionRule("day", "year", 90),
            )
        )
    )
    with pytest.raises(TableConfigError, match="only age one way"):
        settings.validate("t")


def test_shipped_example_is_valid():
    """The example in examples/ is what people copy; keep it loadable."""
    from pathlib import Path

    example = Path(__file__).resolve().parent.parent / "examples" / "table-config.json"
    config = TableConfig.load(example)

    assert set(config.tables) == {
        "analytics.events",
        "analytics.audit_log",
        "analytics.dim_customer",
    }
    assert config.for_table("analytics.events").ordering.mode == "zorder"
    assert config.for_table("analytics.audit_log").partition_evolution.enabled is False


# -- retention -----------------------------------------------------------


def test_retention_defaults_are_iceberg_s_own():
    settings = TableConfig.from_dict(v2({"a.b": {}})).for_table("a.b")
    assert settings.retention.expire_snapshots.enabled is True
    assert settings.retention.remove_orphan_files.enabled is True
    # 3 days is Iceberg's documented default, sized to the longest write.
    assert settings.retention.remove_orphan_files.older_than_days == 3
    # Unset means "defer to the table property, then the spec default".
    assert settings.retention.expire_snapshots.max_snapshot_age_days is None


def test_retention_parses_and_round_trips(tmp_path):
    raw = v2(
        {
            "a.b": {
                "retention": {
                    "expire_snapshots": {
                        "enabled": True,
                        "max_snapshot_age_days": 14,
                        "min_snapshots_to_keep": 5,
                    },
                    "remove_orphan_files": {"enabled": True, "older_than_days": 7},
                }
            }
        }
    )
    config = TableConfig.from_dict(raw)
    config.validate()
    retention = config.for_table("a.b").retention
    assert retention.expire_snapshots.max_snapshot_age_days == 14
    assert retention.expire_snapshots.min_snapshots_to_keep == 5
    assert retention.remove_orphan_files.older_than_days == 7

    path = tmp_path / "tc.json"
    config.dump(path)
    assert TableConfig.load(path).for_table("a.b").retention == retention


def test_a_table_can_disable_reclaim():
    raw = v2(
        {
            "a.b": {
                "retention": {
                    "expire_snapshots": {"enabled": False},
                    "remove_orphan_files": {"enabled": False},
                }
            }
        }
    )
    retention = TableConfig.from_dict(raw).for_table("a.b").retention
    assert not retention.expire_snapshots.enabled
    assert not retention.remove_orphan_files.enabled


def test_orphan_removal_without_expiry_is_rejected():
    """Snapshots hold references, so orphan removal alone reclaims almost nothing."""
    raw = v2(
        {
            "a.b": {
                "retention": {
                    "expire_snapshots": {"enabled": False},
                    "remove_orphan_files": {"enabled": True},
                }
            }
        }
    )
    with pytest.raises(TableConfigError, match="almost nothing would be reclaimable"):
        TableConfig.from_dict(raw).validate()


@pytest.mark.parametrize(
    "block,message",
    [
        ({"expire_snapshots": {"min_snapshots_to_keep": 0}}, "must be >= 1"),
        ({"expire_snapshots": {"max_snapshot_age_days": -1}}, "must be >= 0"),
        ({"remove_orphan_files": {"older_than_days": -1}}, "must be >= 0"),
        ({"expire_snapshots": {"keep_forever": True}}, "unknown key"),
        ({"nonsense": {}}, "unknown key"),
    ],
)
def test_retention_rejects_bad_input(block, message):
    with pytest.raises(TableConfigError, match=message):
        TableConfig.from_dict(v2({"a.b": {"retention": block}})).validate()


def test_a_table_overrides_the_default_retention():
    raw = v2(
        {
            "a.b": {"retention": {"remove_orphan_files": {"older_than_days": 1}}},
            "a.c": {},
        },
        defaults={"retention": {"remove_orphan_files": {"older_than_days": 30}}},
    )
    config = TableConfig.from_dict(raw)
    assert config.for_table("a.b").retention.remove_orphan_files.older_than_days == 1
    assert config.for_table("a.c").retention.remove_orphan_files.older_than_days == 30
