"""The table-config.json specification: parsing, validation, defaults."""

from __future__ import annotations

import json

import pytest

from zamboni.config import config_from_table_settings
from zamboni.tableconfig import (
    DEFAULT_PARTITION_EVOLUTION,
    SPEC_VERSION,
    EvolutionRule,
    NamespaceSettings,
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


# -- a value of the wrong type is refused, not crashed on (ZMBNI-53) ------


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        # The silent one, and the reason this is a safety fix rather than a
        # message improvement. `get("enabled", True)` returns None for an
        # explicit null -- the default is only used when the key is *absent* --
        # and None is falsy, so this loaded and turned reclamation off. A run
        # then reported "disabled in the config" for a config that never said so.
        ({"retention": {"remove_orphan_files": {"enabled": None}}}, "null is not a value"),
        ({"retention": {"rewrite_manifests": {"enabled": None}}}, "null is not a value"),
        ({"retention": {"remove_dangling_deletes": {"enabled": None}}}, "null is not a value"),
        # Reached arithmetic in `validate()` and raised TypeError, which no
        # caller catches: the CLI maps TableConfigError to exit 2 and lets
        # anything else escape as a traceback.
        ({"min_input_files": None}, "null is not a value"),
        ({"retention": {"remove_orphan_files": {"older_than_days": None}}}, "null is not a value"),
        ({"retention": {"rewrite_manifests": {"min_input_manifests": None}}}, "null is not a"),
        ({"min_input_files": "day"}, "expected a number, found a string"),
        # `bool` is a subclass of `int`, so an unguarded isinstance would read
        # this as the integer 1 -- a config that means nothing accepted as one
        # that means something.
        ({"min_input_files": True}, "expected a number, found a boolean"),
        # Slipped past `"column" not in key` and produced a sort key on None.
        ({"ordering": {"mode": "sort", "sort": [{"column": None}]}}, "needs a string"),
        # Thirteen of fourteen nested blocks raised TypeError from `set(raw)`.
        ({"ordering": None}, "expected a block of settings, found null"),
        ({"retention": None}, "expected a block of settings, found null"),
        ({"partition": [None]}, "expected a block of settings, found null"),
        ({"ordering": "day"}, "expected a block of settings, found a string"),
    ],
)
def test_a_wrong_typed_value_is_refused_with_its_path(settings, message):
    with pytest.raises(TableConfigError, match=message) as caught:
        TableConfig.from_dict(v2({"a.b": settings})).validate()
    assert "namespaces.a.tables.b" in str(caught.value), (
        "the refusal must locate itself: a fleet config has hundreds of tables, "
        "and a message that does not say which one is barely better than a crash"
    )


@pytest.mark.parametrize(
    "settings",
    [
        # Documented as "leave whatever is there", so null is a legal spelling of
        # unset and must stay one -- narrowing these would break every config
        # using it, and the shipped schema declares them nullable.
        {"target_file_size_bytes": None},
        {"description": None},
        {"retention": {"expire_snapshots": {"max_snapshot_age_days": None}}},
        {"retention": {"expire_snapshots": {"min_snapshots_to_keep": None}}},
        {"retention": {"expire_snapshots": {"max_ref_age_days": None}}},
        {"retention": {"metadata": {"previous_versions_max": None}}},
        {"retention": {"metadata": {"delete_after_commit": None}}},
        # Containers the loader coalesces to empty. Kept, for the same reason.
        {"partition": None},
        {"ordering": {"sort": None}},
        {"partition_evolution": {"rules": None}},
    ],
)
def test_null_still_means_unset_where_it_always_did(settings):
    TableConfig.from_dict(v2({"a.b": settings})).validate()


# -- what a table inherits, and what it overrides (ZMBNI-216) -------------------


def effective(defaults: dict, table: dict, *, key: str = "skip_partitions_newer_than_windows"):
    """What one table resolves to, read where the run reads it."""
    config = TableConfig.from_dict(v2({"a.b": table}, defaults=defaults))
    config.validate()
    return getattr(config_from_table_settings(config.for_table("a.b")), key)


@pytest.mark.parametrize(
    ("defaults", "table", "expected"),
    [
        ({}, {}, 1),
        ({}, {"skip_partitions_newer_than_windows": None}, None),
        ({"skip_partitions_newer_than_windows": None}, {}, None),
        ({"skip_partitions_newer_than_windows": 3}, {}, 3),
        (
            {"skip_partitions_newer_than_windows": 3},
            {"skip_partitions_newer_than_windows": None},
            None,
        ),
        (
            {"skip_partitions_newer_than_windows": None},
            {"skip_partitions_newer_than_windows": 0},
            0,
        ),
    ],
    ids=[
        "unset-everywhere-holds-one-window",
        "table-null-turns-it-off",
        "defaults-null-turns-it-off-fleet-wide",
        "table-unset-inherits-defaults",
        "table-null-beats-a-default-number",
        "table-number-beats-a-default-null",
    ],
)
def test_the_hold_is_inherited_by_presence_not_by_value(defaults, table, expected):
    """Unset inherits; a number or null overrides. A config with no key holds one window.

    Before ZMBNI-216 the first case was None -- no hold at all, against the
    documented 1 -- and the fifth inherited 3, so the documented way to turn the
    hold off could not.
    """
    assert effective(defaults, table) == expected


def test_a_default_min_input_files_reaches_a_table_with_a_block_of_its_own():
    """Was ignored by every table with a block -- in a config, every table."""
    assert effective({"min_input_files": 5}, {}, key="min_input_files") == 5
    assert effective({"min_input_files": 5}, {"min_input_files": 2}, key="min_input_files") == 2


def test_a_table_null_target_size_overrides_a_default_one():
    """null is "use the table's own property", and a table must be able to say so."""
    defaults = {"target_file_size_bytes": 1000}
    assert effective(defaults, {}, key="target_file_size_bytes") == 1000
    assert (
        effective(defaults, {"target_file_size_bytes": None}, key="target_file_size_bytes") is None
    )


def test_what_a_table_set_survives_a_dump_and_reload(tmp_path):
    """A key written out is an override when read back, so only the set ones are."""
    doc = v2(
        {
            "a.off": {"skip_partitions_newer_than_windows": None, "min_input_files": 2},
            "a.inherits": {},
        },
        defaults={"skip_partitions_newer_than_windows": 3, "min_input_files": 5},
    )
    path = tmp_path / "table-config.json"
    TableConfig.from_dict(doc).dump(path)
    reloaded = TableConfig.load(path)

    off, inherits = reloaded.for_table("a.off"), reloaded.for_table("a.inherits")
    assert (off.skip_partitions_newer_than_windows, off.min_input_files) == (None, 2)
    assert (inherits.skip_partitions_newer_than_windows, inherits.min_input_files) == (3, 5)


def resolved(defaults: dict, table: dict) -> TableSettings:
    config = TableConfig.from_dict(v2({"a.b": table}, defaults=defaults))
    config.validate()
    return config.for_table("a.b")


# -- the same rule at every depth (ZMBNI-220) ----------------------------------


def test_one_retention_key_leaves_the_rest_of_defaults_retention_in_place():
    """The reason inheritance exists: a table writes what differs and no more.
    Before ZMBNI-220 the table took `retention` whole, and lost the fleet's 30
    days to the built-in's unset."""
    s = resolved(
        {"retention": {"expire_snapshots": {"max_snapshot_age_days": 30}}},
        {"retention": {"remove_orphan_files": {"older_than_days": 7}}},
    )
    assert s.retention.expire_snapshots.max_snapshot_age_days == 30
    assert s.retention.remove_orphan_files.older_than_days == 7


def test_one_key_inside_a_retention_block_inherits_its_siblings():
    s = resolved(
        {"retention": {"remove_orphan_files": {"enabled": False, "older_than_days": 10}}},
        {"retention": {"remove_orphan_files": {"older_than_days": 7}}},
    )
    assert s.retention.remove_orphan_files.enabled is False
    assert s.retention.remove_orphan_files.older_than_days == 7


def test_an_empty_partition_list_makes_a_table_unpartitioned():
    s = resolved({"partition": [{"column": "ts", "transform": "day"}]}, {"partition": []})
    assert s.partition == ()


def test_ordering_none_turns_off_a_default_sort():
    s = resolved(
        {"ordering": {"mode": "sort", "sort": [{"column": "a"}]}},
        {"ordering": {"mode": "none"}},
    )
    assert s.ordering.mode == "none"


def test_a_value_equal_to_the_built_in_still_overrides_defaults():
    """Presence decides, not comparison with the built-in -- so a table can ask
    for the built-in back."""
    s = resolved(
        {"retention": {"remove_orphan_files": {"older_than_days": 10}}},
        {"retention": {"remove_orphan_files": {"older_than_days": 3}}},
    )
    assert s.retention.remove_orphan_files.older_than_days == 3


def test_ordering_is_replaced_whole_not_merged():
    """A mode and its keys are one choice. Merged key by key, z-order would
    inherit the default sort keys too, and validation would refuse it."""
    s = resolved(
        {"ordering": {"mode": "sort", "sort": [{"column": "a"}]}},
        {"ordering": {"mode": "zorder", "zorder": {"columns": ["a", "b"]}}},
    )
    assert s.ordering.mode == "zorder"
    assert s.ordering.sort == ()


def test_evolution_rules_are_a_list_and_replace_whole():
    s = resolved(
        {"partition": [{"column": "ts", "transform": "day"}]},
        {"partition_evolution": {"rules": [{"from": "day", "to": "year", "older_than_days": 400}]}},
    )
    assert [(r.from_transform, r.to_transform) for r in s.partition_evolution.rules] == [
        ("day", "year")
    ]
    assert s.partition_evolution.enabled is True, "inherited from the built-in"


def test_a_defaults_block_inherits_the_built_in_where_it_is_silent():
    """`defaults` is itself a block over the built-in. Before ZMBNI-220 a
    `defaults` that mentioned anything at all lost the built-in evolution rule,
    because the parser's empty default stood in for every key it left out."""
    with_defaults = resolved({"min_input_files": 4}, {})
    without = TableConfig.from_dict(v2({"a.b": {}})).for_table("a.b")
    assert with_defaults.partition_evolution == without.partition_evolution
    assert with_defaults.partition_evolution.rules, "the built-in day-to-month rule"


def test_validation_sees_the_merged_table_not_the_fragment():
    """An evolution rule in a table block names a transform the table inherits."""
    resolved(
        {"partition": [{"column": "ts", "transform": "day"}]},
        {"partition_evolution": {"rules": [{"from": "day", "to": "month", "older_than_days": 9}]}},
    )
    with pytest.raises(TableConfigError, match="no partition field uses transform 'hour'"):
        resolved(
            {"partition": [{"column": "ts", "transform": "day"}]},
            {
                "partition_evolution": {
                    "rules": [{"from": "hour", "to": "day", "older_than_days": 1}]
                }
            },
        )


def test_a_dump_writes_only_what_each_block_set(tmp_path):
    """Files stay small after a round trip, and nothing a block left out comes
    back as an override."""
    doc = v2(
        {"a.b": {"retention": {"remove_orphan_files": {"older_than_days": 7}}}},
        defaults={"retention": {"expire_snapshots": {"max_snapshot_age_days": 30}}},
    )
    path = tmp_path / "table-config.json"
    TableConfig.from_dict(doc).dump(path)
    written = json.loads(path.read_text())

    assert written["namespaces"]["a"]["tables"]["b"] == {
        "retention": {"remove_orphan_files": {"older_than_days": 7}}
    }
    assert written["defaults"] == doc["defaults"]
    again = TableConfig.load(path).for_table("a.b")
    assert again.retention.expire_snapshots.max_snapshot_age_days == 30


def test_settings_built_in_code_set_every_key():
    """A `TableSettings` object has no record of what was left out, so it counts
    as setting everything; `TableSettings.from_dict` is the sparse form."""
    config = TableConfig(
        warehouse="acme",
        defaults=TableSettings(min_input_files=5),
        namespaces={
            "a": NamespaceSettings(
                tables={
                    "whole": TableSettings(),
                    "sparse": TableSettings.from_dict({"min_input_files": 3}),
                    "empty": TableSettings.from_dict({}),
                }
            )
        },
    )
    assert config.for_table("a.whole").min_input_files == 2
    assert config.for_table("a.sparse").min_input_files == 3
    assert config.for_table("a.empty").min_input_files == 5


def test_description_belongs_to_one_table():
    s = resolved({"description": "fleet"}, {})
    assert s.description is None
