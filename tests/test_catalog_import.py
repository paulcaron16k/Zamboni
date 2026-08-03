"""Generating table-config.json from a Meltano / Singer catalog."""

from __future__ import annotations

import json

import pytest

from zamboni.catalog_import import EXTENSION_KEY, config_from_catalog, load_catalog
from zamboni.tableconfig import TableConfigError

ICEBERG_BLOCK = {
    "partition": [{"column": "occurred_at", "transform": "day"}],
    "ordering": {"mode": "zorder", "zorder": {"columns": ["customer_id", "product_id"]}},
    "target_file_size_bytes": 268435456,
}


def catalog(**overrides):
    stream = {
        "tap_stream_id": "public-events",
        "stream": "events",
        "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {"table-key-properties": ["id"], EXTENSION_KEY: ICEBERG_BLOCK},
            }
        ],
    }
    stream.update(overrides)
    return {"streams": [stream]}


def test_imports_from_stream_metadata():
    config, report = config_from_catalog(catalog(), namespace="analytics")

    assert [s.origin for s in report.imported] == ["stream metadata"]
    settings = config.for_table("analytics.events")
    assert settings.ordering.mode == "zorder"
    assert settings.target_file_size_bytes == 268435456


def test_imports_from_the_schema_root():
    """The SDK's own vendor extensions live as x-* schema keywords, so authors
    reach for the schema too."""
    entry = catalog()
    entry["streams"][0]["metadata"] = []
    entry["streams"][0]["schema"][EXTENSION_KEY] = ICEBERG_BLOCK

    config, report = config_from_catalog(entry, namespace="analytics")
    assert [s.origin for s in report.imported] == ["schema"]
    assert config.for_table("analytics.events").ordering.mode == "zorder"


def test_streams_without_the_block_are_reported_not_swallowed():
    """Silence here would hide a misspelled extension key."""
    entry = catalog()
    entry["streams"][0]["metadata"] = []
    _, report = config_from_catalog(entry, namespace="analytics")

    assert not report.imported
    assert report.skipped == [("public-events", f"no {EXTENSION_KEY!r} block")]
    assert "skipped public-events" in report.describe()


def test_generated_config_carries_the_evolution_default():
    config, _ = config_from_catalog(catalog(), namespace="analytics")
    settings = config.for_table("analytics.events")
    assert settings.partition_evolution.enabled is True
    assert settings.partition_evolution.rules[0].to_transform == "month"


def test_explicit_table_override_wins():
    block = dict(ICEBERG_BLOCK, table="warehouse.fact_events")
    entry = catalog()
    entry["streams"][0]["metadata"][0]["metadata"][EXTENSION_KEY] = block

    config, _ = config_from_catalog(entry, namespace="analytics")
    assert "warehouse.fact_events" in config.tables


def test_schema_name_supplies_the_namespace():
    entry = catalog()
    entry["streams"][0]["metadata"][0]["metadata"]["schema-name"] = "raw"
    config, _ = config_from_catalog(entry)
    assert "raw.events" in config.tables


def test_unresolvable_identifier_is_skipped_not_guessed():
    entry = catalog()
    entry["streams"][0]["stream"] = "events"
    entry["streams"][0]["tap_stream_id"] = "events"
    config, report = config_from_catalog(entry)

    assert not config.tables
    assert "could not resolve" in report.skipped[0][1]


def test_invalid_block_fails_loudly():
    entry = catalog()
    entry["streams"][0]["metadata"][0]["metadata"][EXTENSION_KEY] = {"nonsense": 1}
    with pytest.raises(TableConfigError, match="unknown key"):
        config_from_catalog(entry, namespace="analytics")


def test_generated_config_is_valid_and_round_trips(tmp_path):
    config, _ = config_from_catalog(catalog(), namespace="analytics")
    path = tmp_path / "table-config.json"
    config.dump(path)

    reloaded = json.loads(path.read_text())
    assert reloaded["version"] == 1
    assert "analytics.events" in reloaded["tables"]


def test_load_catalog_reports_bad_json(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_text("{not json")
    with pytest.raises(TableConfigError, match="invalid catalog JSON"):
        load_catalog(path)


def test_missing_streams_list_is_an_error():
    with pytest.raises(TableConfigError, match="no 'streams' list"):
        config_from_catalog({})
