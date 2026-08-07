"""CLI verbs for the config workflow."""

from __future__ import annotations

import json

import pytest

from zamboni.catalog_import import EXTENSION_KEY
from zamboni.cli import main
from zamboni.profile import profile_table
from zamboni.tableconfig import TableConfig

from .conftest import SCHEMA, batch


@pytest.fixture
def catalog_file(tmp_path):
    catalog = {
        "streams": [
            {
                "tap_stream_id": "public-events",
                "stream": "events",
                "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
                "metadata": [
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "table-key-properties": ["id"],
                            EXTENSION_KEY: {
                                "partition": [{"column": "occurred_at", "transform": "day"}],
                                "ordering": {
                                    "mode": "zorder",
                                    "zorder": {"columns": ["category", "payload"]},
                                },
                            },
                        },
                    }
                ],
            }
        ]
    }
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog))
    return path


def test_from_catalog_writes_a_valid_config(catalog_file, tmp_path, capsys):
    out = tmp_path / "table-config.json"
    argv = ["from-catalog", str(catalog_file), "-o", str(out), "--namespace", "analytics"]
    assert main(argv) == 0

    printed = capsys.readouterr().out
    assert "public-events -> analytics.events" in printed
    assert f"wrote {out}" in printed

    config = TableConfig.load(out)
    settings = config.for_table("analytics.events")
    assert settings.ordering.mode == "zorder"
    assert settings.partition_evolution.enabled is True


def test_no_evolution_flag_disables_the_default(catalog_file, tmp_path):
    out = tmp_path / "table-config.json"
    main(
        [
            "from-catalog",
            str(catalog_file),
            "-o",
            str(out),
            "--namespace",
            "analytics",
            "--no-evolution",
        ]
    )
    config = TableConfig.load(out)
    assert config.defaults.partition_evolution.enabled is False


def test_validate_config_summarises(catalog_file, tmp_path, capsys):
    out = tmp_path / "table-config.json"
    main(["from-catalog", str(catalog_file), "-o", str(out), "--namespace", "analytics"])
    capsys.readouterr()

    assert main(["validate-config", str(out)]) == 0
    printed = capsys.readouterr().out
    assert "valid (version 1, 1 table(s))" in printed
    assert "analytics.events: [occurred_at:day] ordering=zorder" in printed
    assert "evolution=day->month@90d" in printed


def test_validate_config_rejects_a_bad_file(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"tables": {"a.b": {"partiton": []}}}))
    with pytest.raises(Exception, match="unknown key"):
        main(["validate-config", str(bad)])


def test_compact_reads_layout_from_the_config(session, tmp_path, capsys):
    """--table-config drives ordering; the run still commits normally."""
    tbl = session.catalog.create_table(
        "db.events", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(4):
        tbl.append(batch(i * 10, 10))

    config_path = tmp_path / "table-config.json"
    config_path.write_text(
        json.dumps(
            {
                "version": 1,
                "tables": {
                    "db.events": {
                        "ordering": {
                            "mode": "zorder",
                            "zorder": {"columns": ["id", "category"], "precision_bits": 4},
                        }
                    }
                },
            }
        )
    )

    code = main(
        [
            "compact",
            "db.events",
            "--local-warehouse",
            str(tmp_path / "warehouse"),
            "--table-config",
            str(config_path),
            "--yes",
        ]
    )
    assert code == 0
    assert "rewrote 4 file(s)" in capsys.readouterr().out

    after = profile_table(session.table("db.events"))
    assert len(after.live_files) == 1
    assert after.total_records == 40


# -- zamboni table-config (ZMBNI-916) -------------------------------------


def _local_catalog(tmp_path):
    from pyiceberg.partitioning import PartitionField as SpecField
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import IntegerType, NestedField, TimestampType

    from zamboni import CatalogSession

    session = CatalogSession.for_local(warehouse_path=str(tmp_path))
    session.catalog.create_namespace("acme")
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "ts", TimestampType(), required=False),
    )
    spec = PartitionSpec(
        SpecField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day")
    )
    session.catalog.create_table(
        "acme.events", schema=schema, partition_spec=spec, properties={"format-version": "2"}
    )
    session.catalog.create_table("acme.plain", schema=schema, properties={"format-version": "2"})
    return session


def test_generate_describes_the_catalog_as_it_is(tmp_path, capsys):
    """Descriptive, not aspirational. The generated file records each table's
    *current* partition spec, so the first maintenance run against it changes
    nothing but file sizes -- an operator diffs intent against reality instead
    of discovering the difference during a run that deletes things."""
    from zamboni.cli import main
    from zamboni.tableconfig import TableConfig

    _local_catalog(tmp_path)
    out = tmp_path / "tc.json"

    assert (
        main(["table-config", "generate", "-o", str(out), "--local-warehouse", str(tmp_path)]) == 0
    )

    config = TableConfig.load(out)
    assert set(config.tables) == {"acme.events", "acme.plain"}
    assert [(f.column, f.transform) for f in config.tables["acme.events"].partition] == [
        ("ts", "day")
    ]
    assert config.tables["acme.plain"].partition == ()


def test_generate_refuses_to_overwrite_without_force(tmp_path, capsys):
    """The file being overwritten is the one that says what may be deleted."""
    from zamboni.cli import main

    _local_catalog(tmp_path)
    out = tmp_path / "tc.json"
    out.write_text("{}")

    assert (
        main(["table-config", "generate", "-o", str(out), "--local-warehouse", str(tmp_path)]) == 2
    )
    assert out.read_text() == "{}"
    assert "--force" in capsys.readouterr().err

    assert (
        main(
            [
                "table-config",
                "generate",
                "-o",
                str(out),
                "--force",
                "--local-warehouse",
                str(tmp_path),
            ]
        )
        == 0
    )
    assert out.read_text() != "{}"


def test_summary_names_the_fallback_instead_of_printing_none(tmp_path, capsys):
    """An unset knob is not "no value" -- expiry falls back to the Iceberg table
    property and only then to the spec default. Printing `None` told an operator
    nothing and read like a bug."""
    from zamboni.cli import main

    config = tmp_path / "tc.json"
    config.write_text('{"version": 1, "tables": {"db.events": {}}}')

    assert main(["table-config", "summary", str(config)]) == 0

    out = capsys.readouterr().out
    assert "history.expire.max-snapshot-age-ms, else 5" in out
    assert "history.expire.min-snapshots-to-keep, else 1" in out
    assert "None" not in out


def test_summary_marks_which_values_came_from_the_file(tmp_path, capsys):
    from zamboni.cli import main

    config = tmp_path / "tc.json"
    config.write_text(
        '{"version": 1, "tables": {"db.events": {"retention": '
        '{"remove_orphan_files": {"enabled": true, "older_than_days": 30}}}}}'
    )

    assert main(["table-config", "summary", str(config)]) == 0

    out = capsys.readouterr().out
    assert "older than 30 day(s)" in out
    assert "30 (default)" not in out, "a value written in the file is not a default"


def test_summary_warns_when_apply_properties_would_have_nothing_to_do(tmp_path, capsys):
    """`apply-properties` raises when neither metadata knob is set. Better to
    learn that from a summary than from a maintenance run's exit code."""
    from zamboni.cli import main

    config = tmp_path / "tc.json"
    config.write_text('{"version": 1, "tables": {"db.events": {}}}')
    main(["table-config", "summary", str(config)])

    assert "apply-properties has nothing to set" in capsys.readouterr().out


def test_summary_flags_zorder_as_unavailable_on_trino(tmp_path, capsys):
    """The one layout setting that silently does nothing on an engine that
    cannot do it, so the summary says so where the choice is made."""
    from zamboni.cli import main

    config = tmp_path / "tc.json"
    config.write_text(
        '{"version": 1, "tables": {"db.events": {"ordering": {"mode": "zorder", '
        '"zorder": {"columns": ["a", "b"]}}}}}'
    )
    main(["table-config", "summary", str(config)])

    out = capsys.readouterr().out
    assert "trino cannot do this" in out


def test_table_config_validate_matches_the_original_verb(tmp_path, capsys):
    """`validate-config` shipped in 0.1.0 and removing a verb is breaking under
    docs/releasing.md, so the new grouped name is an alias, not a second
    implementation. This pins them to the same output."""
    from zamboni.cli import main

    config = tmp_path / "tc.json"
    config.write_text('{"version": 1, "tables": {"db.events": {}}}')

    assert main(["validate-config", str(config)]) == 0
    old = capsys.readouterr().out
    assert main(["table-config", "validate", str(config)]) == 0
    assert capsys.readouterr().out == old
