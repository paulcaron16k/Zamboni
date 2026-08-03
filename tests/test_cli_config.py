"""CLI verbs for the config workflow."""

from __future__ import annotations

import json

import pytest

from icemaint.catalog_import import EXTENSION_KEY
from icemaint.cli import main
from icemaint.profile import profile_table
from icemaint.tableconfig import TableConfig

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
