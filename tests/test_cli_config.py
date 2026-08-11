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
    argv = [
        "from-catalog",
        "--db",
        "acme",
        str(catalog_file),
        "-o",
        str(out),
        "--namespace",
        "analytics",
    ]
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
            "--db",
            "acme",
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
    main(
        [
            "from-catalog",
            "--db",
            "acme",
            "--db",
            "acme",
            str(catalog_file),
            "-o",
            str(out),
            "--namespace",
            "analytics",
        ]
    )
    capsys.readouterr()

    assert main(["validate-config", str(out)]) == 0
    printed = capsys.readouterr().out
    assert "valid (version 2, warehouse 'acme', 1 namespace(s), 1 table(s))" in printed
    assert "analytics.events: [occurred_at:day] ordering=zorder" in printed
    assert "evolution=day->month@90d" in printed


def test_validate_config_rejects_a_bad_file(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "acme",
                "namespaces": {"a": {"tables": {"b": {"partiton": []}}}},
            }
        )
    )
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
                "version": 2,
                "warehouse": "acme",
                "namespaces": {
                    "db": {
                        "tables": {
                            "events": {
                                "ordering": {
                                    "mode": "zorder",
                                    "zorder": {
                                        "columns": ["id", "category"],
                                        "precision_bits": 4,
                                    },
                                }
                            }
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
        main(
            [
                "table-config",
                "generate",
                "--db",
                "acme",
                "-o",
                str(out),
                "--local-warehouse",
                str(tmp_path),
            ]
        )
        == 0
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
        main(
            [
                "table-config",
                "generate",
                "--db",
                "acme",
                "-o",
                str(out),
                "--local-warehouse",
                str(tmp_path),
            ]
        )
        == 2
    )
    assert out.read_text() == "{}"
    assert "--force" in capsys.readouterr().err

    assert (
        main(
            [
                "table-config",
                "generate",
                "--db",
                "acme",
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
    config.write_text(
        '{"version": 2, "warehouse": "acme", "namespaces": {"db": {"tables": {"events": {}}}}}'
    )

    assert main(["table-config", "summary", str(config)]) == 0

    out = capsys.readouterr().out
    assert "history.expire.max-snapshot-age-ms, else 5" in out
    assert "history.expire.min-snapshots-to-keep, else 1" in out
    assert "None" not in out


def test_summary_marks_which_values_came_from_the_file(tmp_path, capsys):
    from zamboni.cli import main

    config = tmp_path / "tc.json"
    config.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "acme",
                "namespaces": {
                    "db": {
                        "tables": {
                            "events": {
                                "retention": {
                                    "remove_orphan_files": {"enabled": True, "older_than_days": 30}
                                }
                            }
                        }
                    }
                },
            }
        )
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
    config.write_text(
        '{"version": 2, "warehouse": "acme", "namespaces": {"db": {"tables": {"events": {}}}}}'
    )
    main(["table-config", "summary", str(config)])

    assert "apply-properties has nothing to set" in capsys.readouterr().out


def test_summary_flags_zorder_as_unavailable_where_it_is(tmp_path, capsys):
    """The one layout setting that silently does nothing on an engine that
    cannot do it, so the summary says so where the choice is made.

    Derived from the capability declarations rather than asserted against a
    literal: the first version compared against the string "trino cannot do
    this", which would have kept passing after Trino gained Z-order.
    """
    from zamboni.cli import main
    from zamboni.maintainers import LayoutFeature, engines_lacking

    config = tmp_path / "tc.json"
    config.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "acme",
                "namespaces": {
                    "db": {
                        "tables": {
                            "events": {
                                "ordering": {"mode": "zorder", "zorder": {"columns": ["a", "b"]}}
                            }
                        }
                    }
                },
            }
        )
    )
    main(["table-config", "summary", str(config)])

    out = capsys.readouterr().out
    # The zorder line only. Checking the whole output would catch the evolution
    # warning, which names different engines for a different reason.
    line = next(ln for ln in out.splitlines() if "zorder columns" in ln)
    lacking = engines_lacking(LayoutFeature.ZORDER)
    assert lacking, "no engine lacks Z-order; this test no longer means anything"
    for engine in lacking:
        assert engine in line
    for engine in {"local", "spark", "trino"} - set(lacking):
        assert engine not in line


def test_summary_warns_about_evolution_on_the_engines_that_lack_it(tmp_path, capsys):
    """Partition evolution is local-only today, and a config that declares it is
    silently a no-op everywhere else."""
    from zamboni.cli import main
    from zamboni.maintainers import LayoutFeature, engines_lacking

    config = tmp_path / "tc.json"
    config.write_text(
        '{"version": 2, "warehouse": "acme", "namespaces": {"db": {"tables": {"events": {}}}}}'
    )
    main(["table-config", "summary", str(config)])

    out = capsys.readouterr().out
    for engine in engines_lacking(LayoutFeature.PARTITION_EVOLUTION):
        assert engine in out


def test_a_warning_disappears_when_the_engine_gains_the_feature(tmp_path, capsys, monkeypatch):
    """The property that makes this derived rather than decorative.

    Grant Trino Z-order and the summary must stop warning, with no edit to the
    CLI. If this fails, the warning has drifted back into a literal.
    """
    from dataclasses import replace

    from zamboni.cli import main
    from zamboni.maintainers import LayoutFeature
    from zamboni.maintainers.trino import TrinoMaintainer

    granted = replace(
        TrinoMaintainer.capabilities(),
        layout=TrinoMaintainer.capabilities().layout | {LayoutFeature.ZORDER},
    )
    monkeypatch.setattr(TrinoMaintainer, "capabilities", classmethod(lambda cls: granted))

    config = tmp_path / "tc.json"
    config.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "acme",
                "namespaces": {
                    "db": {
                        "tables": {
                            "events": {
                                "ordering": {"mode": "zorder", "zorder": {"columns": ["a", "b"]}}
                            }
                        }
                    }
                },
            }
        )
    )
    main(["table-config", "summary", str(config)])

    out = capsys.readouterr().out
    assert "zorder columns a, b" in out
    assert "not available on" not in out.split("zorder columns a, b")[1].split("\n")[0]


def test_table_config_validate_matches_the_original_verb(tmp_path, capsys):
    """`validate-config` shipped in 0.1.0 and removing a verb is breaking under
    docs/releasing.md, so the new grouped name is an alias, not a second
    implementation. This pins them to the same output."""
    from zamboni.cli import main

    config = tmp_path / "tc.json"
    config.write_text(
        '{"version": 2, "warehouse": "acme", "namespaces": {"db": {"tables": {"events": {}}}}}'
    )

    assert main(["validate-config", str(config)]) == 0
    old = capsys.readouterr().out
    assert main(["table-config", "validate", str(config)]) == 0
    assert capsys.readouterr().out == old


# -- flag defaults track the dataclass (ZMBNI-1910) -----------------------


def test_cli_defaults_match_the_dataclass():
    """A default written down twice is a default that will disagree once.

    `--memory-budget-bytes` was `1 << 30` here while the dataclass said 256MiB,
    so ZMBNI-1906's lowered threshold reached Python callers and not the CLI --
    every command-line run kept the old behaviour and the fix silently missed
    the people most likely to need it.
    """
    from zamboni import CompactionConfig
    from zamboni.cli import _build_parser, _operational_config

    args = _build_parser().parse_args(["compact", "db.t", "--local-warehouse", "/tmp/x"])
    defaults = CompactionConfig()
    built = _operational_config(args)

    for field in (
        "memory_budget_bytes",
        "read_ahead_bytes",
        "max_read_ahead_files",
        "memory_mode",
        "rewrite_all",
        "partial_progress",
        "dangling_delete_policy",
    ):
        assert getattr(built, field) == getattr(defaults, field), (
            f"the CLI default for {field} has drifted from CompactionConfig"
        )


def test_every_operational_knob_is_reachable_from_the_command_line():
    """The knobs existed for a release before any flag set them.

    `read_ahead_bytes` and `max_read_ahead_files` were added to the dataclass
    and wired into the backend, and nothing on the command line could reach
    them -- documented settings an operator could not use. The exclusions below
    are deliberate and each says why.
    """
    from dataclasses import fields

    from zamboni import CompactionConfig
    from zamboni.cli import _build_parser

    # Layout, not operations: these come from table-config.json so that analysts
    # own them and an operator does not set them per-run.
    layout_owned = {
        "target_file_size_bytes",  # flag exists too, but the file wins
        "zorder_columns",
        "zorder_precision_bits",
    }

    parser = _build_parser()
    args = parser.parse_args(["compact", "db.t", "--local-warehouse", "/tmp/x"])
    known = set(vars(args))
    # `--sort-by` sets `sort_expression`; argparse dest names are what count.
    missing = [
        f.name
        for f in fields(CompactionConfig)
        if f.name not in layout_owned and f.name not in known
    ]

    assert not missing, f"CompactionConfig settings with no CLI flag: {missing}"


# -- secret handling (ZMBNI-1811, ZMBNI-1812) -----------------------------


@pytest.mark.parametrize(
    ("flag", "variable"),
    [
        ("--token", "ZAMBONI_TOKEN"),
        ("--credential", "ZAMBONI_CREDENTIAL"),
        ("--s3-secret-access-key", "ZAMBONI_S3_SECRET_ACCESS_KEY"),
    ],
)
def test_the_secret_flags_are_removed_and_say_where_to_put_the_value(flag, variable, capsys):
    """Removed rather than warned about: a command line is readable by every
    local user, verified by reading a token back out of /proc/<pid>/cmdline.

    They still parse, only to fail usefully. Deleting the argument outright
    gives `unrecognized arguments: --token`, which tells an operator running
    yesterday's script nothing about where the value goes now.
    """
    from zamboni.cli import main

    with pytest.raises(SystemExit) as exit_info:
        main(["describe", "db.t", flag, "hunter2"])

    assert exit_info.value.code == 2
    message = capsys.readouterr().err
    assert "was removed" in message
    assert variable in message
    assert "hunter2" not in message, "the error must not echo the value back"


def test_a_key_id_is_still_a_flag():
    """An access key id is an identifier, not a secret. Removing it alongside
    the secret would be cargo-culting the rule rather than applying it."""
    from zamboni.cli import _build_parser

    args = _build_parser().parse_args(
        ["describe", "db.t", "--local-warehouse", "/tmp/x", "--s3-access-key-id", "AKIA"]
    )
    assert args.s3_access_key_id == "AKIA"


def test_the_s3_settings_repr_redacts_the_secret():
    """A frozen dataclass prints every field, so the secret appeared in full
    anywhere this object reached a formatted string -- a traceback rendered with
    locals, a debug log, an error aggregator. Nothing logs it today, which is
    exactly why it would have gone unnoticed until something did."""
    from zamboni import S3Settings

    settings = S3Settings(
        endpoint="http://minio:9000",
        access_key_id="AKIA_IDENTIFIER",
        secret_access_key="hunter2",
        extra={"s3.connect-timeout": "10"},
    )

    text = repr(settings)
    assert "hunter2" not in text
    assert "AKIA_IDENTIFIER" in text, "the key id identifies which credential failed"
    assert "10" not in text, "extra may hold secrets too, so its values stay out"
    # The properties still carry it, or nothing could authenticate.
    assert settings.as_properties()["s3.secret-access-key"] == "hunter2"
