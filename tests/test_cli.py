"""CLI surface: the read-only verbs must stay read-only, and compact must not
fire without consent."""

from __future__ import annotations

import json

import pytest

from zamboni.cli import main
from zamboni.profile import profile_table

from .conftest import SCHEMA, batch


@pytest.fixture
def warehouse(tmp_path, session):
    """A populated local warehouse addressable by --local-warehouse."""
    tbl = session.catalog.create_table(
        "db.events", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(6):
        tbl.append(batch(i * 10, 10))
    # session's warehouse dir is tmp_path/warehouse (see conftest)
    return str(tmp_path / "warehouse")


def test_doctor_needs_no_catalog(capsys):
    assert main(["doctor"]) == 0
    assert "pyiceberg" in capsys.readouterr().out


def test_describe_is_read_only(warehouse, session, capsys):
    before = profile_table(session.table("db.events")).snapshot_id

    assert main(["describe", "db.events", "--local-warehouse", warehouse]) == 0

    assert "live data files : 6" in capsys.readouterr().out
    assert profile_table(session.table("db.events")).snapshot_id == before


def test_plan_is_read_only(warehouse, session, capsys):
    before = profile_table(session.table("db.events")).snapshot_id

    assert main(["plan", "db.events", "--local-warehouse", warehouse]) == 0

    assert "group 0" in capsys.readouterr().out
    assert profile_table(session.table("db.events")).snapshot_id == before


def test_compact_previews_without_consent(warehouse, session, capsys):
    """A bare invocation changes nothing and says so.

    `compact` used to exit 2 here while the other five verbs previewed, so the
    command people reach for first behaved unlike everything else. Reconciled
    toward the safe default: naming a verb never mutates.
    """
    before = profile_table(session.table("db.events")).snapshot_id

    assert main(["compact", "db.events", "--local-warehouse", warehouse]) == 0

    assert "dry run" in capsys.readouterr().out
    assert profile_table(session.table("db.events")).snapshot_id == before


def test_compact_dry_run_changes_nothing(warehouse, session):
    before = profile_table(session.table("db.events")).snapshot_id

    assert main(["compact", "db.events", "--local-warehouse", warehouse, "--dry-run"]) == 0

    assert profile_table(session.table("db.events")).snapshot_id == before


def test_compact_with_yes_commits(warehouse, session, capsys):
    assert main(["compact", "db.events", "--local-warehouse", warehouse, "--yes"]) == 0

    assert "rewrote 6 file(s)" in capsys.readouterr().out
    after = profile_table(session.table("db.events"))
    assert len(after.live_files) == 1
    assert after.total_records == 60


def test_blocked_table_exits_nonzero(tmp_path, session, capsys):
    tbl = session.catalog.create_table("db.v1", schema=SCHEMA, properties={"format-version": "1"})
    for i in range(3):
        tbl.append(batch(i * 10, 10))

    code = main(
        [
            "compact",
            "db.v1",
            "--local-warehouse",
            str(tmp_path / "warehouse"),
            "--yes",
        ]
    )

    assert code == 3
    assert "format-version-1" in capsys.readouterr().err


def test_rest_catalog_requires_uri_and_warehouse(capsys, monkeypatch):
    """Hermetic on purpose.

    Every catalog flag also reads a ZAMBONI_* variable, and dev-stack/README.md
    tells developers to export ZAMBONI_URI and ZAMBONI_WAREHOUSE. Without
    clearing them this test passes on a clean shell and fails on the shell of
    anyone who followed the instructions -- which is exactly what happened.
    """
    for var in (
        "ZAMBONI_URI",
        "ZAMBONI_WAREHOUSE",
        "ZAMBONI_CREDENTIAL",
        "ZAMBONI_TOKEN",
        "ZAMBONI_LOCAL_WAREHOUSE",
    ):
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(SystemExit):
        main(["describe", "db.events"])
    assert "--uri and --warehouse" in capsys.readouterr().err


# -- reclaim verbs -------------------------------------------------------


@pytest.fixture
def reclaim_config(tmp_path):
    """Retention aggressive enough to act on a freshly-built table."""
    path = tmp_path / "retention.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "tables": {
                    "db.events": {
                        "retention": {
                            "expire_snapshots": {
                                "enabled": True,
                                "min_snapshots_to_keep": 2,
                                "max_snapshot_age_days": 0,
                            },
                            "remove_orphan_files": {"enabled": True, "older_than_days": 0},
                        }
                    }
                },
            }
        )
    )
    return path


def test_expire_dry_run_changes_nothing(warehouse, session, reclaim_config, capsys):
    before = len(session.table("db.events").metadata.snapshots)

    assert (
        main(
            [
                "expire",
                "db.events",
                "--local-warehouse",
                warehouse,
                "--table-config",
                str(reclaim_config),
            ]
        )
        == 0
    )

    out = capsys.readouterr().out
    assert "would delete" in out and "dry run" in out
    assert len(session.table("db.events").metadata.snapshots) == before


def test_expire_with_yes_reclaims(warehouse, session, reclaim_config, capsys):
    tbl = session.table("db.events")
    before_rows = tbl.scan().to_arrow().num_rows
    before_snaps = len(tbl.metadata.snapshots)

    assert (
        main(
            [
                "expire",
                "db.events",
                "--local-warehouse",
                warehouse,
                "--table-config",
                str(reclaim_config),
                "--yes",
            ]
        )
        == 0
    )

    out = capsys.readouterr().out
    assert "expired" in out and "deleted" in out

    tbl = session.table("db.events")
    assert len(tbl.metadata.snapshots) < before_snaps
    assert tbl.scan().to_arrow().num_rows == before_rows, "expiry changed the data"


def test_expire_respects_a_disabled_config(warehouse, session, tmp_path, capsys):
    config = tmp_path / "off.json"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "tables": {
                    "db.events": {
                        "retention": {
                            "expire_snapshots": {"enabled": False},
                            "remove_orphan_files": {"enabled": False},
                        }
                    }
                },
            }
        )
    )
    before = len(session.table("db.events").metadata.snapshots)

    assert (
        main(
            [
                "expire",
                "db.events",
                "--local-warehouse",
                warehouse,
                "--table-config",
                str(config),
                "--yes",
            ]
        )
        == 0
    )

    assert "disabled" in capsys.readouterr().out
    assert len(session.table("db.events").metadata.snapshots) == before


def test_remove_orphans_reports_and_warns_on_a_zero_guard(
    warehouse, session, reclaim_config, capsys
):
    assert (
        main(
            [
                "remove-orphans",
                "db.events",
                "--local-warehouse",
                warehouse,
                "--table-config",
                str(reclaim_config),
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "scanned" in captured.out and "dry run" in captured.out
    # A zero-day guard is exactly what the age guard exists to prevent, so it
    # must never pass silently.
    assert "zero-day age guard" in captured.err


def test_remove_orphans_deletes_a_planted_file(warehouse, session, reclaim_config, capsys):
    from pathlib import Path

    tbl = session.table("db.events")
    stray = Path(tbl.location().replace("file://", "")) / "data" / "stray.parquet"
    stray.write_bytes(b"orphan")

    assert (
        main(
            [
                "remove-orphans",
                "db.events",
                "--local-warehouse",
                warehouse,
                "--table-config",
                str(reclaim_config),
                "--yes",
            ]
        )
        == 0
    )

    assert "1 unreferenced" in capsys.readouterr().out
    assert not stray.exists()
    assert session.table("db.events").scan().to_arrow().num_rows == 60


def test_remove_orphans_aborts_with_a_distinct_exit_code(warehouse, session, capsys):
    """A safety abort must be distinguishable from an ordinary failure."""
    from pathlib import Path

    from zamboni.reachable import Category, reachable_files

    tbl = session.table("db.events")
    live = sorted(reachable_files(tbl).by_category[Category.DATA])[0]
    Path(live).unlink()

    code = main(
        [
            "remove-orphans",
            "db.events",
            "--local-warehouse",
            warehouse,
            "--older-than-days",
            "0",
            "--yes",
        ]
    )

    assert code == 4
    assert "aborted, nothing deleted" in capsys.readouterr().err


def test_reclaim_verbs_work_without_a_table_config(warehouse, session, capsys):
    """Defaults alone must be usable; the config is an override, not a requirement."""
    assert main(["expire", "db.events", "--local-warehouse", warehouse]) == 0
    out = capsys.readouterr().out
    # Default retention is 5 days / keep 1, and these snapshots are seconds old,
    # so nothing should be eligible.
    assert "expired 0 snapshot(s)" in out


def test_expire_aborts_with_the_same_exit_code_as_orphans(
    warehouse, session, reclaim_config, capsys, monkeypatch
):
    """Both reclaim verbs report a failed safety check the same way.

    An operator scripting these should not have to learn two exit codes for
    "the tool stopped because it no longer trusts its own arithmetic".
    """
    from zamboni.expire import ExpiryAborted

    def refuse(self, tbl, *, now=None):
        raise ExpiryAborted("db.events: 3 file(s) marked for deletion are still referenced")

    monkeypatch.setattr("zamboni.expire.SnapshotExpirer.run", refuse)

    code = main(
        [
            "expire",
            "db.events",
            "--local-warehouse",
            warehouse,
            "--table-config",
            str(reclaim_config),
            "--yes",
        ]
    )
    assert code == 4
    assert "aborted, nothing deleted" in capsys.readouterr().err


def test_a_negative_age_guard_is_a_usage_error_not_a_crash(warehouse, session, capsys):
    """argparse takes any int, so the range check can only fire in the handler."""
    code = main(
        [
            "remove-orphans",
            "db.events",
            "--local-warehouse",
            warehouse,
            "--older-than-days",
            "-1",
        ]
    )
    assert code == 2
    err = capsys.readouterr().err
    assert "older_than_days must be >= 0" in err
    assert "Traceback" not in err


def test_every_mutating_verb_says_what_omitting_yes_does(capsys):
    """Discoverability, not decoration.

    All six treat a missing `--yes` as a dry run, and their help text said only
    "actually delete" -- so an operator reading --help could not find the safe
    path. The runbook exposed the gap; this keeps it closed.

    `compact` belongs here now that it previews like the rest (ZMBNI-911). It was
    absent while it still exited 2, which is worth saying: this check and the
    behaviour it guards have drifted apart once already.
    """
    for verb in (
        "compact",
        "expire",
        "remove-orphans",
        "remove-dangling-deletes",
        "rewrite-manifests",
        "apply-properties",
    ):
        with pytest.raises(SystemExit):
            main([verb, "--help"])
        # argparse hard-wraps help text, so "dry run" can arrive as
        # "dry\n                        run". Compare on collapsed whitespace.
        out = " ".join(capsys.readouterr().out.lower().split())
        assert "dry run" in out, f"{verb} --help does not mention the dry run"


def test_no_verb_mutates_without_yes(warehouse, session, capsys):
    """One rule for all six, which is what makes it stateable.

    Previously five previewed and `compact` exited 2, so runbook.md had to explain
    the exception instead of giving a rule. Every mutating verb is now checked
    here, so the rule cannot quietly acquire another exception.
    """
    before = profile_table(session.table("db.events")).snapshot_id

    for verb in (
        "compact",
        "expire",
        "remove-orphans",
        "remove-dangling-deletes",
        "rewrite-manifests",
        "apply-properties",
    ):
        code = main([verb, "db.events", "--local-warehouse", warehouse])
        out = capsys.readouterr().out
        assert code == 0, f"{verb} exited {code} on a bare invocation"
        assert "dry run" in out, f"{verb} did not say it was a dry run:\n{out}"

    assert profile_table(session.table("db.events")).snapshot_id == before, (
        "a bare invocation committed something"
    )
