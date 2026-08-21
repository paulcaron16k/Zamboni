"""The shared maintenance loop -- the one `zamboni maintenance` and an
application both run.

It lived only in the CLI, and the user guide told integrators to write their own
copy: the operation order, the `fulfilled_by` skip, which exceptions are
refusals rather than failures, and when to stop existed twice. These tests are
mostly about the decisions a caller no longer has to make.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField

from zamboni import CatalogSession, maintain
from zamboni.maintainers import Operation
from zamboni.maintenance import RUNBOOK_ORDER, Outcome
from zamboni.tableconfig import TableConfigError

SCHEMA = Schema(NestedField(1, "id", IntegerType(), required=False))
ARROW = pa.schema([pa.field("id", pa.int32(), nullable=True)])


@pytest.fixture
def warehouse(tmp_path):
    root = tmp_path / "wh"
    root.mkdir()
    session = CatalogSession.for_local(warehouse_path=str(root))
    session.catalog.create_namespace("db")
    table = session.catalog.create_table(
        "db.events", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(6):
        table.append(pa.table({"id": pa.array([i], type=pa.int32())}, schema=ARROW))
    yield session
    session.close()


def config(tmp_path, **root):
    path = tmp_path / "table-config.json"
    doc = {
        "version": 2,
        "warehouse": "local",
        "namespaces": {"db": {"tables": {"events": {}}}},
    }
    doc.update(root)
    path.write_text(json.dumps(doc))
    return path


def test_one_call_runs_every_operation_in_order(warehouse, tmp_path):
    """The point of the function: a caller writes one line, not a loop."""
    report = maintain(warehouse, table_config=config(tmp_path), commit=True)

    assert report.exit_code == 0
    assert [o.operation for o in report.outcomes] == list(RUNBOOK_ORDER)
    assert report.tables == ("db.events",)


def test_it_actually_compacts(warehouse, tmp_path):
    before = len(list(warehouse.table("db.events").scan().plan_files()))
    assert before == 6

    maintain(warehouse, table_config=config(tmp_path), commit=True)

    assert len(list(warehouse.table("db.events").scan().plan_files())) == 1


def test_nothing_commits_without_asking(warehouse, tmp_path):
    """`commit=False` is the default for the same reason `--yes` is required:
    a caller who has not thought about it gets the safe answer."""
    before = warehouse.table("db.events").metadata.current_snapshot_id

    report = maintain(warehouse, table_config=config(tmp_path))

    assert report.exit_code == 0
    assert warehouse.table("db.events").metadata.current_snapshot_id == before


def test_an_operation_disabled_in_the_config_is_skipped_not_failed(warehouse, tmp_path):
    # Not expiry: the config refuses "orphan removal on, expiry off" as a
    # contradiction, since snapshots hold the references that make superseded
    # files reclaimable. Manifest rewriting has no such coupling.
    path = config(
        tmp_path,
        namespaces={
            "db": {"tables": {"events": {"retention": {"rewrite_manifests": {"enabled": False}}}}}
        },
    )

    report = maintain(warehouse, table_config=path, commit=True)

    outcome = next(o for o in report.outcomes if o.operation is Operation.REWRITE_MANIFESTS)
    assert outcome.exit_code == 0
    assert outcome.skipped
    assert "disabled" in outcome.detail


def test_an_unsupported_operation_is_skipped_not_failed(warehouse, tmp_path, monkeypatch):
    """Trino cannot remove dangling deletes. A nightly fleet run should carry on
    rather than fail every night over a declared limit."""
    from zamboni.maintainers import UnsupportedOperation
    from zamboni.maintainers.local import LocalMaintainer

    def refuse(self, operation):
        if operation is Operation.REWRITE_MANIFESTS:
            raise UnsupportedOperation("nope")

    monkeypatch.setattr(LocalMaintainer, "check_supported", refuse)

    report = maintain(warehouse, table_config=config(tmp_path), commit=True)

    outcome = next(o for o in report.outcomes if o.operation is Operation.REWRITE_MANIFESTS)
    assert outcome.exit_code == 0
    assert outcome.skipped
    assert report.exit_code == 0


def test_a_safety_abort_stops_that_table_and_reports_exit_4(warehouse, tmp_path, monkeypatch):
    """Everything after an abort reads the state we have just said we do not
    trust, so continuing would be doing more work on a warehouse we distrust."""
    from zamboni.maintainers.local import LocalMaintainer
    from zamboni.orphans import OrphanCleanupAborted

    original = LocalMaintainer.execute

    def abort(self, operation, table, *, request, dry_run):
        if operation is Operation.EXPIRE:
            raise OrphanCleanupAborted("a referenced file is missing")
        return original(self, operation, table, request=request, dry_run=dry_run)

    monkeypatch.setattr(LocalMaintainer, "execute", abort)

    report = maintain(warehouse, table_config=config(tmp_path), commit=True)

    assert report.exit_code == 4
    ran = [o.operation for o in report.outcomes]
    assert Operation.EXPIRE in ran
    # remove-orphans follows expire in the runbook order and must not have run.
    assert Operation.REMOVE_ORPHANS not in ran


def test_the_worst_exit_code_wins_not_the_last(warehouse, tmp_path, monkeypatch):
    """A partial failure reported as success is the failure mode this is for:
    it is what a cron line alerts on, and nobody reads the log otherwise."""
    from zamboni.compactor import CompactionBlocked
    from zamboni.maintainers.local import LocalMaintainer

    original = LocalMaintainer.execute

    def block_compact(self, operation, table, *, request, dry_run):
        if operation is Operation.COMPACT:
            raise CompactionBlocked("equality deletes")
        return original(self, operation, table, request=request, dry_run=dry_run)

    monkeypatch.setattr(LocalMaintainer, "execute", block_compact)

    report = maintain(warehouse, table_config=config(tmp_path), commit=True)

    assert report.outcomes[-1].exit_code == 0, "a later operation succeeded"
    assert report.exit_code == 3, "and the run still reports the blocked table"


def test_the_observer_sees_each_outcome_as_it_happens(warehouse, tmp_path):
    """A long fleet run needs progress, not a report at the end."""
    seen: list[Outcome] = []

    report = maintain(warehouse, table_config=config(tmp_path), commit=True, observer=seen.append)

    assert [o.operation for o in seen] == [o.operation for o in report.outcomes]


def test_a_config_for_another_warehouse_stops_the_run(warehouse, tmp_path):
    with pytest.raises(TableConfigError, match="maintaining 'globex'"):
        maintain(warehouse, table_config=config(tmp_path), warehouse="globex")


def test_a_missing_table_config_is_refused_rather_than_defaulted(warehouse):
    """Defaulting would be guessing on the caller's behalf about deleting their
    data: the config is what carries the retention."""
    with pytest.raises(TableConfigError, match="needs a table_config"):
        maintain(warehouse)


def test_the_cli_and_the_api_agree_on_the_exit_code(warehouse, tmp_path, monkeypatch, capsys):
    """The parity claim, asserted rather than stated.

    `zamboni maintenance` is a printing adapter over this function; if the two
    ever disagree on an exit code, one of them is lying to a cron line.
    """
    from zamboni.cli import main
    from zamboni.compactor import CompactionBlocked
    from zamboni.maintainers.local import LocalMaintainer

    original = LocalMaintainer.execute

    def block_compact(self, operation, table, *, request, dry_run):
        if operation is Operation.COMPACT:
            raise CompactionBlocked("equality deletes")
        return original(self, operation, table, request=request, dry_run=dry_run)

    monkeypatch.setattr(LocalMaintainer, "execute", block_compact)
    path = config(tmp_path)

    api = maintain(warehouse, table_config=path, commit=True).exit_code

    monkeypatch.setenv("ZAMBONI_LOCAL_WAREHOUSE", str(tmp_path / "wh"))
    monkeypatch.chdir(tmp_path)
    cli = main(["maintenance", "--table-config", str(path), "--yes"])
    capsys.readouterr()

    assert api == cli == 3
