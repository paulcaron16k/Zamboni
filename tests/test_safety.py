"""Blockers, guards, and the failure paths that must not corrupt a table."""

from __future__ import annotations

import pytest

from zamboni import CompactionConfig, MemoryMode, TableCompactor
from zamboni.committer import (
    ConcurrentModification,
    ReplaceCommitter,
    assert_supported_pyiceberg,
)
from zamboni.compactor import CompactionBlocked
from zamboni.profile import Finding, Severity, TableProfile, profile_table

from .conftest import SCHEMA, batch


def test_pyiceberg_internals_are_as_expected():
    """The private-API contract this package leans on.

    If this fails after a PyIceberg upgrade, read the guard's docstring before
    changing anything -- it explains which behaviour changed and why it matters.
    """
    assert_supported_pyiceberg()  # raises UnsupportedPyIceberg if not


def test_format_version_1_is_blocked(session):
    tbl = session.catalog.create_table("db.v1", schema=SCHEMA, properties={"format-version": "1"})
    for i in range(3):
        tbl.append(batch(i * 10, 10))

    profile = profile_table(tbl)
    assert any(f.code == "format-version-1" for f in profile.blockers)

    with pytest.raises(CompactionBlocked, match="format-version-1"):
        TableCompactor(session, "db.v1", CompactionConfig()).execute()


def test_blockers_and_warnings_are_separated():
    profile = TableProfile(
        identifier="db.t",
        format_version=2,
        snapshot_id=1,
        live_files=[],
        position_delete_files=0,
        equality_delete_files=0,
        spec_ids=set(),
        default_spec_id=0,
        sort_order_id=None,
        findings=[
            Finding(Severity.BLOCKER, "equality-deletes", "..."),
            Finding(Severity.WARNING, "dangling-delete-files", "..."),
        ],
    )
    assert [f.code for f in profile.blockers] == ["equality-deletes"]
    assert [f.code for f in profile.warnings] == ["dangling-delete-files"]


def test_concurrent_write_is_refused_not_lost(session, unpartitioned):
    """A writer that lands between planning and commit must fail the commit."""
    stale_snapshot_id = unpartitioned.metadata.current_snapshot_id

    tbl = session.table("db.unpartitioned")
    tbl.append(batch(999, 5))  # someone else commits

    tbl = session.table("db.unpartitioned")
    with pytest.raises(ConcurrentModification, match="snapshot changed"):
        ReplaceCommitter().commit(
            tbl,
            expected_snapshot_id=stale_snapshot_id,
            removed=[],
            added=[_any_live_data_file(tbl)],
        )


def test_row_count_mismatch_aborts_before_commit(session, unpartitioned, monkeypatch):
    """A rewrite that loses rows must never reach the table."""
    from zamboni.backends.duckdb_arrow import DuckDBArrowBackend

    original = DuckDBArrowBackend._read_table

    def lossy(self, tasks, ctx):
        return original(self, tasks, ctx).slice(0, 5)

    monkeypatch.setattr(DuckDBArrowBackend, "_read_table", lossy)

    before = profile_table(unpartitioned).snapshot_id
    with pytest.raises(RuntimeError, match="nothing was committed"):
        TableCompactor(
            session,
            "db.unpartitioned",
            CompactionConfig(memory_mode=MemoryMode.IN_MEMORY),
        ).execute()

    assert profile_table(session.table("db.unpartitioned")).snapshot_id == before


def test_copy_on_write_delete_then_compact_preserves_survivors(session, unpartitioned):
    """CoW deletes are materialised into the data files before we ever see them."""
    from pyiceberg.expressions import LessThan

    unpartitioned.delete(LessThan("id", 20))
    tbl = session.table("db.unpartitioned")
    expected = sorted(tbl.scan().to_arrow()["id"].to_pylist())

    TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()

    tbl = session.table("db.unpartitioned")
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == expected
    assert all(i >= 20 for i in expected)


def test_empty_table_is_a_no_op(session):
    tbl = session.catalog.create_table(
        "db.empty", schema=SCHEMA, properties={"format-version": "2"}
    )
    assert tbl.current_snapshot() is None

    result = TableCompactor(session, "db.empty", CompactionConfig()).execute()
    assert result.rewritten_data_files == 0
    assert session.table("db.empty").current_snapshot() is None


def test_overwrite_operation_fallback(session, unpartitioned):
    """The escape hatch for anyone unwilling to subclass PyIceberg internals."""
    TableCompactor(
        session, "db.unpartitioned", CompactionConfig(snapshot_operation="overwrite")
    ).execute()

    snapshot = session.table("db.unpartitioned").current_snapshot()
    assert snapshot.summary.operation.value == "overwrite"


def _any_live_data_file(tbl):
    return profile_table(tbl).live_files[0].data_file
