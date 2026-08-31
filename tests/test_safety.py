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


def test_every_replace_producer_consults_the_guard(monkeypatch):
    """The property, not today's five call sites (ZMBNI-37).

    `assert_supported_pyiceberg()` refuses a PyIceberg whose
    `_OverwriteFiles._existing_manifests` prunes by predicate without deriving
    that predicate correctly -- on such a build the manifest holding a replaced
    file is kept verbatim and its rows are counted twice.

    It used to have one caller, `TableCompactor.execute`. Five of the six
    mutating operations never reached it, and two of them -- `rewrite-manifests`
    and `remove-dangling-deletes` -- commit through subclasses of
    `_ReplaceFiles`, which is the machinery the guard exists to protect.
    Compaction refused on such a build and those two proceeded.

    Enumerating `__subclasses__()` rather than listing the operations is what
    makes this survive a seventh one: a new producer is covered by existing, and
    a subclass that overrides `__init__` without calling `super()` fails here
    rather than silently opting out.
    """
    import inspect

    from zamboni import committer
    from zamboni.committer import UnsupportedPyIceberg, _ReplaceFiles

    def refuse() -> None:
        raise UnsupportedPyIceberg("simulated unusable build")

    monkeypatch.setattr(committer, "assert_supported_pyiceberg", refuse)

    def descendants(cls) -> list[type]:
        """Every subclass, not just the direct ones.

        `type.__subclasses__()` returns direct children only, so a *grandchild*
        overriding `__init__` without calling `super()` bypassed the guard while
        this test passed. Found in review, reproduced with a subclass of
        `_RewriteManifests`.
        """
        found = []
        for sub in cls.__subclasses__():
            found.append(sub)
            found.extend(descendants(sub))
        return found

    # Import every module that defines one, so the walk cannot report complete
    # coverage of a set that is short because something was never imported.
    from zamboni import deletes, evolution, manifests  # noqa: F401

    producers = [_ReplaceFiles, *descendants(_ReplaceFiles)]
    expected = {"_ReplaceFiles", "MultiSpecReplaceFiles", "_RewriteManifests", "_RemoveDeleteFiles"}
    assert {p.__name__ for p in producers} >= expected, (
        f"expected {sorted(expected)}, found {sorted(p.__name__ for p in producers)}. "
        "A producer moved, or its module is no longer imported"
    )

    for producer in producers:
        # Required arguments are read off each producer rather than listed:
        # `_RewriteManifests` takes `bins` and `kept` and binds them before
        # delegating upwards, so a hardcoded call would raise TypeError and
        # never reach the guard -- passing the test for the wrong reason.
        kwargs = {
            name: None
            for name, param in inspect.signature(producer.__init__).parameters.items()
            if param.default is inspect.Parameter.empty
            and param.kind
            in (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            and name != "self"
        }
        kwargs.setdefault("operation", None)
        kwargs.setdefault("transaction", None)
        kwargs.setdefault("io", None)

        with pytest.raises(UnsupportedPyIceberg):
            # The values never matter: the guard runs before `super().__init__`,
            # so construction cannot get far enough to need a real transaction.
            producer(**kwargs)


def test_the_committer_refuses_even_when_it_picks_the_stock_producer(monkeypatch):
    """The hole that guarding `_ReplaceFiles` alone left open (ZMBNI-37).

    `ReplaceCommitter.commit` chooses its producer at runtime, and two of the
    three choices are *not* `_ReplaceFiles`:

    * `snapshot_operation="overwrite"`, a documented `CompactionConfig` option,
      selects the stock `_OverwriteFiles`;
    * so does any build whose `replace_summary_supported` is true -- a future
      PyIceberg, which is exactly the kind most likely to carry the pruning
      defect the guard exists for.

    So the first version of this fix made coverage a property of a class that is
    itself a runtime choice. `ReplaceCommitter` is public API too, so a library
    caller reaches `commit()` without going through `TableCompactor.execute`.
    """
    from zamboni import committer
    from zamboni.committer import ReplaceCommitter, UnsupportedPyIceberg

    def refuse() -> None:
        raise UnsupportedPyIceberg("simulated unusable build")

    monkeypatch.setattr(committer, "assert_supported_pyiceberg", refuse)

    for snapshot_operation in ("replace", "overwrite"):
        with pytest.raises(UnsupportedPyIceberg):
            # `added`/`removed` are non-empty so the early return for a no-op
            # commit cannot be what raises -- and the guard runs before it anyway.
            ReplaceCommitter(snapshot_operation=snapshot_operation).commit(
                None, expected_snapshot_id=None, removed=[object()], added=[object()]
            )


def test_upsert_on_a_transformed_partition_replaces_rather_than_duplicates(session):
    """The upstream regression the `<0.12` cap exists for, as a test (ZMBNI-19).

    PyIceberg 0.12 release candidates corrupt a partitioned `upsert`: the row
    that should have been replaced survives *beside* its replacement, silently,
    and a later `upsert` then fails on the duplicates the earlier one created.
    Filed as apache/iceberg-python#3758, fixed by #3780.

    **The transform has to be non-identity**, which is the whole shape of the
    bug: a data file records its partition values already transformed, so a
    predicate comparing a source column against a partition value only holds for
    identity. `EqualTo(Reference('ts'), LongLiteral(20455))` compares a timestamp
    against a day ordinal. Verified upstream that `identity`, `truncate` and
    `bucket` are all correct and `year`/`month`/`day`/`hour` all duplicate.

    This replaces `docs/upstream-0.12-upsert-regression.md`, whose 165 lines
    existed to say in prose what these assertions say by failing. It is a
    *tripwire*, not a trivial pass: 0.11.1 is the correct build, so this passes
    today and exists to fail the moment the cap is lifted onto a build that
    regressed. Upstream carries its own regression test too, as of
    `0bf4d13d` -- which is what makes deleting the prose safe rather than merely
    tidy.

    Zamboni does not call `upsert`; ingestion does. The reproduction uses no
    Zamboni code, and lives here because this is where the reason for a
    dependency bound belongs.
    """
    import datetime as dt

    import pyarrow as pa
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import IntegerType, NestedField, StringType, TimestampType

    table = session.catalog.create_table(
        "db.upsert_regression",
        schema=Schema(
            NestedField(1, "k", StringType(), required=False),
            NestedField(2, "v", IntegerType(), required=False),
            NestedField(3, "ts", TimestampType(), required=False),
        ),
        partition_spec=PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="ts_day")
        ),
        properties={"format-version": "2"},
    )

    arrow = pa.schema(
        [
            pa.field("k", pa.string()),
            pa.field("v", pa.int32()),
            pa.field("ts", pa.timestamp("us")),
        ]
    )
    when = dt.datetime(2026, 1, 6, 12)

    def rows(pairs: list[tuple[str, int]]) -> pa.Table:
        return pa.table(
            {
                "k": [k for k, _ in pairs],
                "v": pa.array([v for _, v in pairs], type=pa.int32()),
                "ts": [when] * len(pairs),
            },
            schema=arrow,
        )

    # Two rows in one partition, then replace one. Both parts matter: the
    # minimum condition for the defect is a manifest holding a replaced row *and*
    # a survivor, because the survivor is what forces the rewrite that reaches
    # the faulty predicate. Append one row and upsert it and every entry is
    # deleted, the manifest is dropped whole, and the bug never fires.
    table.append(rows([("a", 1), ("b", 1)]))
    table.refresh()
    table.upsert(rows([("a", 2)]), join_cols=["k"])
    table.refresh()

    got = sorted(
        zip(
            table.scan().to_arrow()["k"].to_pylist(),
            table.scan().to_arrow()["v"].to_pylist(),
            strict=True,
        )
    )
    assert got == [("a", 2), ("b", 1)], (
        f"upsert on a day-partitioned table returned {got}. Expected the replaced "
        "row to be gone: this is apache/iceberg-python#3758, and a build "
        "exhibiting it must not be inside the pyiceberg bound in pyproject.toml"
    )
