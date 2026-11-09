"""A table config's ``ordering.mode: "none"`` honours the declared sort order (#209).

Shape (a) of the issue: "none" means no ordering of the file's own, so a table
that declares a sort order is compacted in that order and its files stamped with
its id -- as Trino's ``optimize`` always did -- and a table that declares none
stays binpack. An explicit ``sort`` or ``zorder`` wins. The CLI's own flags keep
their meaning: ``--sort-by-table-order`` asks for the order and refuses a table
without one, and no flag means no ordering.

The live engines are exercised in tests/test_dev_stack.py.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import BucketTransform, IdentityTransform

from tests.conftest import SCHEMA, batch
from zamboni import maintain
from zamboni.compactor import TableCompactor
from zamboni.config import (
    CompactionConfig,
    MemoryMode,
    config_from_table_settings,
    orders_by_declared_order,
)
from zamboni.maintainers import MaintenanceRequest, Operation
from zamboni.maintainers.spark import SparkMaintainer
from zamboni.profile import profile_table
from zamboni.tableconfig import Retention, TableConfig

ID_DESC = SortOrder(
    SortField(
        source_id=1,
        transform=IdentityTransform(),
        direction=SortDirection.DESC,
        null_order=NullOrder.NULLS_LAST,
    )
)
BUCKETED = SortOrder(SortField(source_id=1, transform=BucketTransform(4)))
UNSORTED = SortOrder()


def table_config(table: str = "sorted", **block) -> TableConfig:
    return TableConfig.from_dict(
        {"version": 2, "warehouse": "local", "namespaces": {"db": {"tables": {table: block}}}}
    )


def compact(session, table: str, config: TableConfig):
    report = maintain(
        session,
        table_config=config,
        operations=[Operation.COMPACT],
        commit=True,
        base_config=CompactionConfig(memory_mode=MemoryMode.IN_MEMORY),
    )
    (outcome,) = report.outcomes
    assert outcome.exit_code == 0, outcome.detail
    return outcome


def stamps(session, identifier: str) -> set[int | None]:
    return {f.data_file.sort_order_id for f in profile_table(session.table(identifier)).live_files}


def ids(session, identifier: str) -> list[int]:
    return session.table(identifier).scan().to_arrow()["id"].to_pylist()


# -- what the table config asks for ------------------------------------------------


@pytest.mark.parametrize(
    ("ordering", "falls_back"),
    [
        ({}, True),
        ({"mode": "none"}, True),
        ({"mode": "sort", "sort": [{"column": "id"}]}, False),
        ({"mode": "zorder", "zorder": {"columns": ["id", "category"]}}, False),
    ],
    ids=["omitted", "none", "sort", "zorder"],
)
def test_only_mode_none_falls_back_to_the_declared_order(ordering, falls_back):
    block = {"ordering": ordering} if ordering else {}
    config = config_from_table_settings(table_config(**block).for_table("db.sorted"))
    assert config.sort_by_declared_order_if_any is falls_back
    assert config.sort_by_table_order is False


def test_the_cli_without_a_table_config_keeps_no_flag_meaning_no_ordering():
    """Only the table config's "none" changed meaning: a bare CompactionConfig,
    which is what the CLI builds from its flags, still orders nothing."""
    assert CompactionConfig().sort_by_declared_order_if_any is False
    assert not orders_by_declared_order(CompactionConfig(), ID_DESC, identity_only=False)


@pytest.mark.parametrize(
    ("config", "order", "identity_only", "applies"),
    [
        (CompactionConfig(sort_by_declared_order_if_any=True), ID_DESC, True, True),
        (CompactionConfig(sort_by_declared_order_if_any=True), UNSORTED, False, False),
        (CompactionConfig(sort_by_declared_order_if_any=True), BUCKETED, True, False),
        (CompactionConfig(sort_by_declared_order_if_any=True), BUCKETED, False, True),
        (
            CompactionConfig(sort_by_declared_order_if_any=True, sort_expression="id"),
            ID_DESC,
            False,
            False,
        ),
        (
            CompactionConfig(sort_by_declared_order_if_any=True, zorder_columns=("id", "x")),
            ID_DESC,
            False,
            False,
        ),
        # Explicit: applies whatever the table says, and refuses later if it cannot.
        (CompactionConfig(sort_by_table_order=True), UNSORTED, True, True),
        (CompactionConfig(), ID_DESC, False, False),
    ],
    ids=[
        "declared",
        "none-declared",
        "bucket-on-an-identity-only-engine",
        "bucket-on-an-engine-that-can",
        "sort-wins",
        "zorder-wins",
        "explicit",
        "not-asked",
    ],
)
def test_when_the_declared_order_applies(config, order, identity_only, applies):
    assert orders_by_declared_order(config, order, identity_only=identity_only) is applies


# -- the local engine, through maintain() ------------------------------------------


def test_mode_none_orders_by_the_declared_order_and_stamps_its_id(session, sorted_table):
    compact(session, "db.sorted", table_config())

    assert ids(session, "db.sorted") == sorted(range(40), reverse=True)
    declared = session.table("db.sorted").sort_order().order_id
    assert declared != 0
    assert stamps(session, "db.sorted") == {declared}


def test_mode_none_on_a_table_with_no_declared_order_stays_binpack(session, unpartitioned):
    before = sorted(ids(session, "db.unpartitioned"))
    outcome = compact(session, "db.unpartitioned", table_config("unpartitioned"))

    assert outcome.result is not None and outcome.result.groups, "nothing was compacted"
    assert sorted(ids(session, "db.unpartitioned")) == before
    assert stamps(session, "db.unpartitioned") == {None}


@pytest.mark.parametrize(
    ("ordering", "expected"),
    [
        ({"mode": "sort", "sort": [{"column": "id"}]}, sorted(range(40))),
        # z-order on (id, category) with one category is id order: ascending,
        # which is not the declared DESC.
        ({"mode": "zorder", "zorder": {"columns": ["id", "category"]}}, sorted(range(40))),
    ],
    ids=["sort", "zorder"],
)
def test_an_explicit_ordering_wins_over_the_declared_one(session, sorted_table, ordering, expected):
    compact(session, "db.sorted", table_config(ordering=ordering))

    assert ids(session, "db.sorted") == expected
    assert stamps(session, "db.sorted") == {None}, "stamped an order the rows do not follow"


@pytest.fixture
def bucket_sorted(session):
    """A declared order the local engine cannot render: bucket(4, id)."""
    tbl = session.catalog.create_table(
        "db.bucket_sorted", schema=SCHEMA, sort_order=BUCKETED, properties={"format-version": "2"}
    )
    for i in range(4):
        tbl.append(batch(i * 10, 10))
    return tbl


def test_a_declared_order_the_local_engine_cannot_render_is_left_out_and_said(
    session, bucket_sorted
):
    """Not refused: nobody asked for it by name, and the table compacted before
    #209. Not approximated by the raw column either, which would stamp an id
    the rows do not satisfy."""
    outcome = compact(session, "db.bucket_sorted", table_config("bucket_sorted"))

    assert stamps(session, "db.bucket_sorted") == {None}
    assert any("not applied" in w for w in outcome.result.warnings), outcome.result.warnings


def test_asked_for_by_name_an_unrenderable_order_is_still_refused(session, bucket_sorted):
    config = CompactionConfig(sort_by_table_order=True, memory_mode=MemoryMode.IN_MEMORY)
    with pytest.raises(ValueError, match="not identity"):
        TableCompactor(session, "db.bucket_sorted", config).execute()


# -- the spark engine decides against the table it is about to rewrite ---------------


class _Table:
    def __init__(self, order: SortOrder) -> None:
        self._order = order

    def sort_order(self) -> SortOrder:
        return self._order


class _Session:
    def __init__(self, order: SortOrder) -> None:
        self.order = order
        self.loaded: list[str] = []

    def table(self, identifier: str) -> _Table:
        self.loaded.append(identifier)
        return _Table(self.order)


def spark_statement(order: SortOrder, operation=Operation.COMPACT, **compaction):
    session = _Session(order)
    maintainer = SparkMaintainer(session, {"catalog": "iceberg"})  # type: ignore[arg-type]
    request = MaintenanceRequest(
        retention=Retention(),
        compaction=CompactionConfig(sort_by_declared_order_if_any=True, **compaction),
    )
    settled = maintainer._settle_declared_order(operation, "db.events", request)
    return maintainer.statement_for(operation, "db.events", settled), session.loaded


@pytest.mark.parametrize("order", [ID_DESC, BUCKETED], ids=["identity", "bucket"])
def test_spark_sorts_by_a_declared_order_without_naming_one(order):
    """Spark's `sort` strategy defaults to the table's own order, every
    transform included -- so the bucket order Zamboni's engine leaves out is
    applied here."""
    sql, loaded = spark_statement(order)

    assert "strategy => 'sort'" in sql
    assert "sort_order =>" not in sql
    assert loaded == ["db.events"]


def test_spark_stays_binpack_on_a_table_with_no_declared_order():
    """`sort` with no `sort_order` on an unsorted table is an error in Spark,
    not a binpack, which is why the table is asked first."""
    sql, _ = spark_statement(UNSORTED)

    assert "strategy" not in sql


def test_spark_does_not_load_the_table_when_an_explicit_ordering_wins():
    sql, loaded = spark_statement(ID_DESC, sort_expression="id ASC")

    assert "sort_order => 'id ASC'" in sql
    assert loaded == []


def test_spark_statement_for_alone_never_guesses_the_declared_order():
    """statement_for is pure: an unsettled fallback is not read as a sort."""
    request = MaintenanceRequest(
        retention=Retention(), compaction=CompactionConfig(sort_by_declared_order_if_any=True)
    )
    sql = SparkMaintainer(None, {"catalog": "iceberg"}).statement_for(  # type: ignore[arg-type]
        Operation.COMPACT, "db.events", request
    )
    assert "strategy" not in sql


def test_spark_asks_nothing_for_an_operation_that_does_not_rewrite():
    session = _Session(ID_DESC)
    maintainer = SparkMaintainer(session, {"catalog": "iceberg"})  # type: ignore[arg-type]
    request = MaintenanceRequest(
        retention=Retention(), compaction=CompactionConfig(sort_by_declared_order_if_any=True)
    )
    assert maintainer._settle_declared_order(Operation.EXPIRE, "db.events", request) is request
    assert session.loaded == []


# -- what the summary says ---------------------------------------------------------------


def test_the_summary_says_none_still_honours_the_declared_order(tmp_path, capsys):
    from zamboni.cli import main

    path = tmp_path / "tc.json"
    path.write_text(
        '{"version": 2, "warehouse": "acme", "namespaces": {"db": {"tables": {"events": {}}}}}'
    )
    assert main(["table-config", "summary", str(path)]) == 0
    assert "the table's declared sort order, if any" in capsys.readouterr().out


def test_settling_keeps_every_other_compaction_setting():
    session = _Session(ID_DESC)
    maintainer = SparkMaintainer(session, {"catalog": "iceberg"})  # type: ignore[arg-type]
    compaction = CompactionConfig(
        sort_by_declared_order_if_any=True, min_input_files=7, partial_progress=True
    )
    request = MaintenanceRequest(retention=Retention(), compaction=compaction)
    settled = maintainer._settle_declared_order(Operation.COMPACT, "db.events", request)
    assert settled.compaction == replace(
        compaction, sort_by_table_order=True, sort_by_declared_order_if_any=False
    )
