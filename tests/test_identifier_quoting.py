# SPDX-License-Identifier: Apache-2.0
"""Column names reach DuckDB SQL quoted, whatever they contain (ZMBNI-206).

Iceberg allows any string as a column name, and the DuckDB backend builds its
ordering SQL from the table's schema and from table-config.json. Before
ZMBNI-206 a name was wrapped in `"..."` without doubling an embedded quote, so a
column named like `HOSTILE` below added an expression to the rewrite's SELECT.
Each ordering path is exercised end to end: the output must have exactly the
table's columns and rows, in the requested order.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

from zamboni import CompactionConfig, TableCompactor
from zamboni.config import config_from_table_settings
from zamboni.tableconfig import Ordering, SortKey, TableSettings, ZOrder
from zamboni.zorder import build_zorder_sql, quote_identifier

#: Closes the quote, adds a column, reopens it. Unescaped, the rewrite's SELECT
#: gained a column named `injected` (DuckDB 1.5.5).
HOSTILE = 'key", (SELECT 42) AS "injected'
#: The z-order projection was once a template passed through `str.format`, with
#: the column names in it. A name like this one raised KeyError; one spelled
#: `{relation}` was silently replaced by the relation's name, so the rank
#: ordered by the whole row instead (both seen with the code before ZMBNI-206).
BRACED = "{key}"

NAMES = [HOSTILE, BRACED, 'plain"quote', "id"]


@pytest.mark.parametrize("name", NAMES)
def test_a_quoted_identifier_names_exactly_that_column(name):
    con = duckdb.connect()
    con.register("t", pa.table({name: [1], "other": [2]}))
    result = con.execute(f"SELECT {quote_identifier(name)} FROM t").to_arrow_table()
    assert result.column_names == [name]
    assert result.column(0).to_pylist() == [1]


def test_zorder_sql_takes_any_column_name():
    con = duckdb.connect()
    # Both keys rise together, so their Morton order is the order of either.
    con.register("t", pa.table({HOSTILE: [3, 1, 2], BRACED: ["c", "a", "b"]}))
    z = build_zorder_sql([HOSTILE, BRACED], precision_bits=2)
    result = con.execute(z.select("t", [HOSTILE, BRACED])).to_arrow_table()
    assert result.column_names == [HOSTILE, BRACED]
    assert result.column(0).to_pylist() == [1, 2, 3]


SCHEMA = Schema(
    NestedField(1, HOSTILE, IntegerType(), required=False),
    NestedField(2, BRACED, StringType(), required=False),
)


def make_table(session, *, sort_order: SortOrder | None = None):
    kwargs = {"sort_order": sort_order} if sort_order is not None else {}
    tbl = session.catalog.create_table(
        "db.hostile", schema=SCHEMA, properties={"format-version": "2"}, **kwargs
    )
    arrow_schema = pa.schema(
        [pa.field(HOSTILE, pa.int32(), nullable=True), pa.field(BRACED, pa.string(), nullable=True)]
    )
    for start in (20, 0, 10):
        keys = list(range(start + 9, start - 1, -1))  # each file descending
        tbl.append(
            pa.table({HOSTILE: keys, BRACED: [f"v{k:02d}" for k in keys]}, schema=arrow_schema)
        )
    return tbl


def table_sort_order() -> SortOrder:
    return SortOrder(
        SortField(
            source_id=1,
            transform=IdentityTransform(),
            direction=SortDirection.ASC,
            null_order=NullOrder.NULLS_LAST,
        )
    )


def table_config_sort() -> CompactionConfig:
    settings = TableSettings(ordering=Ordering(mode="sort", sort=(SortKey(column=HOSTILE),)))
    return config_from_table_settings(settings)


def table_config_zorder() -> CompactionConfig:
    settings = TableSettings(
        ordering=Ordering(mode="zorder", zorder=ZOrder(columns=(HOSTILE, BRACED), precision_bits=8))
    )
    return config_from_table_settings(settings)


ORDERINGS = {
    "table sort order": (lambda: CompactionConfig(sort_by_table_order=True), True),
    "table-config sort": (table_config_sort, False),
    "table-config z-order": (table_config_zorder, False),
}


@pytest.mark.parametrize("ordering", list(ORDERINGS))
def test_compaction_orders_by_a_column_whose_name_would_inject(session, ordering):
    make_config, declares_order = ORDERINGS[ordering]
    tbl = make_table(session, sort_order=table_sort_order() if declares_order else None)
    before = tbl.scan().to_arrow()

    TableCompactor(session, "db.hostile", make_config()).execute()

    tbl = session.catalog.load_table("db.hostile")
    after = tbl.scan().to_arrow()
    assert after.column_names == [HOSTILE, BRACED]
    assert sorted(after.to_pylist(), key=str) == sorted(before.to_pylist(), key=str)
    assert len(list(tbl.scan().plan_files())) == 1, "the three files were not rewritten as one"
    keys = after.column(HOSTILE).to_pylist()
    if ordering == "table-config z-order":
        # Both columns rise together, so their Morton order is the key order.
        assert keys == sorted(keys)
    else:
        assert keys == list(range(30))
