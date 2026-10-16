# SPDX-License-Identifier: Apache-2.0
"""Multi-key Z-order (Morton order) as DuckDB SQL.

Neither Iceberg, PyIceberg, nor duckdb-iceberg has any notion of z-order --
there is no transform, no metadata field, and no function. It has to be built
from ordinary SQL, which is what this module does.

The construction, per rewrite group:

1. **Rank each key.** ``DENSE_RANK() OVER (ORDER BY col)`` maps every key column
   onto a dense integer regardless of its type -- strings, timestamps, decimals
   and numbers all become comparable small integers, and the mapping preserves
   order. Ranking beats hashing here because z-order needs *locality*: rows with
   nearby values must get nearby codes, which a hash deliberately destroys.

2. **Scale into a fixed bit width.** Ranks are squeezed into
   ``precision_bits`` by proportional scaling, so each column contributes the
   same number of bits no matter how many distinct values it has. Without this
   a high-cardinality column would dominate the interleave.

3. **Interleave.** Bit *i* of column *j* moves to position ``i * k + j`` of the
   output, the standard Morton encoding. Sorting on the result clusters rows
   that are close in *all* key dimensions at once, which is the point: a
   single-key sort only clusters the leading column.

The output key is a signed BIGINT, so ``precision_bits * len(columns)`` must fit
in 62 bits; :class:`~zamboni.tableconfig.ZOrder` enforces that at config-load
time rather than letting it silently overflow here.

The ranking windows mean a full pass over the group before ordering. DuckDB
spills both the window and the sort to ``temp_directory``, so this stays within
the memory envelope the rest of the package holds to.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Signed BIGINT, minus the sign bit.
MAX_ZORDER_BITS = 62


def quote_identifier(name: str) -> str:
    """Quote a DuckDB identifier, doubling any embedded quote.

    Every column name in the SQL this package builds goes through here. The
    names come from a table's schema and from table-config.json, and Iceberg
    allows any string as a column name: before ZMBNI-206, `"{name}"` let a
    column named `id", (SELECT 42) AS "x` add an expression to the rewrite's
    SELECT (reproduced with DuckDB 1.5.5) -- and DuckDB can read local files.
    The same rule as `maintainers.trino.quote`.
    """
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class ZOrderSQL:
    """The SQL needed to order a relation by a z-order key."""

    #: Key columns, in interleave order.
    columns: tuple[str, ...]
    precision_bits: int
    #: The column name holding the interleaved key.
    key_column: str = "_zamboni_zkey"

    def projection(self, relation: str) -> str:
        """The CTE body computing the interleaved key alongside every column of ``relation``.

        Built per call rather than stored as a template to `str.format`: the
        template would hold the column names, and one containing `{` or `}`
        would break the formatting, or be substituted into.
        """
        k = len(self.columns)
        max_code = (1 << self.precision_bits) - 1

        # DENSE_RANK is 1-based; shifting to 0-based keeps the whole code space
        # usable and makes a single-distinct-value column collapse to 0 rather than
        # to an arbitrary constant.
        rank_terms = [
            f"DENSE_RANK() OVER (ORDER BY {quote_identifier(col)}) - 1 AS _r{j}"
            for j, col in enumerate(self.columns)
        ]
        span_terms = [f"GREATEST(MAX(_r{j}) OVER (), 1) AS _span{j}" for j in range(k)]

        # Proportional scale into [0, max_code]. Integer division after multiplying
        # keeps this exact for the endpoints and monotonic in between.
        scale_terms = [f"CAST(_r{j} * {max_code} / _span{j} AS BIGINT) AS _c{j}" for j in range(k)]

        interleave = " + ".join(
            f"(((_c{j} >> {i}) & 1) << {i * k + j})"
            for i in range(self.precision_bits)
            for j in range(k)
        )

        return (
            "SELECT *, "
            f"({interleave}) AS {self.key_column} "
            "FROM (SELECT *, "
            + ", ".join(scale_terms)
            + " FROM (SELECT *, "
            + ", ".join(span_terms)
            + " FROM (SELECT *, "
            + ", ".join(rank_terms)
            + f" FROM {relation}) _z_ranked) _z_spanned) _z_scaled"
        )

    def select(self, relation: str, columns: list[str]) -> str:
        """A full SELECT that returns ``columns`` ordered by the z-order key."""
        projected = ", ".join(quote_identifier(c) for c in columns)
        return (
            f"WITH _zamboni_ranked AS ({self.projection(relation)}) "
            f"SELECT {projected} FROM _zamboni_ranked ORDER BY {self.key_column}"
        )


def build_zorder_sql(columns: list[str], precision_bits: int = 16) -> ZOrderSQL:
    """Build the z-order projection for ``columns``.

    Args:
        columns: Key columns, in the order their bits are interleaved. Order
            affects only tie-breaking at equal distance, not clustering quality.
        precision_bits: Bits of resolution kept per column.
    """
    if len(columns) < 2:
        raise ValueError("z-order needs at least two columns")
    if precision_bits < 1:
        raise ValueError("precision_bits must be >= 1")
    total = precision_bits * len(columns)
    if total > MAX_ZORDER_BITS:
        raise ValueError(
            f"{len(columns)} columns x {precision_bits} bits = {total} exceeds the "
            f"{MAX_ZORDER_BITS}-bit ordering key"
        )

    return ZOrderSQL(columns=tuple(columns), precision_bits=precision_bits)


def zorder_key(values: list[int], precision_bits: int) -> int:
    """Reference implementation of the interleave, for tests.

    Takes already-scaled per-column codes and returns the Morton code the SQL
    above should produce. Keeping an independent implementation means the SQL is
    checked against something other than itself.
    """
    k = len(values)
    out = 0
    for i in range(precision_bits):
        for j, value in enumerate(values):
            out += ((value >> i) & 1) << (i * k + j)
    return out
