"""Multi-key Z-order.

Nothing in the stack provides z-order, so the SQL is checked against an
independent reference implementation of the Morton encoding rather than against
itself.
"""

from __future__ import annotations

import itertools

import duckdb
import pytest

from icemaint.zorder import build_zorder_sql, zorder_key


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def grid(con, side: int, cols=("x", "y")):
    values = ",".join(
        "(" + ",".join(str(v) for v in point) + ")"
        for point in itertools.product(range(side), repeat=len(cols))
    )
    names = ",".join(cols)
    con.execute(f"CREATE OR REPLACE TABLE g AS SELECT * FROM (VALUES {values}) AS v({names})")


def test_matches_the_reference_morton_encoding(con):
    grid(con, 4)
    z = build_zorder_sql(["x", "y"], precision_bits=2)
    rows = con.execute(z.select("g", ["x", "y"])).fetchall()

    expected = [
        (x, y)
        for _, x, y in sorted(
            (zorder_key([x, y], 2), x, y) for x, y in itertools.product(range(4), repeat=2)
        )
    ]
    assert rows == expected


def test_three_keys_interleave(con):
    grid(con, 2, cols=("x", "y", "z"))
    zs = build_zorder_sql(["x", "y", "z"], precision_bits=1)
    rows = con.execute(zs.select("g", ["x", "y", "z"])).fetchall()

    expected = [
        (x, y, z)
        for _, x, y, z in sorted(
            (zorder_key([x, y, z], 1), x, y, z) for x, y, z in itertools.product(range(2), repeat=3)
        )
    ]
    assert rows == expected


def test_clusters_better_than_a_single_key_sort(con):
    """The reason z-order exists: locality in *every* key at once.

    A lexicographic sort on (x, y) groups x tightly and scatters y, so a query
    filtering only on y has to read every file. Z-order trades a little x
    locality for y locality and prunes both.

    Measured directly as that query cost: how many blocks a single-value filter
    on the *second* key touches, averaged over all its values. Bounding-box area
    is the wrong measure -- for a fixed block size it is identical under both
    layouts.
    """
    side = 8
    block = 8
    grid(con, side)
    z = build_zorder_sql(["x", "y"], precision_bits=3)

    zorder_rows = con.execute(z.select("g", ["x", "y"])).fetchall()
    sorted_rows = con.execute("SELECT x, y FROM g ORDER BY x, y").fetchall()

    def blocks_touched_filtering_y(rows):
        blocks = [rows[i : i + block] for i in range(0, len(rows), block)]
        # A reader prunes on min/max stats, so a block is read whenever the
        # value falls inside its y range.
        ranges = [(min(r[1] for r in b), max(r[1] for r in b)) for b in blocks]
        touched = [sum(lo <= y <= hi for lo, hi in ranges) for y in range(side)]
        return sum(touched) / len(touched)

    z_cost = blocks_touched_filtering_y(zorder_rows)
    lex_cost = blocks_touched_filtering_y(sorted_rows)

    assert lex_cost == len(sorted_rows) / block, "lex sort should force a full scan on y"
    assert z_cost < lex_cost, f"z-order touched {z_cost} blocks vs lex {lex_cost}"


def test_preserves_every_row(con):
    grid(con, 5)
    z = build_zorder_sql(["x", "y"], precision_bits=4)
    rows = con.execute(z.select("g", ["x", "y"])).fetchall()
    assert sorted(rows) == sorted(itertools.product(range(5), repeat=2))


def test_handles_non_numeric_keys(con):
    """Ranking makes the encoding type-agnostic: strings and timestamps work."""
    con.execute(
        "CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES "
        "('b', DATE '2026-01-02'), ('a', DATE '2026-01-01'), "
        "('a', DATE '2026-01-02'), ('b', DATE '2026-01-01')) AS v(name, d)"
    )
    z = build_zorder_sql(["name", "d"], precision_bits=2)
    rows = con.execute(z.select("t", ["name", "d"])).fetchall()
    assert len(rows) == 4
    assert rows[0][0] == "a"  # lowest rank in both dimensions sorts first


def test_single_valued_column_does_not_break_the_scale(con):
    """A column with one distinct value has zero span; the SQL must not divide by zero."""
    con.execute(
        "CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES (1, 9), (2, 9), (3, 9)) AS v(x, y)"
    )
    z = build_zorder_sql(["x", "y"], precision_bits=4)
    rows = con.execute(z.select("t", ["x", "y"])).fetchall()
    assert [r[0] for r in rows] == [1, 2, 3]


def test_rejects_an_over_budget_key():
    with pytest.raises(ValueError, match="exceeds the 62-bit"):
        build_zorder_sql(["a", "b", "c", "d"], precision_bits=32)


def test_rejects_a_single_column():
    with pytest.raises(ValueError, match="at least two columns"):
        build_zorder_sql(["a"])
