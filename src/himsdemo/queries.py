# SPDX-License-Identifier: Apache-2.0
"""The `query` command: what a developer would actually ask this data.

Read path is PyIceberg scan -> Arrow -> DuckDB. DuckDB cannot ATTACH a SQLite
Iceberg catalog (only REST), and this is the same pattern `zamboni` itself
uses for its rewrite reads.

**The Iceberg read is inside the timed region.** That matters: the read is the
part compaction actually changes, so timing only the DuckDB execution would
report a number that has almost nothing to do with the file count printed
beside it. Files scanned is exact and is the headline; the millisecond figure is
labelled indicative because at 625 rows it is still dominated by fixed overhead.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import pyarrow as pa
from pyiceberg.table import Table

TIMING_RUNS = 3


@dataclass(frozen=True)
class Query:
    title: str
    note: str
    #: Tables the SQL reads. Only these are re-read per timed run, so the cost
    #: measured is the cost this query actually pays.
    reads: tuple[str, ...]
    sql: str


@dataclass
class QueryResult:
    query: Query
    data_files: int
    delete_files: int
    median_ms: float
    rows: pa.Table = field(repr=False)

    @property
    def files_scanned(self) -> int:
        return self.data_files + self.delete_files

    def render(self, limit: int = 10) -> str:
        lines = [
            f"  {self.query.title}",
            f"      {self.query.note}",
            (
                f"      files scanned {self.files_scanned}"
                + (
                    f" ({self.data_files} data + {self.delete_files} delete)"
                    if self.delete_files
                    else ""
                )
                + f"    {self.median_ms:.1f} ms (indicative, n={TIMING_RUNS}, includes the read)"
            ),
            "",
        ]
        lines += _table_lines(self.rows, limit)
        lines.append("")
        return "\n".join(lines)


QUERIES = (
    Query(
        title="Discharges by replication key",
        note="filters updated_at to the last day of changes -- the incremental-read path",
        reads=("hims_discharge", "hims_employees"),
        sql="""
            SELECT d.process_id, d.status, d.patient_id, e.full_name AS attending_doctor,
                   d.updated_at
            FROM hims_discharge d
            LEFT JOIN hims_employees e ON e.employee_id = d.attending_doctor_id
            WHERE d.updated_at >= (SELECT MAX(updated_at) FROM hims_discharge) - INTERVAL 1 DAY
            ORDER BY d.updated_at DESC
        """,
    ),
    Query(
        title="Cancelled and restarted discharges",
        note="same process_id updated on a later day -- the rows that make CoW and MoR differ",
        reads=("hims_discharge", "hims_events"),
        sql="""
            WITH cancelled AS (
                SELECT DISTINCT process_id FROM hims_events
                WHERE event_name = 'billing_clearance_failed'
            )
            SELECT d.process_id, d.status,
                   CAST(d.created_at AS DATE) AS created_day,
                   CAST(d.updated_at AS DATE) AS updated_day,
                   CAST(d.completed_at AS DATE) AS completed_day
            FROM hims_discharge d JOIN cancelled c USING (process_id)
            ORDER BY d.created_at
        """,
    ),
    Query(
        title="Event trace for one discharge",
        note="filters process_id -- the lookup path the z-order exists for",
        reads=("hims_events", "hims_employees"),
        sql="""
            WITH pick AS (
                SELECT process_id FROM hims_events
                WHERE event_name = 'billing_clearance_failed'
                ORDER BY process_id LIMIT 1
            )
            SELECT ev.occurred_at, ev.event_name,
                   COALESCE(e.role, '(system)') AS actor_role
            FROM hims_events ev
            JOIN pick USING (process_id)
            LEFT JOIN hims_employees e ON e.employee_id = ev.actor_id
            ORDER BY ev.occurred_at
        """,
    ),
    Query(
        title="Daily metric: EVS room turnaround",
        note="joins events to themselves by process_id and groups by day",
        reads=("hims_events",),
        sql="""
            WITH out AS (
                SELECT process_id, occurred_at AS left_at FROM hims_events
                WHERE event_name = 'patient_displaced_discharged'
            ), clean AS (
                SELECT process_id, occurred_at AS ready_at FROM hims_events
                WHERE event_name = 'bed_cleaned'
            )
            SELECT CAST(o.left_at AS DATE) AS day,
                   COUNT(*) AS rooms,
                   ROUND(AVG(date_diff('minute', o.left_at, c.ready_at)), 1)
                       AS avg_turnaround_min,
                   MAX(date_diff('minute', o.left_at, c.ready_at)) AS worst_min
            FROM out o JOIN clean c USING (process_id)
            GROUP BY 1 ORDER BY 1
        """,
    ),
)


def run_all(con, tables: dict[str, Table]) -> list[QueryResult]:
    return [run(con, query, tables) for query in QUERIES]


def run(con, query: Query, tables: dict[str, Table]) -> QueryResult:
    missing = [name for name in query.reads if name not in tables]
    if missing:
        raise KeyError(
            f"{query.title!r} reads {missing}, which do not exist yet -- "
            "run './bin/demo next-day' first"
        )
    used = {name: tables[name] for name in query.reads}
    data_files, delete_files = _files_read(used.values())

    timings: list[float] = []
    rows: pa.Table | None = None
    for _ in range(TIMING_RUNS):
        start = time.perf_counter()
        for name, tbl in used.items():
            con.register(name, tbl.scan().to_arrow())
        rows = con.execute(query.sql).to_arrow_table()
        timings.append((time.perf_counter() - start) * 1000)

    timings.sort()
    return QueryResult(
        query=query,
        data_files=data_files,
        delete_files=delete_files,
        median_ms=timings[len(timings) // 2],
        rows=rows,
    )


def _files_read(tables) -> tuple[int, int]:
    """Files the read actually opens: data files *and* their delete files.

    Counting only data files understates merge-on-read by roughly half -- in
    the one mode the demo exists to differentiate -- so the headline number
    would misreport exactly the case it is there to show.
    """
    data = 0
    deletes: set[str] = set()
    for tbl in tables:
        for task in tbl.scan().plan_files():
            data += 1
            deletes.update(d.file_path for d in task.delete_files)
    return data, len(deletes)


def _table_lines(table: pa.Table, limit: int) -> list[str]:
    if table.num_rows == 0:
        return ["      (no rows)"]
    names = table.column_names
    rows = [
        [_fmt(v) for v in row] for row in zip(*[c.to_pylist() for c in table.columns], strict=True)
    ]
    shown = rows[:limit]
    widths = [
        max(len(n), *(len(r[i]) for r in shown)) if shown else len(n) for i, n in enumerate(names)
    ]
    out = ["      " + "  ".join(n.ljust(w) for n, w in zip(names, widths, strict=True))]
    out.append("      " + "  ".join("─" * w for w in widths))
    out += ["      " + "  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)) for r in shown]
    if len(rows) > limit:
        out.append(f"      … {len(rows) - limit} more row(s)")
    return out


def _fmt(value) -> str:
    if value is None:
        return "-"
    text = str(value)
    return text if len(text) <= 34 else text[:31] + "…"
