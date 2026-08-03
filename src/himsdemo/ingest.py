"""Ingest one simulated day.

Two things here are deliberate and worth reading before changing:

**Micro-batching.** Each day's rows are split into hourly batches and each
batch is a separate write. That is what a streaming replication job actually
does, and it is what produces the small-file condition the maintenance tool
exists to fix. Ingesting a day as one file would leave nothing to compact and
make the whole demo vacuous.

**Update handling differs by mode.** New rows always append. Rows that already
exist -- a discharge advancing through its state machine, or a cancelled one
restarting the next day -- are where copy-on-write and merge-on-read diverge,
which is the entire point of the `mode` switch.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.table import Table

from icemaint.testing import locate_rows, write_position_deletes

from .schema import SchemaDocument, TableDefinition, read_csv


@dataclass
class TableIngestResult:
    table: str
    batches: int = 0
    rows_appended: int = 0
    rows_updated: int = 0
    delete_files_written: int = 0


@dataclass
class DayIngestResult:
    day_no: int
    write_mode: str
    tables: list[TableIngestResult] = field(default_factory=list)

    def describe(self) -> str:
        lines = [f"ingested day {self.day_no} ({self.write_mode})"]
        for t in self.tables:
            detail = f"{t.rows_appended} new"
            if t.rows_updated:
                detail += f", {t.rows_updated} updated"
            if t.delete_files_written:
                detail += f", {t.delete_files_written} delete file(s)"
            lines.append(f"  {t.table:<22} {t.batches:>3} batch(es)  {detail}")
        return "\n".join(lines)


def ingest_day(
    tables: dict[str, Table],
    schema: SchemaDocument,
    day_dir: Path,
    day_no: int,
    write_mode: str,
) -> DayIngestResult:
    result = DayIngestResult(day_no=day_no, write_mode=write_mode)

    result.tables.append(
        _ingest_employees(tables["hims_employees"], schema.by_name("hims_employees"), day_dir)
    )
    # The key and the batching column come from table_schema.json rather than
    # being repeated here, so the declared primary key and replication key are
    # load-bearing instead of decorative and cannot drift from behaviour.
    for name, filename in (("hims_discharge", "discharges.csv"), ("hims_events", "events.csv")):
        result.tables.append(
            _ingest_keyed(
                tables[name],
                schema.by_name(name),
                day_dir / filename,
                write_mode=write_mode,
            )
        )
    return result


def _ingest_employees(tbl: Table, definition: TableDefinition, day_dir: Path) -> TableIngestResult:
    """Master data: a full snapshot replaces the previous one.

    The source has no incremental key, so there is nothing to batch on and
    nothing to merge -- the whole table is replaced.

    Note what this means for the demo: `overwrite` drops the previous day's
    file from the live set, so the table sits at exactly one data file forever
    and `maintenance` correctly reports it as having nothing to do. That is the
    point of including it -- not every table has a small-file problem, and a
    maintenance tool that claimed otherwise would be the one to distrust.
    """
    data = read_csv(day_dir / "employees.csv", definition)
    with warnings.catch_warnings():
        # On the first day the table is empty, so the overwrite's delete half
        # matches nothing. Expected, and not something the operator should see.
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        tbl.overwrite(data)
    tbl.refresh()
    return TableIngestResult(table=definition.name, batches=1, rows_appended=data.num_rows)


def _ingest_keyed(
    tbl: Table,
    definition: TableDefinition,
    csv_path: Path,
    *,
    write_mode: str,
) -> TableIngestResult:
    key = definition.single_primary_key()
    batch_column = definition.require_replication_key()
    data = read_csv(csv_path, definition)
    result = TableIngestResult(table=definition.name)

    # Read the live key set once and maintain it as we go. Re-scanning per
    # batch is quadratic in the file count -- by day 5 that would be ten scans
    # a day across fifty-odd files -- and a real replication job tracks what it
    # has already loaded anyway.
    live_keys = _live_keys(tbl, key)

    for raw_batch in _hourly_batches(data, batch_column):
        # A discharge can transition more than once inside one hour, so a batch
        # may carry several versions of the same key. Replication targets apply
        # the latest; upsert refuses ambiguous input outright.
        batch = _latest_per_key(raw_batch, key, batch_column)
        batch_keys = set(batch.column(key).to_pylist())
        existing = batch_keys & live_keys
        fresh = _filter_by_key(batch, key, existing, keep_matching=False)
        changed = _filter_by_key(batch, key, existing, keep_matching=True)

        if fresh.num_rows:
            tbl.append(fresh)
            tbl.refresh()
            live_keys |= batch_keys - existing
            result.rows_appended += fresh.num_rows

        if changed.num_rows:
            if write_mode == "mor":
                result.delete_files_written += _apply_merge_on_read(tbl, changed, key)
            else:
                # PyIceberg's upsert is copy-on-write: matched rows' files are
                # rewritten in full.
                with tbl.transaction() as txn:
                    txn.upsert(changed, join_cols=[key])
                tbl.refresh()
            result.rows_updated += changed.num_rows

        result.batches += 1

    return result


def _apply_merge_on_read(tbl: Table, changed: pa.Table, key: str) -> int:
    """Simulate what a merge-on-read engine would write.

    PyIceberg cannot do this: its `delete()` and `upsert()` are both
    copy-on-write. So the superseded rows get a position delete file and the
    new versions are appended, which is the shape Spark or Flink would leave
    behind. See :mod:`icemaint.testing`.
    """
    keys = set(changed.column(key).to_pylist())
    # Locate the superseded rows *before* appending, or the new versions would
    # be found and deleted alongside the old ones.
    deletes = locate_rows(tbl, key, keys)
    tbl.append(changed)
    tbl.refresh()
    return write_position_deletes(tbl, deletes, name_hint="demo")


def _hourly_batches(data: pa.Table, column: str) -> list[pa.Table]:
    """Split a day's rows into one batch per hour of activity.

    Hours with no rows produce no batch, so a quiet night does not create empty
    files.
    """
    if data.num_rows == 0:
        return []

    values = data.column(column).to_pylist()
    hours = sorted({v.replace(minute=0, second=0, microsecond=0) for v in values if v is not None})
    batches = []
    for hour in hours:
        mask = [
            v is not None and v.replace(minute=0, second=0, microsecond=0) == hour for v in values
        ]
        batch = data.filter(pa.array(mask))
        if batch.num_rows:
            batches.append(batch)
    return batches


def _latest_per_key(data: pa.Table, key: str, order_column: str) -> pa.Table:
    """Keep only the newest row per key, by ``order_column``."""
    keys = data.column(key).to_pylist()
    order = data.column(order_column).to_pylist()
    newest: dict[str, int] = {}
    for i, (k, when) in enumerate(zip(keys, order, strict=True)):
        if k not in newest or when >= order[newest[k]]:
            newest[k] = i
    return data.take(pa.array(sorted(newest.values()), type=pa.int64()))


def _live_keys(tbl: Table, key: str) -> set[str]:
    """Every key currently live in the table."""
    if tbl.current_snapshot() is None:
        return set()
    return set(tbl.scan(selected_fields=(key,)).to_arrow().column(key).to_pylist())


def _filter_by_key(data: pa.Table, key: str, keys: set[str], *, keep_matching: bool) -> pa.Table:
    mask = pc.is_in(data.column(key), value_set=pa.array(sorted(keys), type=pa.string()))
    return data.filter(mask if keep_matching else pc.invert(mask))
