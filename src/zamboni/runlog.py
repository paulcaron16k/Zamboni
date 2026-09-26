# SPDX-License-Identifier: Apache-2.0
"""Read a series of run summaries back, and say what the fleet is doing.

`zamboni maintenance --json PATH` appends one JSON object per run. This module
is the other end: it turns a directory of those into the two numbers the
feedback loop in docs/devops.md is built on.

Two questions, and they are not the same question:

* **One-off.** What share of scheduled work has no input? That is the gate on
  ZMBNI-106 -- whether event-driven triggering is worth building -- and it is
  answered once, from a few weeks of runs.
* **Standing.** Is maintenance keeping up? Failures, exit codes and how long a
  sweep takes, per warehouse, forever. This one has no end date, and it is why
  the loop outlives the gate.

**Tolerant by construction.** A run log is written by a cron line on a machine
nobody is watching: it will contain a half-written record from the night the box
was rebooted, and records from three different Zamboni versions. Refusing to
read the series because one line is bad would make the tool useless at exactly
the moment it is needed, so unreadable lines are *counted and reported* rather
than raised on. A number beside "3 unreadable record(s)" is a number an
operator can judge; a traceback is not.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .maintenance import RunCounters

#: What `--json` writes, and what this module reads. A file rather than a
#: database because the lowest deployment model is a crontab line, and anything
#: that needs a service to record telemetry does not get used by it.
SUFFIX = ".jsonl"


@dataclass(frozen=True)
class WarehouseSummary:
    """One warehouse's share of the series."""

    warehouse: str
    runs: int = 0
    counters: RunCounters = field(default_factory=RunCounters)
    failed_runs: int = 0
    worst_exit_code: int = 0

    def describe(self) -> str:
        rate = self.counters.skip_rate
        share = "  --" if rate is None else f"{rate:6.0%}"
        return (
            f"  {self.warehouse:<24} {self.runs:>4} run(s) {share} skipped "
            f"{self.failed_runs:>4} failed run(s)"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "warehouse": self.warehouse,
            "runs": self.runs,
            "counters": self.counters.as_dict(),
            "failed_runs": self.failed_runs,
            "worst_exit_code": self.worst_exit_code,
        }


@dataclass(frozen=True)
class FleetSummary:
    """Every run in the series, aggregated.

    `counters.considered`, `skipped`, `maintained` and `failed` are **sums**:
    units of work are per run and add up across nights. `counters.tables` is a
    **distinct count**, because the same table appears every night and summing
    it would report a four-table warehouse as twenty-eight -- counted per
    ``(warehouse, table)``, since an identifier is only unique within one.
    """

    runs: int = 0
    unreadable: int = 0
    counters: RunCounters = field(default_factory=RunCounters)
    failed_runs: int = 0
    worst_exit_code: int = 0
    first_run: str | None = None
    last_run: str | None = None
    longest_run_seconds: float | None = None
    versions: tuple[str, ...] = ()
    warehouses: tuple[WarehouseSummary, ...] = ()

    def describe(self) -> str:
        if not self.runs:
            unreadable = f", {self.unreadable} unreadable record(s)" if self.unreadable else ""
            return f"no runs found{unreadable}"

        window = f"{self.first_run} to {self.last_run}"
        lines = [
            f"{self.runs} run(s), {window}",
            f"  {self.counters.describe()}",
            f"  runs with failures       {self.failed_runs} (worst exit {self.worst_exit_code})",
        ]
        if self.longest_run_seconds is not None:
            lines.append(f"  longest run              {self.longest_run_seconds:.1f}s")
        if len(self.versions) > 1:
            # Which operations are even attempted depends on the installed
            # PyIceberg, so a series spanning builds can move for a reason that
            # is nothing to do with the workload. Say so rather than average
            # over it silently.
            lines.append(f"  built by                 {len(self.versions)} version(s):")
            lines.extend(f"      {v}" for v in self.versions)
        if self.unreadable:
            lines.append(f"  unreadable records       {self.unreadable}")
        if len(self.warehouses) > 1:
            lines.append("")
            lines.extend(w.describe() for w in self.warehouses)
        return "\n".join(lines)

    def as_dict(self) -> dict[str, Any]:
        return {
            "runs": self.runs,
            "unreadable": self.unreadable,
            "counters": self.counters.as_dict(),
            "failed_runs": self.failed_runs,
            "worst_exit_code": self.worst_exit_code,
            "first_run": self.first_run,
            "last_run": self.last_run,
            "longest_run_seconds": self.longest_run_seconds,
            "versions": list(self.versions),
            "warehouses": [w.as_dict() for w in self.warehouses],
        }


def run_logs(paths: Iterable[str | Path]) -> list[Path]:
    """Resolve what was named into the files to read.

    A directory expands to the ``.jsonl`` files directly inside it, sorted, so
    the documented weekly command is `zamboni runs /var/log/zamboni` and not a
    shell glob an operator has to get right. A named file is read whatever it
    is called.
    """
    found: list[Path] = []
    for entry in paths:
        path = Path(entry)
        if path.is_dir():
            found.extend(sorted(p for p in path.iterdir() if p.suffix == SUFFIX and p.is_file()))
        else:
            found.append(path)
    return found


def read_records(paths: Iterable[str | Path]) -> tuple[list[dict[str, Any]], int]:
    """Every run record in the named files, and how many lines were unreadable.

    Returns the count rather than logging it, because the caller is what decides
    whether three bad lines out of four hundred is worth a word or worth an
    alert.
    """
    records: list[dict[str, Any]] = []
    unreadable = 0
    for path in run_logs(paths):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            unreadable += 1
            continue
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except ValueError:
                # A half-written record from the night the box was rebooted.
                unreadable += 1
                continue
            if isinstance(record, dict) and "counters" in record:
                records.append(record)
            else:
                unreadable += 1
    return records, unreadable


def _counters(record: dict[str, Any]) -> RunCounters:
    raw = record.get("counters") or {}
    return RunCounters(
        tables=int(raw.get("tables") or 0),
        considered=int(raw.get("considered") or 0),
        skipped=int(raw.get("skipped") or 0),
        maintained=int(raw.get("maintained") or 0),
        failed=int(raw.get("failed") or 0),
    )


def _total(parts: Sequence[RunCounters], tables: int) -> RunCounters:
    return RunCounters(
        tables=tables,
        considered=sum(p.considered for p in parts),
        skipped=sum(p.skipped for p in parts),
        maintained=sum(p.maintained for p in parts),
        failed=sum(p.failed for p in parts),
    )


def summarise(records: Sequence[dict[str, Any]], unreadable: int = 0) -> FleetSummary:
    """Aggregate a series of run records. See :class:`FleetSummary`."""
    if not records:
        return FleetSummary(unreadable=unreadable)

    by_warehouse: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_warehouse.setdefault(str(record.get("warehouse") or "(unnamed)"), []).append(record)

    warehouses = []
    for name in sorted(by_warehouse):
        rows = by_warehouse[name]
        tables = {t for row in rows for t in (row.get("tables") or [])}
        warehouses.append(
            WarehouseSummary(
                warehouse=name,
                runs=len(rows),
                counters=_total([_counters(r) for r in rows], len(tables)),
                failed_runs=sum(1 for r in rows if r.get("exit_code")),
                worst_exit_code=max((int(r.get("exit_code") or 0) for r in rows), default=0),
            )
        )

    stamps = sorted(str(r["started_at"]) for r in records if r.get("started_at"))
    durations = [
        float(r["duration_seconds"]) for r in records if r.get("duration_seconds") is not None
    ]
    banners = {
        " ".join(f"{k} {v}" for k, v in sorted((r.get("versions") or {}).items()))
        for r in records
        if r.get("versions")
    }
    # Keyed on (warehouse, table), not the identifier alone. A table identifier
    # is only unique *within* a warehouse -- `db.events` exists in every tenant
    # of a multi-tenant deployment -- so a bare set collapses them and reports a
    # forty-table fleet as four. Found by running this over two warehouses that
    # both have a `db.events`.
    all_tables = {
        (str(r.get("warehouse") or "(unnamed)"), t)
        for r in records
        for t in (r.get("tables") or [])
    }

    return FleetSummary(
        runs=len(records),
        unreadable=unreadable,
        counters=_total([_counters(r) for r in records], len(all_tables)),
        failed_runs=sum(1 for r in records if r.get("exit_code")),
        worst_exit_code=max((int(r.get("exit_code") or 0) for r in records), default=0),
        first_run=stamps[0] if stamps else None,
        last_run=stamps[-1] if stamps else None,
        longest_run_seconds=max(durations) if durations else None,
        versions=tuple(sorted(banners)),
        warehouses=tuple(warehouses),
    )


def summarise_logs(paths: Iterable[str | Path]) -> FleetSummary:
    """The whole thing in one call: read the named logs and aggregate them."""
    records, unreadable = read_records(paths)
    return summarise(records, unreadable)


__all__ = [
    "FleetSummary",
    "WarehouseSummary",
    "read_records",
    "run_logs",
    "summarise",
    "summarise_logs",
]
