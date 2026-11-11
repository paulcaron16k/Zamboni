# SPDX-License-Identifier: Apache-2.0
"""What the dashboard reads: the maintenance tier's rows, never the runtime.

docs/maintenance-runtime.md §9.2: state crosses tiers as rows. The web tier
reads the rows the maintenance tier's observer wrote (:mod:`lakehouse_app.state`)
and the claims its replicas hold, and works out from them whether maintenance is
there at all. Nothing here imports ``zamboni.runtime`` (§9.5 point 3).

**"Maintenance unavailable" is a stale tick, not a missing answer.** A running
runtime's scheduler ticks at least every `zamboni.scheduler.MAX_SLEEP` (60 s)
and its observer rewrites ``last_tick`` each time, so a runtime whose newest
tick is older than :data:`STALE_AFTER` has stopped ticking, whatever its row
says -- a SIGKILLed process writes no ``stopped``. That is what liveness
measures for ``zamboni serve``, read from a row instead of a file.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from zamboni.scheduler import MAX_SLEEP

from .db import Database

#: Three missed ticks: long enough that one slow observer write or a database
#: hiccup does not raise the banner, short enough that a dead tier is reported
#: within minutes.
STALE_AFTER = 3 * MAX_SLEEP


@dataclass(frozen=True)
class RuntimeRow:
    holder: str
    phase: str
    last_tick: datetime | None
    busy: int
    queued: int
    last_error: str | None
    updated_at: datetime


@dataclass(frozen=True)
class Availability:
    """Whether any maintenance replica is ticking, and since when it is not."""

    available: bool
    #: When maintenance was last seen ticking; ``None`` if it never was.
    since: datetime | None
    #: The replicas ticking now.
    live: tuple[RuntimeRow, ...]


@dataclass(frozen=True)
class RunRow:
    id: int
    holder: str
    warehouse: str
    table: str
    reason: str
    exit_code: int
    died: str | None
    dropped: str | None
    finished_at: datetime
    record: dict[str, Any] | None


@dataclass(frozen=True)
class ClaimRow:
    """A table a replica holds now: running, or about to."""

    warehouse: str
    table: str
    holder: str
    reason: str
    acquired_at: datetime


@dataclass(frozen=True)
class QuarantineRow:
    warehouse: str
    table: str
    failures: int
    until: datetime
    last_failure: str | None


class Dashboard:
    def __init__(self, database: Database, *, stale_after: timedelta = STALE_AFTER) -> None:
        self.database = database
        self.stale_after = stale_after

    def now(self) -> datetime:
        """The database's clock, which every row's timestamp is on."""
        with self.database.connect() as conn:
            row = conn.execute("SELECT now()").fetchone()
        assert row is not None
        return row[0]

    def runtimes(self) -> list[RuntimeRow]:
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT holder, phase, last_tick, busy, queued, last_error, updated_at"
                " FROM zamboni_runtime ORDER BY last_tick DESC NULLS LAST"
            ).fetchall()
        return [RuntimeRow(*row) for row in rows]

    def availability(self) -> Availability:
        now = self.now()
        rows = self.runtimes()
        live = tuple(
            r
            for r in rows
            if r.phase == "running"
            and r.last_tick is not None
            and now - r.last_tick <= self.stale_after
        )
        ticks = [r.last_tick for r in rows if r.last_tick is not None]
        return Availability(bool(live), max(ticks) if ticks else None, live)

    def running(self) -> list[ClaimRow]:
        """Tables a replica holds an unexpired claim on. A claim is taken before
        a table starts and released when it ends (§6), so this is what is
        running across every replica -- read from the claims, not from any
        one runtime's memory."""
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT warehouse, table_identifier, holder, reason, acquired_at"
                " FROM zamboni_claim WHERE expires_at > now() ORDER BY acquired_at"
            ).fetchall()
        return [ClaimRow(*row) for row in rows]

    def runs(
        self,
        *,
        warehouse: str | None = None,
        table: str | None = None,
        since: datetime | None = None,
        limit: int = 50,
    ) -> list[RunRow]:
        clauses: list[str] = []
        params: list[object] = []
        for column, value in (("warehouse", warehouse), ("table_identifier", table)):
            if value is not None:
                clauses.append(f"{column} = %s")
                params.append(value)
        if since is not None:
            clauses.append("finished_at >= %s")
            params.append(since)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT id, holder, warehouse, table_identifier, reason, exit_code, died,"
                f" dropped, finished_at, record FROM zamboni_run{where}"
                " ORDER BY id DESC LIMIT %s",
                (*params, limit),
            ).fetchall()
        return [RunRow(*row) for row in rows]

    def quarantined(self) -> list[QuarantineRow]:
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT warehouse, table_identifier, failures, until, last_failure"
                " FROM lakehouse_quarantine ORDER BY warehouse, table_identifier"
            ).fetchall()
        return [QuarantineRow(*row) for row in rows]
