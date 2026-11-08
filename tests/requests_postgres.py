# SPDX-License-Identifier: Apache-2.0
"""§9.2's PostgreSQL reference: a `RequestSource`, and an observer writing state rows.

Test-only, like `tests/claims_postgres.py`.

**It runs the SQL the design document prints, read from the document**, as
`tests/claims_postgres.py` does for §6.1: an edit there is an edit to what is
tested. A Lakehouse App writes its own against its own database and driver;
this one proves the schema and the statements pass `zamboni.conformance`, and
that two replicas reading one table offer each request once.

Each instance works in a schema of its own, created and dropped by the caller,
so a run never touches the database's other tables -- in the dev stack,
Lakekeeper's.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
from typing import Any

import psycopg

from zamboni.pool import ItemResult
from zamboni.requests import Request
from zamboni.runstate import RuntimeState
from zamboni.runtime import MaintenanceRuntime
from zamboni.scheduler import MANUAL

DESIGN = Path(__file__).resolve().parent.parent / "docs" / "maintenance-runtime.md"


def reference_sql(heading: str) -> dict[str, str]:
    """The SQL block under ``heading``, by its ``-- name:`` comments; what
    precedes the first is ``schema``. ``:name`` placeholders become psycopg's
    ``%(name)s``."""
    section = DESIGN.read_text().split(heading, 1)[1]
    block = section.split("```sql\n", 1)[1].split("```", 1)[0]
    parts = re.split(r"^-- (\w+):.*$", block, flags=re.M)
    statements = {"schema": parts[0], **dict(zip(parts[1::2], parts[2::2], strict=True))}
    return {
        name: re.sub(r"(?<!:):(\w+)", r"%(\1)s", sql.strip()) for name, sql in statements.items()
    }


REQUESTS = "Reference schema for the request table (PostgreSQL)"
STATE = "Reference schema for the state rows (PostgreSQL)"


class PostgresRequests:
    """§9.2's request table: what a web tier writes, and what each replica's
    :meth:`source` reads. A connection per call from the web tier's side."""

    def __init__(self, conninfo: str, schema: str, *, poll_every: float = 0.1) -> None:
        self._conninfo = conninfo
        self._options = f"-c search_path={schema}"
        self._poll_every = poll_every
        self.sql = reference_sql(REQUESTS)

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._conninfo, options=self._options, autocommit=True)

    def create_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(self.sql["schema"])

    def submit(
        self,
        warehouse: str,
        tables: Sequence[str] | None = None,
        *,
        reason: str = MANUAL,
        requested_by: str = "test",
    ) -> str:
        """The web tier's INSERT: not one of the document's statements, since
        it is the application's own."""
        with self._connect() as conn:
            row = conn.execute(
                "INSERT INTO zamboni_request (warehouse, tables, reason, requested_by)"
                " VALUES (%s, %s, %s, %s) RETURNING id",
                (warehouse, None if tables is None else list(tables), reason, requested_by),
            ).fetchone()
        assert row is not None
        return str(row[0])

    def outcome(self, id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT outcome FROM zamboni_request WHERE id = %s", (int(id),)
            ).fetchone()
        assert row is not None
        return row[0]

    def source(self, name: str = "postgres requests") -> PostgresRequestSource:
        return PostgresRequestSource(self, name)


class PostgresRequestSource:
    """One replica's reader: a connection of its own, and a transaction per
    request -- taken with ``take``, settled with ``handle`` and a commit, or
    given back with a rollback."""

    def __init__(self, requests: PostgresRequests, name: str) -> None:
        self.name = name
        self._requests = requests
        self.connected = False

    async def __aiter__(self) -> AsyncIterator[_Received]:
        r = self._requests
        conn = await psycopg.AsyncConnection.connect(r._conninfo, options=r._options)
        self.connected = True
        try:
            while True:
                row = await (await conn.execute(r.sql["take"])).fetchone()
                if row is None:
                    await conn.rollback()
                    await asyncio.sleep(r._poll_every)
                    continue
                id, warehouse, tables, reason = row
                received = _Received(
                    Request(str(id), warehouse, None if tables is None else tuple(tables), reason),
                    conn,
                    r.sql["handle"],
                )
                try:
                    yield received
                finally:
                    if not received.settled:
                        # Asked for the next, or closed, with this one in hand:
                        # given back, as a replica that died gives it back.
                        await conn.rollback()
        finally:
            self.connected = False
            await conn.close()


class _Received:
    def __init__(self, request: Request, conn: psycopg.AsyncConnection, handle: str) -> None:
        self.request = request
        self._conn, self._handle = conn, handle
        self.settled = False

    async def ack(self, outcome: str) -> None:
        await self._conn.execute(self._handle, {"id": int(self.request.id), "outcome": outcome})
        await self._conn.commit()
        self.settled = True

    async def nak(self) -> None:
        await self._conn.rollback()
        self.settled = True


class PostgresStateRows:
    """A reference observer (§8) writing what a dashboard reads: one row per
    runtime, rewritten on every change, and a row per finished table. Slow, as
    any database write is: hand it to the runtime through
    `zamboni.observe.handed_off`."""

    def __init__(self, conninfo: str, schema: str) -> None:
        self._conninfo = conninfo
        self._options = f"-c search_path={schema}"
        self._holder = ""
        self._state: Callable[[], RuntimeState] | None = None
        self.sql = reference_sql(STATE)

    def attach(self, runtime: MaintenanceRuntime) -> None:
        """Whose rows these are. The runtime takes its observers when it is
        built, and has a ``holder`` only once it is, so this comes after."""
        self._holder, self._state = runtime.holder, runtime.state

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._conninfo, options=self._options, autocommit=True)

    def create_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(self.sql["schema"])

    def _changed(self, *_: Any) -> None:
        assert self._state is not None, "attach() the runtime first"
        state = self._state()
        with self._connect() as conn:
            conn.execute(
                self.sql["state"],
                {
                    "holder": self._holder,
                    "phase": state.phase,
                    "last_tick": state.last_tick,
                    "busy": state.busy,
                    "queued": len(state.queued),
                    "last_error": state.last_error,
                },
            )

    phase_changed = ticked = fired = started = fleet_changed = refused = _changed

    def finished(self, result: ItemResult) -> None:
        with self._connect() as conn:
            conn.execute(
                self.sql["run"],
                {
                    "holder": self._holder,
                    "warehouse": result.item.warehouse,
                    "table": result.item.table,
                    "reason": result.item.reason,
                    "exit_code": result.exit_code,
                    "died": result.died,
                    "dropped": result.dropped,
                    "record": None if result.record is None else json.dumps(result.record),
                },
            )
        self._changed()
