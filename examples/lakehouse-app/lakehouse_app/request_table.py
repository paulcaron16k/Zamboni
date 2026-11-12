# SPDX-License-Identifier: Apache-2.0
"""§9.2's request table: the web tier writes a row, the maintenance tier reads it.

Both tiers import this module, so it must not import ``zamboni.runtime``;
``zamboni.requests``, which it does import, does not either.

The web tier calls :meth:`RequestTable.submit` ("run now") and reads
:meth:`RequestTable.outcome`. Each maintenance replica reads through a
:class:`PostgresRequestSource` from :meth:`RequestTable.source`: one request
per transaction, taken ``FOR UPDATE SKIP LOCKED`` so no two replicas hold the
same one, and marked with its outcome in that transaction -- §9.2 explains why
not a batch. ``TAKE`` and ``HANDLE`` are the document's statements, held to it
by tests/test_lakehouse_app.py; the dev-stack suite runs
`zamboni.conformance.request_source_conformance` against this table (§9.5
point 5).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import datetime

import psycopg

from zamboni.requests import Request
from zamboni.scheduler import MANUAL

from .db import Database

TAKE = """\
SELECT id, warehouse, tables, reason FROM zamboni_request
 WHERE handled_at IS NULL ORDER BY id
 FOR UPDATE SKIP LOCKED LIMIT 1;"""

HANDLE = """\
UPDATE zamboni_request SET handled_at = now(), outcome = %(outcome)s WHERE id = %(id)s;"""

#: How often an idle replica looks for a request. A person pressing "run now"
#: waits up to this long before the maintenance tier sees it.
POLL_SECONDS = 1.0


@dataclass(frozen=True)
class RequestRow:
    """One request as the dashboard shows it."""

    id: int
    warehouse: str
    tables: tuple[str, ...] | None
    reason: str
    requested_by: str
    requested_at: datetime
    handled_at: datetime | None
    outcome: str | None


class RequestTable:
    """The request table, from either tier."""

    def __init__(self, database: Database, *, poll_every: float = POLL_SECONDS) -> None:
        self.database = database
        self.poll_every = poll_every

    # -- the web tier ---------------------------------------------------------

    def submit(
        self,
        warehouse: str,
        tables: Sequence[str] | None = None,
        *,
        reason: str = MANUAL,
        requested_by: str = "lakehouse-app",
    ) -> str:
        """Ask for ``tables`` of ``warehouse`` -- all of them when ``None`` --
        to be maintained; the new request's id. The app's own INSERT: the
        document prints only what the maintenance tier runs."""
        with self.database.connect() as conn:
            row = conn.execute(
                "INSERT INTO zamboni_request (warehouse, tables, reason, requested_by)"
                " VALUES (%s, %s, %s, %s) RETURNING id",
                (warehouse, None if tables is None else list(tables), reason, requested_by),
            ).fetchone()
        assert row is not None
        return str(row[0])

    def outcome(self, id: str) -> str | None:
        """What became of request ``id``; ``None`` while no replica has handled it."""
        with self.database.connect() as conn:
            row = conn.execute(
                "SELECT outcome FROM zamboni_request WHERE id = %s", (int(id),)
            ).fetchone()
        if row is None:
            raise KeyError(id)
        return row[0]

    def recent(self, limit: int = 50) -> list[RequestRow]:
        """The newest requests first, for the dashboard."""
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT id, warehouse, tables, reason, requested_by, requested_at,"
                " handled_at, outcome FROM zamboni_request ORDER BY id DESC LIMIT %s",
                (limit,),
            ).fetchall()
        return [RequestRow(r[0], r[1], None if r[2] is None else tuple(r[2]), *r[3:]) for r in rows]

    # -- the maintenance tier -------------------------------------------------

    def source(self, name: str = "lakehouse requests") -> PostgresRequestSource:
        """A new reader: one per maintenance replica, on a connection of its own."""
        return PostgresRequestSource(self, name)


class PostgresRequestSource:
    """A `zamboni.requests.RequestSource` over the request table.

    One connection, and one open transaction while a request is in hand: the
    row lock is what keeps every other replica off it. A request given back
    -- nak'd, or abandoned by a runtime that stopped -- is a rollback, and the
    row is free for the next replica to take.
    """

    def __init__(self, table: RequestTable, name: str) -> None:
        self.name = name
        self._table = table
        self.connected = False

    async def __aiter__(self) -> AsyncIterator[_Received]:
        conn = await self._table.database.connect_async()
        self.connected = True
        try:
            while True:
                row = await (await conn.execute(TAKE)).fetchone()
                if row is None:
                    await conn.rollback()
                    await asyncio.sleep(self._table.poll_every)
                    continue
                id, warehouse, tables, reason = row
                received = _Received(
                    Request(str(id), warehouse, None if tables is None else tuple(tables), reason),
                    conn,
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
    def __init__(self, request: Request, conn: psycopg.AsyncConnection) -> None:
        self.request = request
        self._conn = conn
        self.settled = False

    async def ack(self, outcome: str) -> None:
        await self._conn.execute(HANDLE, {"id": int(self.request.id), "outcome": outcome})
        await self._conn.commit()
        self.settled = True

    async def nak(self) -> None:
        await self._conn.rollback()
        self.settled = True
