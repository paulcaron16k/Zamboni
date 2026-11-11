# SPDX-License-Identifier: Apache-2.0
"""The maintenance tier's `zamboni.claims.ClaimStore`, over the app's database.

docs/maintenance-runtime.md §6.1's statements, in psycopg's placeholder
spelling. tests/test_lakehouse_app.py holds them to the document, and the
dev-stack suite runs `zamboni.conformance.claim_store_conformance` against
this class, which is §9.5 point 1.

Time is the database's: every expiry is compared with ``now()`` in SQL, so
replicas whose clocks disagree still agree about who holds a table.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta

from zamboni.claims import Claim

from .db import Database

ACQUIRE = """\
INSERT INTO zamboni_claim AS c
       (warehouse, table_identifier, holder, token, reason, acquired_at, expires_at)
VALUES (%(warehouse)s, %(table)s, %(holder)s, nextval('zamboni_claim_token'), %(reason)s,
        now(), now() + %(lease)s)
ON CONFLICT (warehouse, table_identifier) DO UPDATE
   SET holder = EXCLUDED.holder, token = EXCLUDED.token, reason = EXCLUDED.reason,
       acquired_at = now(), expires_at = now() + %(lease)s
 WHERE c.expires_at <= now()
RETURNING token, expires_at;"""

RENEW = """\
UPDATE zamboni_claim SET expires_at = now() + %(lease)s
 WHERE warehouse = %(warehouse)s AND table_identifier = %(table)s
   AND holder = %(holder)s AND token = %(token)s AND expires_at > now()
RETURNING expires_at;"""

RELEASE = """\
DELETE FROM zamboni_claim
 WHERE warehouse = %(warehouse)s AND table_identifier = %(table)s
   AND holder = %(holder)s AND token = %(token)s;"""


class PostgresClaimStore:
    """A connection per call: safe from the runtime's worker threads, which
    is where the runtime calls a store from (`ClaimStore`'s contract)."""

    def __init__(self, database: Database) -> None:
        self._db = database

    def now(self) -> datetime:
        """The database's clock: what the conformance checks wait on."""
        with self._db.connect() as conn:
            row = conn.execute("SELECT now()").fetchone()
        assert row is not None
        return row[0]

    def acquire(
        self, warehouse: str, table: str, *, holder: str, lease: timedelta, reason: str
    ) -> Claim | None:
        params = {
            "warehouse": warehouse,
            "table": table,
            "holder": holder,
            "reason": reason,
            "lease": lease,
        }
        with self._db.connect() as conn:
            row = conn.execute(ACQUIRE, params).fetchone()
        return None if row is None else Claim(warehouse, table, holder, row[0], row[1])

    def renew(self, claim: Claim, *, lease: timedelta) -> Claim | None:
        with self._db.connect() as conn:
            row = conn.execute(RENEW, {**_key(claim), "lease": lease}).fetchone()
        return None if row is None else dataclasses.replace(claim, expires_at=row[0])

    def release(self, claim: Claim) -> None:
        with self._db.connect() as conn:
            conn.execute(RELEASE, _key(claim))


def _key(claim: Claim) -> dict[str, object]:
    return {
        "warehouse": claim.warehouse,
        "table": claim.table,
        "holder": claim.holder,
        "token": claim.token,
    }
