# SPDX-License-Identifier: Apache-2.0
"""The PostgreSQL reference `ClaimStore` (docs/maintenance-runtime.md §6.1). Test-only.

**It runs the SQL the design document prints, read from the document.** §6.1
says its statements were checked against PostgreSQL "statement for statement as
above"; reading them from there, rather than keeping a copy here, is what keeps
that sentence true after the document is edited. A deployment writes its own
store against its own database and driver; this one exists to prove the schema
and the statements pass `zamboni.conformance`.

Each store works in a schema of its own, created and dropped by the caller, so
a run never touches the database's other tables -- in the dev stack, Lakekeeper's.
"""

from __future__ import annotations

import dataclasses
import re
from datetime import datetime, timedelta
from pathlib import Path

import psycopg

from zamboni.claims import Claim

DESIGN = Path(__file__).resolve().parent.parent / "docs" / "maintenance-runtime.md"


def reference_sql() -> dict[str, str]:
    """§6.1's SQL block, by its comments: ``schema``, ``acquire``, ``renew``, ``release``.

    The document's ``:name`` placeholders become psycopg's ``%(name)s``.
    """
    section = DESIGN.read_text().split("### 6.1 Reference schema (PostgreSQL)", 1)[1]
    block = section.split("```sql\n", 1)[1].split("```", 1)[0]
    parts = re.split(r"^-- (\w+)\b.*$", block, flags=re.M)
    statements = {"schema": parts[0], **dict(zip(parts[1::2], parts[2::2], strict=True))}
    return {
        name: re.sub(r"(?<!:):(\w+)", r"%(\1)s", sql.strip()) for name, sql in statements.items()
    }


class PostgresClaimStore:
    """A connection per call: safe from any thread, and nothing held between calls."""

    def __init__(self, conninfo: str, schema: str) -> None:
        self._conninfo = conninfo
        self._options = f"-c search_path={schema}"
        self._sql = reference_sql()

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._conninfo, options=self._options, autocommit=True)

    def create_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(self._sql["schema"])

    def now(self) -> datetime:
        with self._connect() as conn:
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
        with self._connect() as conn:
            row = conn.execute(self._sql["acquire"], params).fetchone()
        return None if row is None else Claim(warehouse, table, holder, row[0], row[1])

    def renew(self, claim: Claim, *, lease: timedelta) -> Claim | None:
        with self._connect() as conn:
            row = conn.execute(self._sql["renew"], {**self._key(claim), "lease": lease}).fetchone()
        return None if row is None else dataclasses.replace(claim, expires_at=row[0])

    def release(self, claim: Claim) -> None:
        with self._connect() as conn:
            conn.execute(self._sql["release"], self._key(claim))

    @staticmethod
    def _key(claim: Claim) -> dict[str, object]:
        return {
            "warehouse": claim.warehouse,
            "table": claim.table,
            "holder": claim.holder,
            "token": claim.token,
        }
