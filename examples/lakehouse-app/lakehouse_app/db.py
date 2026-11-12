# SPDX-License-Identifier: Apache-2.0
"""The app's database: where it is, and the one migration both tiers apply.

Shared by both tiers, so it imports nothing from ``zamboni.runtime`` -- the web
tier imports this module too (docs/maintenance-runtime.md §9.2, §9.5 point 3).

Two settings, from the environment:

- ``LAKEHOUSE_DATABASE_URL``: a libpq connection string or URI. It carries the
  database password, so it is a secret: it comes from the environment (a
  Kubernetes Secret, a compose ``.env``), never from the registry.
- ``LAKEHOUSE_DATABASE_SCHEMA``: the PostgreSQL schema the app's tables live in,
  ``lakehouse`` unless set. A schema of its own keeps the app's tables apart
  from anything else in the database -- in the dev stack, Lakekeeper's -- and
  lets the tests give each run a fresh one.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib.resources import files

import psycopg

URL = "LAKEHOUSE_DATABASE_URL"
SCHEMA = "LAKEHOUSE_DATABASE_SCHEMA"
DEFAULT_SCHEMA = "lakehouse"

#: A schema name the app will put in a ``search_path`` and a ``CREATE SCHEMA``:
#: an unquoted SQL identifier, so neither needs quoting and none can inject.
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}")


class DatabaseNotConfigured(RuntimeError):
    """``LAKEHOUSE_DATABASE_URL`` is not set."""


@dataclass(frozen=True)
class Database:
    """Where the app's tables are. Cheap to build and to pass to a thread.

    Every call opens its own connection: safe from any thread, nothing held
    between calls, and a database restart costs one failed call rather than a
    broken pool. A web tier with real traffic would pool; this one has a
    handful of users.
    """

    url: str = field(repr=False)  # holds the password
    schema: str = DEFAULT_SCHEMA

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.schema):
            raise ValueError(f"{SCHEMA} must be a lower-case SQL identifier, not {self.schema!r}")

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None) -> Database:
        environ = os.environ if environ is None else environ
        url = environ.get(URL)
        if not url:
            raise DatabaseNotConfigured(f"{URL} is not set: where is the app's database?")
        return cls(url, environ.get(SCHEMA) or DEFAULT_SCHEMA)

    @property
    def _options(self) -> str:
        return f"-c search_path={self.schema}"

    def connect(self, *, autocommit: bool = True) -> psycopg.Connection:
        return psycopg.connect(self.url, options=self._options, autocommit=autocommit)

    async def connect_async(self) -> psycopg.AsyncConnection:
        """A connection in a transaction per statement group: the caller commits."""
        return await psycopg.AsyncConnection.connect(self.url, options=self._options)

    def migrate(self) -> None:
        """Create the schema and every table in it, if missing. Idempotent."""
        with self.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            conn.execute(schema_sql())


def schema_sql() -> str:
    """lakehouse_app/schema.sql, as shipped beside this module."""
    return files(__package__).joinpath("schema.sql").read_text(encoding="utf-8")
