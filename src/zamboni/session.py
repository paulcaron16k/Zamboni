"""Catalog + compute-engine wiring.

A :class:`CatalogSession` owns both halves of a maintenance run so they cannot
drift apart: the PyIceberg catalog that reads metadata and commits snapshots,
and the DuckDB connection used to sort and to spill large rewrite groups.

DuckDB is deliberately *not* asked to attach the Iceberg catalog here. Data is
read through PyIceberg's scan (see :mod:`zamboni.backends.duckdb_arrow`), which
works identically for a Lakekeeper REST catalog and for the local SQL catalog
used by the tests, and hands DuckDB a plain Arrow stream. The DuckDB Iceberg
extension is only needed for the equality-delete story, which phase 1 blocks on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Self

import duckdb
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

logger = logging.getLogger(__name__)


@dataclass
class CatalogSession:
    """A catalog plus the DuckDB connection used to move data for it."""

    catalog: Catalog
    con: duckdb.DuckDBPyConnection
    #: Threads DuckDB may use. Kept low by default because a maintenance job
    #: usually runs beside something more important.
    threads: int = 4

    def table(self, identifier: str) -> Table:
        """Load a table by ``namespace.name`` identifier."""
        return self.catalog.load_table(identifier)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("closing DuckDB connection failed: %s", exc)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- constructors ----------------------------------------------------

    @classmethod
    def for_lakekeeper(
        cls,
        *,
        uri: str,
        warehouse: str,
        credential: str | None = None,
        token: str | None = None,
        oauth2_server_uri: str | None = None,
        scope: str | None = None,
        s3: S3Settings | None = None,
        threads: int = 4,
        extra: dict[str, Any] | None = None,
    ) -> CatalogSession:
        """Build a session against a Lakekeeper REST catalog backed by MinIO.

        Args:
            uri: Lakekeeper catalog endpoint, e.g. ``http://localhost:8181/catalog``.
            warehouse: Warehouse name configured in Lakekeeper.
            credential: ``client_id:client_secret`` for OAuth2 client credentials.
            token: A bearer token, as an alternative to ``credential``.
            oauth2_server_uri: Token endpoint, when not discoverable from ``uri``.
            scope: OAuth2 scope, e.g. ``lakekeeper``.
            s3: Direct MinIO credentials. Omit when Lakekeeper vends credentials.
        """
        props: dict[str, Any] = {"type": "rest", "uri": uri, "warehouse": warehouse}
        if credential:
            props["credential"] = credential
        if token:
            props["token"] = token
        if oauth2_server_uri:
            props["oauth2-server-uri"] = oauth2_server_uri
        if scope:
            props["scope"] = scope
        if s3:
            props.update(s3.as_properties())
        if extra:
            props.update(extra)

        catalog = load_catalog("lakekeeper", **props)
        return cls(catalog=catalog, con=_new_duckdb(threads), threads=threads)

    @classmethod
    def for_local(
        cls,
        *,
        warehouse_path: str,
        uri: str | None = None,
        name: str = "local",
        threads: int = 4,
    ) -> CatalogSession:
        """Build a session against a local SQL catalog on a filesystem warehouse.

        Used by the test suite so the whole package is exercisable without
        Docker, MinIO, or Lakekeeper.
        """
        catalog = load_catalog(
            name,
            type="sql",
            uri=uri or f"sqlite:///{warehouse_path.rstrip('/')}/catalog.db",
            warehouse=f"file://{warehouse_path.rstrip('/')}",
        )
        return cls(catalog=catalog, con=_new_duckdb(threads), threads=threads)

    @classmethod
    def from_catalog(cls, catalog: Catalog, *, threads: int = 4) -> CatalogSession:
        """Wrap an already-configured PyIceberg catalog."""
        return cls(catalog=catalog, con=_new_duckdb(threads), threads=threads)


@dataclass(frozen=True)
class S3Settings:
    """MinIO / S3 connection details for PyIceberg's FileIO."""

    endpoint: str
    access_key_id: str
    secret_access_key: str
    region: str = "us-east-1"
    path_style_access: bool = True
    extra: dict[str, str] = field(default_factory=dict)

    def as_properties(self) -> dict[str, str]:
        props = {
            "s3.endpoint": self.endpoint,
            "s3.access-key-id": self.access_key_id,
            "s3.secret-access-key": self.secret_access_key,
            "s3.region": self.region,
            # MinIO serves bucket-in-path, not bucket-as-subdomain.
            "s3.path-style-access": "true" if self.path_style_access else "false",
        }
        props.update(self.extra)
        return props


def _new_duckdb(threads: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads = {int(threads)}")
    return con
