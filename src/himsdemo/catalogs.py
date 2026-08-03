"""Where the demo's tables live: a local SQLite catalog, or the dev stack.

Two backends, one interface, because they differ in more than a connection
string:

- **Existence.** A SQLite catalog is a file, so "does a catalog exist yet" is a
  file check -- and it has to be, because SQLAlchemy creates the file on connect,
  so opening one to look would be the mutation a read-only command must not
  make. A REST catalog always exists; the question becomes whether the demo's
  *namespace* is there.
- **Clearing.** Locally, `clear` deletes a directory and a file. Remotely there
  is nothing to delete on disk: the tables must be dropped through the catalog,
  and *when* the objects go is the warehouse's business, not ours -- it depends
  on its delete-profile, which may purge immediately or after a delay.

Selection is by ``--catalog`` or ``ZAMBONI_DEMO_CATALOG``; the default stays
``sqlite`` so `./bin/demo` works with no Docker and no configuration.
"""

from __future__ import annotations

import contextlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from zamboni import CatalogSession, S3Settings

NAMESPACE = "healthims"
BACKENDS = ("sqlite", "lakekeeper")
DEFAULT_BACKEND = "sqlite"

#: Read from the environment so `dev-stack/.env` can drive the demo directly.
ENV_BACKEND = "ZAMBONI_DEMO_CATALOG"
ENV_URI = "ZAMBONI_URI"
ENV_WAREHOUSE = "ZAMBONI_WAREHOUSE"
ENV_S3_ENDPOINT = "ZAMBONI_S3_ENDPOINT"
ENV_S3_KEY = "ZAMBONI_S3_ACCESS_KEY_ID"
ENV_S3_SECRET = "ZAMBONI_S3_SECRET_ACCESS_KEY"
ENV_S3_REGION = "ZAMBONI_S3_REGION"


class CatalogUnavailable(RuntimeError):
    """The selected catalog cannot be reached, with what to do about it."""


def resolve_backend(explicit: str | None) -> str:
    name = explicit or os.environ.get(ENV_BACKEND) or DEFAULT_BACKEND
    if name not in BACKENDS:
        raise CatalogUnavailable(f"unknown catalog {name!r}; choose from {', '.join(BACKENDS)}")
    return name


@dataclass(frozen=True)
class DemoCatalog:
    """How to open, test for, and clear the demo's tables."""

    backend: str
    #: Human-readable location, for `status` and `clear` output.
    location: str

    @property
    def is_remote(self) -> bool:
        return self.backend != "sqlite"


def build(backend: str, state) -> DemoCatalog:
    if backend == "sqlite":
        return DemoCatalog(backend="sqlite", location=str(state.catalog_path))

    uri = os.environ.get(ENV_URI)
    warehouse = os.environ.get(ENV_WAREHOUSE)
    if not uri or not warehouse:
        raise CatalogUnavailable(
            f"--catalog lakekeeper needs {ENV_URI} and {ENV_WAREHOUSE}.\n"
            "  cd dev-stack && docker compose up -d && uv run bootstrap.py\n"
            "  then export the two lines it prints."
        )
    return DemoCatalog(backend=backend, location=f"{uri} warehouse={warehouse}")


def _s3_settings() -> S3Settings | None:
    """Direct MinIO settings, when given.

    Omitted entirely against an STS-vending warehouse: Lakekeeper hands back
    temporary credentials per table and those win, so supplying keys here is
    redundant. They are still accepted because a warehouse configured without
    STS needs them.
    """
    endpoint = os.environ.get(ENV_S3_ENDPOINT)
    if not endpoint:
        return None
    return S3Settings(
        endpoint=endpoint,
        access_key_id=os.environ.get(ENV_S3_KEY),
        secret_access_key=os.environ.get(ENV_S3_SECRET),
        region=os.environ.get(ENV_S3_REGION, "us-east-1"),
    )


def open_session(catalog: DemoCatalog, state, *, create: bool) -> CatalogSession:
    if not catalog.is_remote:
        if create:
            state.warehouse_path.mkdir(parents=True, exist_ok=True)
        return CatalogSession.for_local(
            warehouse_path=str(state.warehouse_path),
            uri=f"sqlite:///{state.catalog_path}",
            name=NAMESPACE,
        )

    return CatalogSession.for_lakekeeper(
        uri=os.environ[ENV_URI],
        warehouse=os.environ[ENV_WAREHOUSE],
        s3=_s3_settings(),
    )


def exists(catalog: DemoCatalog, state) -> bool:
    """Whether the demo has anything in this catalog yet."""
    if not catalog.is_remote:
        return state.catalog_path.exists()

    session = open_session(catalog, state, create=False)
    try:
        session.catalog.list_tables(NAMESPACE)
        return True
    except NoSuchNamespaceError:
        return False
    finally:
        session.close()


def clear(catalog: DemoCatalog, state, table_names: list[str]) -> str:
    """Remove everything the demo created. Returns a line describing what went.

    Inputs -- the source CSVs and the two config files -- are never touched.
    """
    if not catalog.is_remote:
        if state.warehouse_path.exists():
            shutil.rmtree(state.warehouse_path)
        if state.spill_path.exists():
            shutil.rmtree(state.spill_path)
        state.catalog_path.unlink(missing_ok=True)
        for suffix in ("-wal", "-shm", "-journal"):
            Path(str(state.catalog_path) + suffix).unlink(missing_ok=True)
        return f"dropped the local catalog and warehouse under {state.root}"

    if state.spill_path.exists():
        shutil.rmtree(state.spill_path)

    session = open_session(catalog, state, create=False)
    dropped = 0
    try:
        for name in table_names:
            try:
                session.catalog.drop_table(f"{NAMESPACE}.{name}")
                dropped += 1
            except (NoSuchTableError, NoSuchNamespaceError):
                pass
        # A non-empty namespace, or a catalog that refuses to drop one, is not a
        # failure to clear -- the tables are gone either way.
        with contextlib.suppress(Exception):
            session.catalog.drop_namespace(NAMESPACE)
    finally:
        session.close()
    # Deliberately says nothing about when the objects go. That depends on the
    # warehouse's delete-profile, which this code does not set and cannot see
    # from here -- the dev stack's default turns out to be `hard`, so an earlier
    # version of this message telling the user it was a soft delete was simply
    # wrong.
    return f"dropped {dropped} table(s) from {catalog.location}"
