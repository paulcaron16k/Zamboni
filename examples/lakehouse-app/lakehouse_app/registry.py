# SPDX-License-Identifier: Apache-2.0
"""The warehouse registry: the app's list of warehouses, and the fleet built from it.

Each row is one entry of a fleet file -- a name, a schedule and an inline table
config -- plus the PyIceberg catalog properties the worker entry builds its
session from. So the fleet is :meth:`zamboni.fleet.FleetConfig.from_dict` of the
rows, with Zamboni's own validation, and the app keeps no second copy of what
a valid warehouse is (docs/maintenance-runtime.md §9.1: turning the registry
into a `FleetConfig` is the app's; what the config means is Zamboni's).

Both tiers import this module: the web tier to list and edit warehouses, the
maintenance tier to build its fleet. ``zamboni.fleet`` does not import
``zamboni.runtime``, so neither does this.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb

from zamboni.fleet import FleetConfig, FleetConfigError
from zamboni.urls import carries_userinfo

from .db import Database

#: Catalog property names that hold a credential. Refused in the registry, so
#: a credential is never in a table both tiers and every dashboard user read:
#: the worker entry adds them from its environment instead (lakehouse_app.catalogs).
SECRET_CATALOG_KEYS = frozenset(
    {"credential", "token", "password", "s3.secret-access-key", "s3.session-token"}
)


class RegistryError(ValueError):
    """A warehouse the registry will not hold, or a fleet it cannot build."""


@dataclass(frozen=True)
class WarehouseRow:
    name: str
    #: A cron expression, or ``{"window": {...}}``, as a fleet file writes it.
    schedule: str | dict[str, Any]
    #: The body of a table-config.json.
    table_config: dict[str, Any]
    #: PyIceberg catalog properties, never a credential.
    catalog: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime | None = None

    def fleet_entry(self) -> dict[str, Any]:
        """This warehouse as a fleet file's entry."""
        entry: dict[str, Any] = {
            "name": self.name,
            "schedule": self.schedule,
            "table_config": self.table_config,
        }
        if uri := self.catalog.get("uri"):
            entry["uri"] = uri
        return entry


class Registry:
    def __init__(self, database: Database) -> None:
        self.database = database

    def warehouses(self) -> list[WarehouseRow]:
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT name, schedule, table_config, catalog, updated_at"
                " FROM lakehouse_warehouse ORDER BY name"
            ).fetchall()
        return [WarehouseRow(*row) for row in rows]

    def warehouse(self, name: str) -> WarehouseRow:
        with self.database.connect() as conn:
            row = conn.execute(
                "SELECT name, schedule, table_config, catalog, updated_at"
                " FROM lakehouse_warehouse WHERE name = %s",
                (name,),
            ).fetchone()
        if row is None:
            raise KeyError(name)
        return WarehouseRow(*row)

    def put(self, row: WarehouseRow) -> None:
        """Create or replace a warehouse -- only if Zamboni would accept it.

        Validated by building a one-warehouse fleet from it, so a bad schedule
        or table config is refused here, where the person who wrote it is,
        rather than by every maintenance replica's next reload.
        """
        check(row)
        with self.database.connect() as conn:
            conn.execute(
                "INSERT INTO lakehouse_warehouse (name, schedule, table_config, catalog)"
                " VALUES (%s, %s, %s, %s)"
                " ON CONFLICT (name) DO UPDATE SET schedule = EXCLUDED.schedule,"
                " table_config = EXCLUDED.table_config, catalog = EXCLUDED.catalog,"
                " updated_at = now()",
                (row.name, Jsonb(row.schedule), Jsonb(row.table_config), Jsonb(row.catalog)),
            )

    def remove(self, name: str) -> None:
        with self.database.connect() as conn:
            conn.execute("DELETE FROM lakehouse_warehouse WHERE name = %s", (name,))

    def fleet(self) -> FleetConfig:
        """Every registered warehouse, as the runtime's fleet.

        Raises :class:`RegistryError` when there is none: Zamboni refuses an
        empty fleet, because a generator that fails open writes one, and
        adopting it would stop all maintenance by being wrong.
        """
        return build_fleet(self.warehouses())


def build_fleet(rows: list[WarehouseRow]) -> FleetConfig:
    try:
        return FleetConfig.from_dict({"warehouses": [row.fleet_entry() for row in rows]})
    except FleetConfigError as exc:
        raise RegistryError(str(exc)) from exc


def check(row: WarehouseRow) -> None:
    secrets = sorted(SECRET_CATALOG_KEYS & set(row.catalog))
    if secrets:
        raise RegistryError(
            f"warehouse {row.name!r}: the catalog properties hold a credential ({secrets}); "
            "the registry is read by every dashboard user, so give it to the maintenance "
            "tier's environment instead"
        )
    uri = row.catalog.get("uri")
    if isinstance(uri, str) and carries_userinfo(uri):
        raise RegistryError(f"warehouse {row.name!r}: the catalog uri has a credential in it")
    build_fleet([row])
