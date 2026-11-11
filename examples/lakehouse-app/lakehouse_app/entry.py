# SPDX-License-Identifier: Apache-2.0
"""The maintenance tier's worker entries (docs/maintenance-runtime.md §7, §9.5 point 2).

Each runs in a worker process the runtime spawned, given one job by pickle, and
builds its catalog session **the way an application does**: catalog properties
from the registry, credentials from its own environment, the session from
`zamboni.CatalogSession.from_catalog`. Not through ``zamboni.yml`` or the CLI,
which is what the default entries (`zamboni.pool.run_table`) do -- the point of
this app is to show the integration a Lakehouse App writes.

- :func:`run` maintains one table through `zamboni.maintain` and returns its
  run record, the same object ``zamboni maintenance --json`` writes.
- :func:`check` is the pressure-check entry: `zamboni.health.check_tables` over
  the warehouse, which exists for exactly this, an entry with its own catalog.

Credentials, by environment variable, added to the registry's properties:

- ``LAKEHOUSE_CATALOG_CREDENTIAL``: a REST catalog's OAuth2 client credential,
  ``client_id:client_secret`` (PyIceberg's ``credential``);
- ``LAKEHOUSE_CATALOG_TOKEN``: a REST catalog's bearer token (``token``).

One pair for every warehouse, which is enough for one Lakekeeper. An app with a
credential per warehouse would look it up by name here, from its secret store.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pyiceberg.catalog import Catalog, load_catalog

from zamboni import (
    RUNBOOK_ORDER,
    CatalogSession,
    CompactionConfig,
    MinimumInterval,
    Operation,
    maintain,
)
from zamboni.health import check_tables

from .db import Database
from .registry import Registry

if TYPE_CHECKING:
    from zamboni.pool import WorkerContext
    from zamboni.scheduler import PressureCheck, WorkItem

CREDENTIALS = {"LAKEHOUSE_CATALOG_CREDENTIAL": "credential", "LAKEHOUSE_CATALOG_TOKEN": "token"}


def catalog_for(warehouse: str, environ: Mapping[str, str] | None = None) -> Catalog:
    """``warehouse``'s catalog: its registry properties, plus this process's credentials."""
    environ = os.environ if environ is None else environ
    properties = dict(Registry(Database.from_environ(environ)).warehouse(warehouse).catalog)
    for variable, key in CREDENTIALS.items():
        if value := environ.get(variable):
            properties[key] = value
    return load_catalog(warehouse, **properties)


def run(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Maintain ``item.table``; its run record.

    Honours everything the runtime put on the item, as the default entry's
    command line does (`zamboni.pool.worker_argv`): its operations, its
    ``commit`` (False previews -- the runtime's consent, never the entry's), its
    minimum interval, and this worker's share of threads, memory and spill.
    """
    context.progress(f"maintenance {item.table}")
    base = CompactionConfig(max_temp_directory_bytes=context.max_temp_directory_bytes)
    minimum = None
    if item.interval_rule is not None:
        interval, anyway = item.interval_rule
        minimum = MinimumInterval(interval, frozenset(Operation(o) for o in anyway))
    with CatalogSession.from_catalog(
        catalog_for(item.warehouse),
        threads=context.threads,
        memory_limit_bytes=context.duckdb_memory_bytes,
    ) as session:
        report = maintain(
            session,
            table_config=item.table_config,
            tables=[item.table],
            operations=RUNBOOK_ORDER if item.operations is None else item.operations,
            commit=item.commit,
            warehouse=item.warehouse,
            base_config=base,
            min_interval=minimum,
            progress=context.progress,
        )
    return report.as_dict()


def check(job: PressureCheck, context: WorkerContext) -> dict[str, Any]:
    """Measure every table of ``job.warehouse`` from metadata; the health record."""
    return check_tables(
        catalog_for(job.warehouse), job.table_config.tables, warehouse=job.warehouse
    )
