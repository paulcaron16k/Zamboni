# The reference Lakehouse App

A small application that embeds Zamboni's maintenance runtime the way
[docs/maintenance-runtime.md §9](../../docs/maintenance-runtime.md#9-the-lakehouse-app-the-integration-contract)
says an application must. It exists for two reasons:

- **to demonstrate** a Lakehouse deployment to anyone building one. Every
  module is short and says which part of the contract it is;
- **to verify** the contract end to end. It runs in this repository's CI, so
  the design is proven by something that runs rather than only described.

It is not part of the `iceberg-zamboni` distribution
(`test_the_wheel_ships_no_example`).

## Two tiers, one database

```
  web tier                                     maintenance tier
  (lakehouse_app.web)                          (python -m lakehouse_app.maintenance)
  ┌───────────────────────────┐                ┌──────────────────────────────────────┐
  │ registry: add, edit       ├──── rows ────▶ │ RegistryWatcher ──▶ update_fleet()    │
  │ "run now"                 ├──── rows ────▶ │ RequestSource ──▶ offer(manual)       │
  │ dashboard ◀── SELECT ─────┤                │ MaintenanceRuntime                    │
  │                           │ ◀─── rows ──── │  ├── claims (PostgresClaimStore)      │
  │ never imports             │                │  ├── workers: lakehouse_app.entry     │
  │ zamboni.runtime           │                │  └── observer: StateRows              │
  └─────────────┬─────────────┘                └──────────────────┬───────────────────┘
                └─────────────────── PostgreSQL ───────────────────┘
```

The tiers never call each other. Everything that crosses is a row:

| Table | Written by | Read by | Defined in |
|---|---|---|---|
| `lakehouse_warehouse` | web tier | maintenance tier | the app ([schema.sql](lakehouse_app/schema.sql)) |
| `zamboni_request` | web tier ("run now") | maintenance tier | §9.2 |
| `zamboni_claim` | maintenance tier | maintenance tier, dashboard ("running now") | §6.1 |
| `zamboni_runtime`, `zamboni_run` | maintenance tier | dashboard | §9.2 |
| `lakehouse_quarantine` | maintenance tier | dashboard | the app |

The §6.1 and §9.2 tables and statements are the document's, held to it by
`test_each_statement_is_the_documents` and
`test_the_schema_creates_the_documents_tables`.

## The maintenance tier

`python -m lakehouse_app.maintenance` ([maintenance.py](lakehouse_app/maintenance.py)).
It creates its tables if they are missing, waits until a warehouse is
registered, and then runs one `MaintenanceRuntime` on its main thread:

| §9 asks for | Here |
|---|---|
| the fleet, from the app's registry, every change pushed through `update_fleet()` (§9.5 point 4) | [registry.py](lakehouse_app/registry.py) builds it with `FleetConfig.from_dict`; `RegistryWatcher` polls and pushes |
| a `ClaimStore` over the app's database (§9.5 point 1) | [claims.py](lakehouse_app/claims.py) |
| a worker entry that builds its own session (§9.5 point 2) | [entry.py](lakehouse_app/entry.py): catalog properties from the registry, credentials from the environment, `zamboni.maintain` |
| a `RequestSource` (§9.5 point 5) | [request_table.py](lakehouse_app/request_table.py) |
| state rows the dashboard reads (§9.2) | [state.py](lakehouse_app/state.py), handed off the runtime's loop |

Each passes the matching `zamboni.conformance` suite against PostgreSQL
(`tests/test_lakehouse_app_live.py`): `test_the_apps_claim_store_passes`,
`test_the_apps_entry_passes`, `test_the_apps_health_entry_passes` and
`test_the_apps_request_table_passes`.
`test_a_registry_edit_reaches_the_running_runtime_and_re_arms_its_schedule`
edits the registry under a running runtime and watches the new schedule fire.

### Settings

All from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `LAKEHOUSE_DATABASE_URL` | — | the app's PostgreSQL, as a libpq string or URI. A secret: it holds the password |
| `LAKEHOUSE_DATABASE_SCHEMA` | `lakehouse` | the schema the app's tables live in |
| `LAKEHOUSE_MAINTENANCE_COMMIT` | unset | `yes` lets maintenance commit; anything else previews every table |
| `LAKEHOUSE_REGISTRY_POLL_SECONDS` | `10` | how often the registry is re-read |
| `LAKEHOUSE_RESERVED_MEMORY_BYTES` | `0` | this process's own baseline, kept out of the pool's plan (§9.4) |
| `LAKEHOUSE_WORKDIR` | a temporary directory | workers' scratch files |
| `LAKEHOUSE_CATALOG_CREDENTIAL` | unset | a REST catalog's OAuth2 `client_id:client_secret` |
| `LAKEHOUSE_CATALOG_TOKEN` | unset | a REST catalog's bearer token |

**Credentials never go in the registry.** Every dashboard user can read it, so
`Registry.put` refuses catalog properties that hold one
(`test_the_registry_refuses_a_credential`). The worker entry adds the two
catalog credentials above from its own environment.

**Without `LAKEHOUSE_MAINTENANCE_COMMIT=yes` nothing is committed**, the same
rule as `zamboni serve`'s `--yes`: consent is given, never assumed.

SIGTERM stops it gracefully: no new tables start, and those in flight finish.
A second signal kills them, which is safe for the reason in §7.1.

### Running it from this repository

```bash
cd dev-stack && docker compose up -d --wait && cd ..
export LAKEHOUSE_DATABASE_URL="host=localhost port=5433 dbname=lakekeeper user=lakekeeper password=lakekeeper"
PYTHONPATH=examples/lakehouse-app uv run python -m lakehouse_app.maintenance
```

Those are the dev stack's own throwaway credentials, from `dev-stack/.env.sample`.
To install it elsewhere instead, `pip install ./examples/lakehouse-app`
gives the same module and a `lakehouse-maintenance` command.

### Registering a warehouse

Until the web tier exists, from Python:

```python
from lakehouse_app.db import Database
from lakehouse_app.registry import Registry, WarehouseRow

Registry(Database.from_environ()).put(
    WarehouseRow(
        name="zamboni",  # the Lakekeeper warehouse
        schedule="0 2 * * *",  # or {"window": {...}}, as in a fleet file
        table_config={"namespaces": {"raw": {"tables": {"events": {}}}}},
        catalog={"type": "rest", "uri": "http://localhost:8182/catalog", "warehouse": "zamboni"},
    )
)
```

`schedule` and `table_config` are exactly a fleet file's entry, and
`Registry.put` refuses one Zamboni would refuse.
