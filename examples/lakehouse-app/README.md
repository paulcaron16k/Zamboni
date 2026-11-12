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
  (python -m lakehouse_app.web)                (python -m lakehouse_app.maintenance)
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
| `LAKEHOUSE_WORKERS` | as many as fit | at most this many workers; lowers the pool's own plan, never raises it |
| `LAKEHOUSE_STATE_FILE` | unset | write `zamboni serve`'s state file here, for `zamboni service-status --probe` |
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
gives the same modules and the `lakehouse-maintenance` and `lakehouse-web` commands.

### Registering a warehouse

In the web tier, at `/warehouses/new`, or from Python:

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
`Registry.put` refuses one Zamboni would refuse -- the web tier shows why and
keeps what was typed (`test_a_registration_zamboni_would_refuse_is_refused_and_the_form_kept`).

## The web tier

`python -m lakehouse_app.web` ([web.py](lakehouse_app/web.py)), serving on
`LAKEHOUSE_WEB_HOST`:`LAKEHOUSE_WEB_PORT` (`0.0.0.0:8080`). **It never imports
`zamboni.runtime`** (`test_the_web_tier_never_imports_the_runtime`): a crash,
hang or out-of-memory kill in maintenance cannot reach a process that never
loaded it.

| Page | Shows | Reads |
|---|---|---|
| `/` | warehouses, running now, maintenance replicas, quarantine, recent runs and requests | every table above |
| `/warehouses/<name>` | each table's state and last run; edit the schedule; "run now" for one table or all | the registry, `zamboni_claim`, `zamboni_run` |
| `/warehouses/new` | the registration form | — |
| `/requests/<id>` | one "run now": waiting, running, finished, and its outcome | `zamboni_request`, `zamboni_claim`, `zamboni_run` |
| `/healthz` | `ok`: liveness of this process only | nothing |
| `/readyz` | `ok` when the app's database answers | `SELECT 1` |

**"Run now" is a row.** It writes a `zamboni_request`; a maintenance replica
offers it as `manual`, claims the table and runs it, and the request's page
follows it from "waiting for a maintenance replica" through "running since" to
"finished" (`test_run_now_writes_a_request_and_its_page_follows_the_run_to_the_end`).
It works while maintenance is down too: the row waits for a replica.

**"Maintenance unavailable" is a stale tick.** A running runtime's scheduler
ticks at least every 60 s (`zamboni.scheduler.MAX_SLEEP`) and its observer
rewrites `zamboni_runtime.last_tick` each time. When no replica has ticked in
three of those, every page carries "maintenance unavailable since …" -- read
from the age of the tick, not the row's `phase`, because a SIGKILLed tier
writes no `stopped`
(`test_with_the_maintenance_tier_stopped_every_page_says_since_when`). Every
page still renders, from the database. `/healthz` never asks about maintenance:
a web pod restarted because maintenance is down would turn one outage into two.

**Deliberately minimal**: the standard library's WSGI server, hand-written HTML
with every value escaped, no JavaScript. **No authentication**: users and roles
are the app's to own (§9.1), and inventing them here would teach the wrong
thing. Put it behind your own.

```bash
PYTHONPATH=examples/lakehouse-app uv run python -m lakehouse_app.web
```

## Deploying it

One image, two tiers: [Dockerfile](Dockerfile), built from the repository root
so Zamboni comes from this tree and its lock -- the PyIceberg maintenance fork
included -- rather than from whatever PyPI resolves. The default command is the
web tier; the maintenance tier is the same image with
`python -m lakehouse_app.maintenance`. It runs as a non-root user with a
read-only root filesystem; the maintenance tier writes only to
`/run/lakehouse` and `/tmp`.

```bash
docker build -f examples/lakehouse-app/Dockerfile -t lakehouse-app .
```

### docker compose, on the dev stack

[compose.yaml](compose.yaml) adds the two tiers to the dev stack: PostgreSQL,
Lakekeeper, the object store and NATS come from
`dev-stack/docker-compose.yaml`, which it includes. It shares the dev stack's
project name, so it extends a running dev stack rather than starting a second
one -- the dev stack pins its network's subnet, and two stacks would both claim
it.

```bash
cp dev-stack/.env.sample dev-stack/.env         # once, as for the dev stack
docker compose -f examples/lakehouse-app/compose.yaml up -d --build --wait
uv run dev-stack/bootstrap.py                   # Lakekeeper's warehouse, as for the dev stack
```

Then open <http://localhost:8090/>, register a warehouse with the catalog
properties `{"type": "rest", "uri": "http://lakekeeper:8181/catalog",
"warehouse": "zamboni"}` and press "run now". The web tier never learns where
Lakekeeper is; only the maintenance tier connects to it, from the registry
row. Done by hand on 2026-11-11: a table of six small files in Lakekeeper,
registered and run from the dashboard, was compacted to one file by the
maintenance container and its request page read "finished: ok".

### Kubernetes

[kubernetes/maintenance.yaml](kubernetes/maintenance.yaml) and
[kubernetes/web.yaml](kubernetes/web.yaml) (with [its Service](kubernetes/web-service.yaml)): two Deployments from the one
image, each with its own resources. What differs from `zamboni serve`'s
manifest in [docs/devops.md §8](../../docs/devops.md#8-zamboni-serve--a-fleet-with-no-crontab),
and why:

| Setting | Here | Why |
|---|---|---|
| maintenance `replicas: 2`, `RollingUpdate` | serve needs 1 and `Recreate` | claims in the app's database make each table one replica's at a time (§9.3) |
| maintenance `terminationGracePeriodSeconds: 21900` | longest table plus margin | a table can run no longer than its reason's `TABLE_TIMEOUTS` limit, 6 h at most, so that is the longest table; `test_the_grace_period_outlasts_the_longest_table` holds it to the declaration |
| maintenance probes | serve's own | `LAKEHOUSE_STATE_FILE` makes this process write serve's state file, so `zamboni service-status --probe` reads it unchanged. A replica with no warehouse registered yet is alive and not ready (`test_a_tier_waiting_for_its_first_warehouse_is_alive_and_not_ready`) |
| maintenance `timeoutSeconds: 10` | over the measured time | the probe took 0.74–0.95 s at 1–2 CPUs and 4.7–5.6 s at 0.25 CPU in this image, five runs each; the 1 s default would fail it |
| web probes, `timeoutSeconds: 2` | HTTP, never about maintenance | `/healthz` answered in 1–23 ms and `/readyz` (the database) in 12–14 ms, ten requests each; a web pod restarted because maintenance is down would turn one outage into two |
| web `terminationGracePeriodSeconds: 10` | short | a page in flight is all it has to lose |

`test_every_probe_has_a_timeout_over_its_measured_time` and
`test_each_tier_has_its_own_resources` hold the rest. Secrets come from
`lakehouse-maintenance` and `lakehouse-web` Secrets by `envFrom`: the database
URL for both, a catalog credential for the maintenance tier only. The web tier
never touches a catalog.

The manifests are examples: an image registry, a Secret and a database are
yours to supply. They have not been applied to a cluster in this repository's
CI; the tests above check what they say against Zamboni's declarations and the
measured figures.
