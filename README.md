# Zamboni

[![CI](https://github.com/paulcaron16k/Zamboni/actions/workflows/ci.yml/badge.svg)](https://github.com/paulcaron16k/Zamboni/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Iceberg table maintenance -- compaction, Z-order clustering, partition evolution,
dangling-delete removal, manifest rewriting, snapshot expiry and orphan-file removal --
**without needing Trino or Spark**. It can drive either Trino or Spark if you have one.

```bash
pipx install iceberg-zamboni       # the distribution name; you still `import zamboni`
# Copy zamboni.yml.sample to $HOME/.zamboni.yml and edit
cat ~/.zamboni.yml
zamboni doctor                      # is this PyIceberg build usable?
zamboni --db acme maintenance       # previews; --yes commits
```

**New here?** [docs/user_guide.md](docs/user_guide.md) is the place to start: four ways to
run it, an engine-capability table to choose with, secrets handling, and the memory
numbers. This README is the *why* -- the evidence behind the design decisions.


## Installation Use-Cases

There are various ways to create and use an Iceberg DB. While Zamboni is for maintenance
only, depending on how you _CREATE_ and _USE_ your Iceberg DB the installation varies.

**Everything about *using* the tables is reference only.** Reading them with DuckDB,
pandas, polars, Trino or a BI tool is outside what Zamboni does -- it compacts, sorts,
expires and reclaims, and then gets out of the way. The reading tools are named below so
you can see where a maintenance install sits in a working stack, not because this package
provides, configures or supports them.

### Dev -- `iceberg-zamboni[sql]`

A laptop, no services, nothing to start. A local SQLite catalog and a warehouse directory,
which is enough to write tables programmatically through the PyIceberg catalog and read
them back with pandas or DuckDB. `zamboni-demo` runs on exactly this.

```bash
pipx install "iceberg-zamboni[sql]"
zamboni-demo next-day        # five days of simulated ingestion, then maintain it
```

### Home Lab -- `iceberg-zamboni[s3]`

A real catalog and a real object store, still on one machine: **Lakekeeper** (which brings
Postgres) and **MinIO**, both from the dev stack's docker services. This is the smallest
setup that behaves like production -- REST catalog, S3 paths, vended credentials -- and the
best base for programmatic data science against the tables: pandas or polars, embedded
DuckDB, SQLAlchemy.

```bash
cd dev-stack && docker compose up -d --wait      # Lakekeeper + Postgres + MinIO
pipx install "iceberg-zamboni[s3]"
zamboni maintenance --warehouse demo --yes
```

### Home Lab Professional -- `iceberg-zamboni[s3]` + Trino for *query*

Home Lab, plus the dev stack's Trino profile:

```bash
cd dev-stack && docker compose --profile trino up -d trino
```

Trino is what lets **Apache Superset** and other BI tools query the Iceberg tables -- they
speak SQL to an engine, and DuckDB will not serve them. **You do _NOT_ use Trino for
maintenance here.** Compaction stays with the local engine: Trino cannot Z-order, cannot
remove dangling deletes, and enforces retention floors that reject Zamboni's defaults.
Adding Trino for BI does not mean adding `--engine trino`.

The `s3` extra is unchanged -- Superset talks to Trino, not to Zamboni, so nothing here
needs the `trino` extra either. That extra is only for `--engine trino`.

### Enterprise -- `iceberg-zamboni[s3,spark]`

Your Spark cluster does the heavy lifting: large-scale compaction and sorting, on hardware
sized for it rather than on one machine's disk and CPU. IT provides the object store (MinIO
on real disk, AWS S3, GCS), the REST catalog (Lakekeeper or Polaris), and the Spark
master -- you point Zamboni at those services when you run it.

```bash
pip install "iceberg-zamboni[s3,spark]"
zamboni maintenance --warehouse acme --engine spark \
        --spark-remote sc://spark.internal:15002 --yes
```

`spark` here is the Spark **Connect client**, ~13MB and no JVM on your side. For BI, IT
provides a Trino cluster -- again for query, not for maintenance.

## Install

### Optional Package Extensions

Depending on the installation use-case you are using, the installation requires different
extensions or extras.

| Extra | Purpose, and when you need it | Python dependencies |
|---|---|---|
| `s3` | Object-store access. Required whenever data files live in S3, MinIO or GCS rather than on a local path -- so for every use-case above except Dev. Provides the fsspec filesystem PyIceberg and orphan removal use to read, write, **list** and delete objects | `s3fs` |
| `sql` | A local SQLite catalog, with no catalog server to run. Enough to create and open a warehouse on a laptop; used by the test suite, the demo, and dry runs. Not needed against a REST catalog such as Lakekeeper or Polaris | `pyiceberg[sql-sqlite]` (`sqlalchemy`) |
| `trino` | The client for `--engine trino`, which runs five of the six operations as `ALTER TABLE … EXECUTE`. **Requires a local Trino stand-alone instance via `dev-stack --profile trino`, or an Enterprise Trino cluster** -- this extra is a client and starts nothing. Not needed to *query* through Trino from a BI tool | `trino` |
| `spark` | The Spark **Connect** client for `--engine spark --spark-remote sc://…`, which runs all six operations through the Iceberg Spark procedures, Z-order included. **Requires a local Spark stand-alone instance via `dev-stack --profile spark`, or an Enterprise Spark master/cluster.** ~13MB on disk, pure Python, and **no JVM on your machine** -- the driver runs on the server. Needs Spark 4 | `pyspark-client` |
| `spark-lib` | The **embedded** Spark library: a driver JVM inside this process, so no Spark server or cluster is required anywhere. The cost is why it is not the default -- **~472MB on disk** and **~470MB resident** for even a trivial session (measured: 440MB JVM + 34MB Python), rising toward `spark.driver.memory` (1GB by default) under real work, plus a JDK 17 or 21 you install yourself. Mutually exclusive with `spark`: both provide the `pyspark` module | `pyspark` |

`spark` and `spark-lib` are the two halves of the same engine and the names say which side
the JVM is on. Pick `spark` if a cluster exists, `spark-lib` only if none does and you
would rather carry half a gigabyte than run a server.

### Installation Best-Practices

**Never into your system Python.** Pick by what you want:

**The distribution is `iceberg-zamboni`; the import is `zamboni`.** They differ because
`zamboni` on PyPI is a dormant registration by an unrelated project. Only the distribution
name has to be globally unique, and the mismatch is ordinary — `beautifulsoup4` imports as
`bs4`.

**The CLI, isolated — recommended.** `pipx` gives it its own virtual environment and puts
`zamboni` on your PATH. We include `sql` to run zamboni-demo:

```bash
pipx install "iceberg-zamboni[s3,sql]"
uv tool install "iceberg-zamboni[s3,sql]"     # the uv equivalent
```

**The library, in your project's environment.** Activate a virtual environment first:

```bash
python -m venv .venv && . .venv/bin/activate
pip install "iceberg-zamboni[s3,sql]"

uv add "iceberg-zamboni[s3,sql]"              # the uv equivalent; no activation needed
```

**From the repository**, for the unreleased tip:

```bash
pipx install "git+https://github.com/paulcaron16k/Zamboni"
```

## Status: what depending on this commits you to

Read this before depending on it.

- **`0.x`.** The public surface is believed reasonably firm, but a `0.x` release may still
  break a CLI script or an API call; every effort will be made to avoid it. From `1.0.0`
  onward the usual semver promise applies -- minor and patch releases will not break you.
  **The unusual part, and the reason to read
  [docs/releasing.md](docs/releasing.md):** for this tool a changed *default* is a breaking
  change even when no signature moves. Lowering `older_than_days` deletes files on the next
  nightly run with nothing in the API having moved at all, so the destructive defaults are
  treated as public surface.
- **Small team (of one).** Security response is best-effort rather than contractual
  ([SECURITY.md](SECURITY.md)), and there is no backport branch: fixes land on the latest
  `0.x`, or `1.x.x` once released, with a minor version, or possibly only patch
  version, increment. Contributors welcome.
- **CI runs green.** Five jobs on every push and pull request -- six runs, since `test` is
  a matrix over Python 3.11 and 3.13 -- including `dev-stack`, which brings up a real
  Lakekeeper, Postgres and MinIO and runs the demo end to end, and `spark`, which builds a
  Spark Connect server with the Iceberg runtime. The badge above reports the current state
  rather than a claim about it.
- **Verified against real infrastructure.** Every operation has been run against a
  live Lakekeeper + MinIO, and against real Trino 483 and Spark 4.0.4 servers.
- **PyIceberg 0.11.x is fully supported**, and `pyproject.toml` caps at `<0.12` while the
  0.12 release candidates are being tested -- see [below](#why-pyiceberg-is-capped-at-012).
- **On PyPI as `iceberg-zamboni`**, imported as `zamboni`. See
  [Install](#install); the names differ because `zamboni` on PyPI is a dormant
  registration by an unrelated project.

## What it is, and what it is not

**It is** a maintenance tool for Iceberg tables that runs as a single process: PyIceberg for
metadata, DuckDB for sorting. One command a cron line can call, one declarative file
describing what each table's layout should be and what maintenance may delete. It may optionally
use Trino or Spark standalone or clusters.

**It is not:**

- **A query engine.** It rewrites files and commits snapshots; it does not serve queries.
- **A catalog.** It talks to yours.
- **A scheduler.** It exits with a code; cron, Airflow or systemd decides when.
- **An ingestion tool.** Something else writes the data.
- **A cluster.** Throughput is one machine's disk and CPU. For a partition larger than that
  machine can chew through in the window you have, use `--engine spark`.
- **A replacement for Trino or Spark** where you already run them. It is what you use when
  you do not, and it drives them through the same config when you do.

---

Nothing in the MinIO + Lakekeeper stack compacts tables today:

- **Lakekeeper OSS** ships queues for its own bookkeeping only. Verified against running
  servers: `v0.13.1` reports `["tabular_expiration", "tabular_purge", "task_log_cleanup"]`
  and `latest-main` reports `["soft_deletion", "tabular_purge", "task_log_cleanup"]`.
  `tabular_expiration` is easy to misread — it expires soft-deleted *tables* after a delay
  (7 days by default), not snapshots. None of these compacts data files, expires snapshots
  or removes orphan files. `tests/test_dev_stack.py` checks the queue set against an
  allow-list, so a genuinely new queue fails the build rather than slipping past a
  keyword match.
- **PyIceberg** (0.11.1, the current release) exposes `table.maintenance.expire_snapshots()`
  and nothing else.
  That call is metadata-only — it emits a `RemoveSnapshotsUpdate` and never deletes a file.
- **DuckDB's released Iceberg extension** has no compaction. `iceberg_rewrite_data_files`
  exists on `duckdb-iceberg` main but is not in the shipped extension; loading `iceberg`
  in DuckDB 1.5.4 exposes 16 `iceberg_*` functions and that is not one of them.

## Try it: the HIMS discharge demo

Five days of simulated hospital discharge ingestion, so you can watch small files
accumulate and see what maintenance does about them. Copy-on-Write (CoW) and
Merge-on-Read (MoR) are selectable, the default is `mode cow` - which is not optimal for
ingestion via frequent updates or transactional data. The Iceberg warehouse created is
in data/healthims/iceberg_warehouse/ in namespace/schema healthims/ folder. The `iceberg_warehouse`
folder is created _after_ the first `next-day` action.

**From an install**, with no clone — the five days of input data ship in the wheel
(212 KB), and the demo writes to `./zamboni-demo/` in whatever directory you run it from:

```bash
pipx install "iceberg-zamboni[sql]"
zamboni-demo next-day        # x5
zamboni-demo query           # note "files scanned"
zamboni-demo maintenance
zamboni-demo query           # identical rows, far fewer files
```

**From a clone**, `./bin/zamboni-demo` is the same program and keeps its state in
`data/healthims/`:

```bash
./bin/zamboni-demo clear
./bin/zamboni-demo mode cow        # Copy-on-Write is the default. Clear and repeat with "mode mor"
                           # and time the next-day ingestion to see performance difference.
./bin/zamboni-demo next-day        # Repeat 5 times -- each prints status; file counts climb
./bin/zamboni-demo query           # note "files scanned"
./bin/zamboni-demo maintenance     # compact + drop dangling deletes + expire + remove orphans
./bin/zamboni-demo query           # identical rows, far fewer files
./bin/zamboni-demo next-day        # "No More Data"
```

The employees table is deliberately included as a control: a full daily replace leaves
exactly one live file, so maintenance correctly reports nothing to do there.

After five days in the default copy-on-write mode, `hims_events` holds 625 rows in
**58 data files averaging 3.8 KiB**, with **340 KiB of metadata over 218 KiB of data** --
metadata costs more than the data it describes. One `maintenance` run takes that to a
single file and 89 KiB of metadata, with every row unchanged.

The queries feel it. Files scanned and elapsed time, before and after the same
`maintenance` run:

| Query | files before | files after | ms before | ms after |
|---|---|---|---|---|
| Discharges by replication key | 18 | 2 | 142 | 26 |
| Cancelled and restarted | 75 | 2 | 357 | 31 |
| Event trace for one discharge | 59 | 2 | 262 | 53 |
| Daily EVS turnaround metric | 58 | 1 | 253 | 38 |

Timings include the Iceberg read, which is the part compaction changes; they are still
labelled indicative in the tool's own output because at 625 rows fixed overhead is a large
share of them. Files scanned is exact.

### Why storage does not fall

`maintenance` reports two counts per table, and they diverge:

```
data files    1      total    7.0KiB   avg    7.0KiB
on disk       120    total  538.6KiB   119 superseded, 0 unreferenced
```

Compaction supersedes files; it does not delete them. Every one of those 119 is still
referenced by an older snapshot and still readable by time travel. Only snapshot expiry
drops those references, and only then can the files be deleted.

The default run does both -- and still deletes nothing, because the demo's files were
written minutes ago and both defaults are age-based (5 days for snapshots, 3 for orphans).
It says so rather than hiding it:

```
healthims.hims_discharge: scanned 426 file(s), 421 referenced, 5 unreferenced
  deleted 0 file(s) (0B)
  5 file(s) (7.0KiB) left in place: younger than the 3-day age guard
```

Those are `metadata.json` files nothing references, and the cause is worth knowing:
`SqlCatalog.create_table` writes the metadata file to storage *before* it attempts the
catalog insert, so `create_table_if_not_exists` on an existing table leaves the file behind.
The demo calls it on every state-changing command, so each day after the first strands one
per table. That is the canonical orphan -- a write that was made and then abandoned -- and
it is why orphan removal cannot ask the catalog what exists.

The `on disk` line above counts parquet only, to stay comparable with `data files`, which is
why it reports zero unreferenced. Superseded and unreferenced are different problems with
different owners: expiry and orphan removal respectively.

`./bin/zamboni-demo maintenance --reclaim-now` lifts both. Storage then falls to exactly what is
live -- 120 parquet files to 1, with all 44 rows intact:

```
data files    1      total    7.0KiB   avg    7.0KiB
on disk       1      total    7.0KiB   0 superseded, 0 unreferenced
```

That flag is a demo affordance, not a recommendation: the age guard is what stops orphan
removal deleting a file another writer has written but not yet committed.

`./bin/zamboni-demo mode mor` replays the same five days as a merge-on-read table, where updates
land as position deletes instead of rewrites. Those delete files are **written directly to
simulate what Spark or Flink would emit** -- PyIceberg's `delete()` and `upsert()` are both
copy-on-write, so it cannot produce them. The demo says so in its own output.

Simulating that faithfully needs one thing PyIceberg does not provide. Appending a delete
file through its own producer puts it in a manifest labelled `content: data`, because
`ManifestWriterV2` hardcodes the content and there is no delete-manifest writer. PyIceberg
and DuckDB both dispatch on the *entry's* content and read such a table correctly, so
nothing would have failed -- but an engine that prunes on manifest content would apply no
deletes at all. `zamboni/testing.py` supplies the missing writer, and a test asserts every
manifest's content agrees with the files inside it.

### Query it from the DuckDB CLI

The demo writes real Iceberg tables, so anything that reads Iceberg reads them -- including
`duckdb` from a shell, with no Zamboni in the picture at all. That is worth seeing: it is
what makes maintenance *worth* doing, and it is how you check that a compaction changed
file counts and not answers.

**Ask the catalog which metadata is current.** This is the step to do properly, because it
is the entire reason a catalog exists: it holds the pointer to the table's current
metadata, and every writer updates it atomically. The demo's catalog is SQLite, so a
one-liner reads it:

```console
$ uv run python -c "
from pyiceberg.catalog.sql import SqlCatalog
c = SqlCatalog('healthims', uri='sqlite:///data/healthims/iceberg_catalog.db',
               warehouse='file://data/healthims/iceberg_warehouse')
print(c.load_table('healthims.hims_events').metadata_location)"
file:///.../data/healthims/iceberg_warehouse/healthims/hims_events/metadata/00065-e4147334-....metadata.json
```

**Then hand that to DuckDB.** `iceberg_scan()` takes the `file://` URI as it comes:

```console
$ duckdb
INSTALL iceberg; LOAD iceberg;

CREATE VIEW hims_events AS SELECT * FROM iceberg_scan(
  'file:///.../hims_events/metadata/00065-e4147334-....metadata.json');
```

Getting the DuckDB CLI: `curl https://install.duckdb.org | sh`, or `brew install duckdb`.
Note that `pip install duckdb` and `pipx install duckdb` give you the Python *library* --
the wheel ships no console script, so neither puts a `duckdb` command on your PATH.

**Daily EVS turnaround** -- how long a room sits between the patient leaving and the bed
being ready. Environmental Services cleans the room, and the metric is the gap between two
events sharing a `process_id`, the second of which arrives *after* the discharge process
has already reached its terminal state:

```sql
WITH out AS (
    SELECT process_id, occurred_at AS left_at FROM hims_events
    WHERE event_name = 'patient_displaced_discharged'
), clean AS (
    SELECT process_id, occurred_at AS ready_at FROM hims_events
    WHERE event_name = 'bed_cleaned'
)
SELECT CAST(o.left_at AS DATE)                                    AS day,
       COUNT(*)                                                   AS rooms,
       ROUND(AVG(date_diff('minute', o.left_at, c.ready_at)), 1)  AS avg_turnaround_min,
       MAX(date_diff('minute', o.left_at, c.ready_at))            AS worst_min
FROM out o JOIN clean c USING (process_id)
GROUP BY 1 ORDER BY 1;
```

```
┌────────────┬───────┬────────────────────┬───────────┐
│    day     │ rooms │ avg_turnaround_min │ worst_min │
├────────────┼───────┼────────────────────┼───────────┤
│ 2026-01-05 │     7 │               77.7 │       104 │
│ 2026-01-06 │     9 │              104.3 │       140 │
│ 2026-01-07 │     9 │               85.9 │       115 │
│ 2026-01-08 │    10 │               92.6 │       113 │
│ 2026-01-09 │     9 │               75.2 │       100 │
└────────────┴───────┴────────────────────┴───────────┘
```

The SQL is the demo's own -- `src/himsdemo/queries.py` runs this exact statement -- so the
README and the demo cannot drift into computing different things under one name.

**Run it before and after `./bin/zamboni-demo maintenance`.** The numbers do not move. Measured on
the five-day demo: `hims_events` went from **60 live data files to 5**, and the output
above was byte-identical either side, once through the pre-maintenance metadata pointer and
once through the new one. That is the whole claim in one comparison -- compaction, expiry
and orphan removal change how the data is stored and never what it says. `./bin/zamboni-demo query`
runs this and three others for exactly that reason, and reports "files scanned" alongside,
which *does* move.

**Skipping the pointer lookup.** If you would rather not ask the catalog, DuckDB can find
the newest metadata itself, but it makes you say so:

```sql
SET unsafe_enable_version_guessing = true;
CREATE VIEW hims_events AS SELECT * FROM
  iceberg_scan('data/healthims/iceberg_warehouse/healthims/hims_events');
```

The refusal is a good one and the setting is named honestly: globbing the metadata
directory can pick up a file a writer has not committed yet. It is fine against this demo
warehouse, which nothing else is writing to, and it is not how you should read a live
table.

**Against a REST catalog** -- `./bin/zamboni-demo --catalog lakekeeper` -- none of this applies:
DuckDB attaches the catalog with `ATTACH ... (TYPE ICEBERG)` and resolves tables by name,
which is what a real deployment does.

Requirements and domain model: [data/healthims/Demo_Requirements.md](data/healthims/Demo_Requirements.md).
Event catalogue: [data/healthims/HIMS_Discharge_Process_Events.md](data/healthims/HIMS_Discharge_Process_Events.md).

## CI

[.github/workflows/ci.yml](.github/workflows/ci.yml) defines five jobs on push and pull
request:

| Job | What it guards |
|---|---|
| `lint` | ruff check and format; mypy over `src` and `scripts`; `uv sync --frozen` fails on a stale lockfile; pre-commit and CI must pin the same ruff; every source file carries its SPDX tag |
| `test` | The suite on Python **3.11 and 3.13** — the floor `pyproject.toml` claims and the version pinned for development |
| `executables` | `bin/` regenerates to a no-op, and both PEP 723 scripts run **from outside the project directory** |
| `spark` | Builds a Spark Connect server with the Iceberg runtime and S3A, then runs the live Spark tests against it |
| `dev-stack` | The real thing: brings up Lakekeeper + Postgres + MinIO from `.env.sample`, bootstraps it, runs the dev-stack tests, then the demo end to end |

Two jobs guard against a green tick that means nothing. `dev-stack` sets
`ZAMBONI_REQUIRE_DEV_STACK=1`, which turns "cannot reach the stack" from a skip into a
failure; `spark` selects its tests by marker and fails if any of them *skipped*, because
those fixtures skip on a closed port by design. Without both, a stack that never started
yields a suite of skips and a tick that means nothing was tested.

The `executables` job has already earned its place in advance: `bin/` was found stale
against three separate changes, which is exactly what that job exists to catch and exactly
what nothing caught while CI was not running.

Locally, [.pre-commit-config.yaml](.pre-commit-config.yaml) runs the fast checks on every
commit:

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

The full suite stays out of the hook deliberately — a four-minute hook gets bypassed, and a
bypassed hook is worse than none.

## Contributing, security, licence

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — the five conventions that make this codebase's
  claims trustworthy, each with the evidence that earned it. Read rule 1 before opening a
  pull request: verify a claim before making it.
- **[SECURITY.md](SECURITY.md)** — this tool deletes files, so the failure mode of a defect
  is somebody's data. Report anything that could delete a still-referenced file privately,
  and note that it does **not** need to be attacker-triggerable to count.
- **Licence: Apache-2.0** — the same licence as Iceberg and PyIceberg, so contributions
  flow both ways without friction. Every source file carries an SPDX tag; see
  [LICENSE](LICENSE).

## Documentation

**Start here: [docs/user_guide.md](docs/user_guide.md)** — the four ways to run
Zamboni (Python API, cron + CLI, Trino, Spark), a capability table to choose an
engine with, secrets handling, and the local engine's measured memory ceiling.

- **[docs/design.md](docs/design.md)** — high-level design: how Iceberg stores a table, how
  that grows, why each maintenance operation exists, architecture, sequence diagrams,
  constraints, and who owns what.
- **[docs/plan.md](docs/plan.md)** — delivery plan: scope, phasing, requirements
  traceability, verification approach, residual risk.
- **[docs/runbook.md](docs/runbook.md)** — when a maintenance cycle fails: exit codes,
  getting a stack trace out of cron, table status, a health check, and recovery.
- **[docs/runbook-dev.md](docs/runbook-dev.md)** — running each step by hand: the order
  and why each position matters, cadence arithmetic, sizing the orphan guard, the dev stack.
- **[docs/devops.md](docs/devops.md)** — running it in production: the cron line, `zamboni.yml`
  and `.env`, why there is no shell wrapper, and the multi-tenant layout for one
  warehouse per customer.
- **[docs/tasks.md](docs/tasks.md)** — the ZMBNI backlog: what is done, what is left, and
  what is deliberately not being done.
- **[docs/roadmap.md](docs/roadmap.md)** — the six features planned beyond `v0.1.0`, five of
  which shipped. Mostly a record now: what each was for, the evidence it was chosen on, and
  the one place the sequencing was wrong. The remaining item is PyIceberg 0.12, blocked
  upstream.
- **[docs/engine-comparison.md](docs/engine-comparison.md)** — what Zamboni, Trino and Spark
  each can do, and the twelve places where the same-sounding operation differs.
- **[docs/ice-keeper-comparison.md](docs/ice-keeper-comparison.md)** — against a deployed
  maintenance service: what to adopt, what to decline, and the defect the comparison found.
- **[docs/table-config.md](docs/table-config.md)** — the `table-config.json` specification.
- **[docs/live-verification.md](docs/live-verification.md)** — what runs against a real
  Lakekeeper + MinIO, what a remote-signing warehouse blocks, and the two bugs it found.
- **[docs/releasing.md](docs/releasing.md)** — what a version number promises, and the release
  checklist. Mostly the first half: for a tool that deletes files, a changed default is a
  breaking change with no signature moved, so the destructive defaults are public surface.
- **[CHANGELOG.md](CHANGELOG.md)** — notable changes, with `BREAKING` and `SAFETY` called out.
- **[examples/table-config.json](examples/table-config.json)** — worked example.

## Declarative table layout

Partitioning, partition ageing, sorting and z-order are declared per table in
`table-config.json` — see **[docs/table-config.md](docs/table-config.md)** for the full
specification and [examples/table-config.json](examples/table-config.json) for a worked
example. Analysts can author it inside the Meltano/Singer catalog under an `x-iceberg`
key and generate the file:

```bash
zamboni from-catalog .meltano/catalog.json --namespace analytics -o table-config.json
zamboni validate-config table-config.json
zamboni compact analytics.events --table-config table-config.json --yes
```

Days-to-months partition evolution is **on by default** (90 days); disable it fleet-wide in
`defaults.partition_evolution.enabled`, per table, or at generation time with
`from-catalog --no-evolution`.

> The catalog is a good authoring surface but a bad transport: the Singer SDK's typed
> `Metadata`/`Schema` dataclasses silently drop unknown keys on round-trip, verified
> against the SDK checkout. Meltano core keeps them (it edits raw dicts). Hence generate an
> artifact rather than reading the catalog at compaction time.

## What this does

`TableCompactor` reads a table's small files through PyIceberg's scan, rewrites them into
target-sized files with PyIceberg's own writer, and commits the swap as a single Iceberg
`replace` snapshot per partition.

```python
import os

from zamboni import CatalogSession, CompactionConfig, S3Settings, TableCompactor

session = CatalogSession.for_lakekeeper(
    uri="http://localhost:8181/catalog",
    warehouse="demo",
    # From the environment, or your secret manager -- never a literal, and never
    # a command-line flag. See docs/user_guide.md#secrets.
    credential=os.environ["ZAMBONI_CREDENTIAL"],
    oauth2_server_uri="http://localhost:30080/realms/iceberg/protocol/openid-connect/token",
    scope="lakekeeper",
    # Omit entirely when the catalog vends credentials, which is the arrangement
    # worth having: one revocable secret instead of long-lived object-store keys.
    s3=S3Settings(
        endpoint="http://localhost:9000",
        access_key_id=os.environ["ZAMBONI_S3_ACCESS_KEY_ID"],
        secret_access_key=os.environ["ZAMBONI_S3_SECRET_ACCESS_KEY"],
    ),
)

compactor = TableCompactor(session, "default.events", CompactionConfig())

print(compactor.describe().summary())   # read-only: layout, blockers, warnings
print(compactor.plan().describe())      # what would be rewritten, and what is skipped
print(compactor.execute().describe())   # do it
```

`execute(dry_run=True)` plans and logs without touching the table.

## Dev stack

A Lakekeeper + Postgres + MinIO stack, configured so reclamation works, lives in
[dev-stack/](dev-stack/):

```bash
cp dev-stack/.env.sample dev-stack/.env
cd dev-stack && docker compose up -d && uv run bootstrap.py

export ZAMBONI_URI=http://localhost:8182/catalog
export ZAMBONI_WAREHOUSE=zamboni
./bin/zamboni-demo --catalog lakekeeper next-day        # the demo, on Lakekeeper and MinIO
uv run pytest tests/test_dev_stack.py          # skipped when the stack is down
```

The same 13 checks can be pointed at a deployment this repo did not create:

```bash
uv run scripts/verify-live.py --port 8181 --warehouse acme_db \
    --s3-host 172.19.0.2 --s3-port 9000
```

A failure is the diagnosis. A warehouse that cannot reclaim storage fails
`test_the_warehouse_vends_credentials_rather_than_signing` by name.

Ports are shifted off the defaults so it coexists with anything else you are running.
The two non-obvious settings — why the warehouse needs `sts-enabled` and why its S3 endpoint
is the compose gateway rather than `minio` — are explained in
[dev-stack/README.md](dev-stack/README.md).

## Environment

Everything runs from a locked virtualenv, and nothing resolves against global site-packages.
That is not fastidiousness: PyIceberg's SQL-catalog extra pins SQLAlchemy, and a shared
site-packages is under no obligation to agree. A venv makes that a non-question rather than a
resolution problem.

```bash
uv sync            # builds .venv from uv.lock, Python pinned by .python-version
uv run pytest -q   # no Docker needed; the dev-stack tests skip when it is down
uv run ruff check src tests scripts
```

To move to newer dependencies deliberately:

```bash
uv sync --upgrade
uv run pytest -q
uv run scripts/build-executable.py   # keep the executable in step with the lock
```

### The `zamboni` executable

`bin/zamboni` is a single-file PEP 723 script with every dependency pinned from
`uv.lock` and a `#!/usr/bin/env -S uv run --script` shebang. uv builds and caches an
isolated environment for it on first run, so it works from any directory without an
activated venv and never picks up global packages:

```console
$ /path/to/Zamboni/bin/zamboni doctor
  pyiceberg                    0.11.1
  operation injectable         True
  REPLACE summary native       False
  streaming writes             False
  ...
usable: True
```

Regenerate it with `uv run scripts/build-executable.py` after any lock change. It pins
`requires-python` from `.python-version`, so the executable runs on the same interpreter
the tests do — without that pin uv resolves `>=3.11` to the newest interpreter present and
you ship on a Python you never tested.

```console
$ zamboni describe default.events --uri http://localhost:8181/catalog --warehouse demo
$ zamboni plan     default.events ...
$ zamboni compact  default.events ... --yes          # without --yes, every verb previews
$ zamboni expire   default.events ... --yes          # apply retention, delete what it orphans
$ zamboni remove-orphans default.events ... --yes    # sweep unreferenced files
$ zamboni remove-dangling-deletes default.events ... --yes   # drop deletes that apply to nothing
$ zamboni rewrite-manifests default.events ... --yes  # regroup manifests by partition
$ zamboni apply-properties default.events ... --yes   # metadata-retention table properties
$ zamboni engines                                     # what each engine supports, and refuses
```

For daily operation none of that is the interface. One command runs the six in the right
order over every configured table:

```console
$ zamboni maintenance --warehouse acme --status --yes
```

With `./zamboni.yml` and `./.env` present that is the whole cron line — see
**[docs/devops.md](docs/devops.md)**, which also covers why there is deliberately no shell
wrapper and how a multi-tenant fleet is scheduled.

Each mutating verb takes `--engine` (default `local`, the PyIceberg one).

**Spark works** — all six operations, over the Iceberg Spark procedures, verified against
Spark 4.0.4:

```bash
pip install "iceberg-zamboni[spark]"             # the Connect client: ~13MB, no JVM
zamboni compact default.events --engine spark --spark-remote sc://localhost:15002 --yes
```

**Trino works** for five of the six — it cannot remove dangling deletes:

```bash
pip install "iceberg-zamboni[trino]"
zamboni compact default.events --engine trino --trino-host localhost --yes
```

Spark comes first because it is the more complete of the two: it Z-orders and Trino does
not, so on Trino only your leading `sorted_by` column gets file skipping. `zamboni engines`
reports exactly what each one does and refuses, which is worth reading before planning a
migration. The `--yes` rule holds on every engine: where one cannot preview an operation, a
run without `--yes` is *refused* rather than executed or dressed up as a dry run it did not
perform.

`expire` and `remove-orphans` are dry-run without `--yes`, like `compact`. Both take
`--table-config` for the [`retention`](docs/table-config.md#retention) block, and both
accept overrides (`--max-snapshot-age-days`, `--min-snapshots-to-keep`,
`--older-than-days`) for one-off runs.

Catalog and S3 settings also read from `ZAMBONI_*` environment variables, so the same
invocation works from a shell, a cron entry, or a container.

## Memory

`MemoryMode.CHUNKED` bounds peak memory to roughly one output file. On a build without
streaming writes the bin-packing happens here before calling PyIceberg's writer; on a build
with them (`streaming_write_supported`) the record-batch reader is handed straight to
PyIceberg for unpartitioned tables, which bin-packs it itself. Partitioned tables always
bin-pack locally — partitioned streaming is apache/iceberg-python#2152.

When `sort_expression` is set the stream is routed through DuckDB, whose `ORDER BY` spills
to `temp_directory` on disk rather than holding the group in memory.

`MemoryMode.AUTO` (the default) picks `IN_MEMORY` for groups under `memory_budget_bytes`
(256MiB) and `CHUNKED` above it. CHUNKED reads **one data file at a time**, which is what
bounds it: peak memory is set by the largest file rather than by the group, so a partition
larger than RAM still compacts. It costs about 1.5x on read, which is why small groups stay
on the materialising path. See [docs/user_guide.md](docs/user_guide.md) for the numbers.

## Limitations

### Format versions

| | V1 | V2 | V3 |
|---|---|---|---|
| This package | **blocked** | yes | yes |
| PyIceberg metadata | yes | yes | yes (deletion vectors, puffin) |
| DuckDB Iceberg writes | rejected | yes | yes |
| `iceberg_rewrite_data_files` (unreleased) | n/a | yes | **rejected** |

V1 has no sequence numbers or row-level deletes and DuckDB cannot write it at all; upgrade
to V2 first. Note the inversion: the native DuckDB compactor refuses V3, so this routine
covers strictly more.

### Merge-on-read

| Delete kind | Status |
|---|---|
| Copy-on-write | supported |
| Position deletes | supported and tested — materialised on read, new files get a higher sequence number so the old deletes no longer apply |
| Deletion vectors (V3) | supported via the same position-delete path |
| Equality deletes | **blocked** while `equality_deletes_readable` is false — `DataScan._plan_files_local` raises `ValueError("PyIceberg does not yet support equality deletes")`. The blocker lifts automatically when a build supports them. |

A merge-on-read table's manifest `record_count` is the file's *physical* row count and so
overstates the live count by exactly the deleted rows. The row-preservation check therefore
compares against a delete-aware count (`RewriteOutput.source_live_rows`), computed the way
`DataScan.count()` does: `record_count` for delete-free tasks, an actual read for the rest.
Comparing against `record_count` would reject every correct MoR rewrite.

Delete files are left referenced after compaction. `_OverwriteFiles._deleted_entries` only
marks `DataFileContent.DATA` entries deleted. This is correct but adds metadata that only
snapshot expiry will clear; the count is reported as `CompactionResult.dangling_delete_files`.

### Partitioning

- **Partition-spec evolution** is handled by splitting: each `(spec_id, partition)` becomes
  its own group and its own commit. Files are never mixed across specs. (The native DuckDB
  compactor refuses such tables outright.)
- **All transforms work**, including `bucket`. Rewrites go through
  `_dataframe_to_data_files`, which derives the partition key from the data. The
  `add_files` route cannot do this — it infers partition values from column statistics and
  raises for any transform where `preserves_order` is false.
- **Directory layout** follows whatever PyIceberg's location provider produces for the
  written partition key, so `key=value/` layout is preserved. (The native compactor writes
  compacted files flat into the data path.)

### Sort, clustering, z-order

Declared as `ordering.mode` in table-config (`sort` | `zorder` | `none`), or with the CLI
flags below. Z-order is implemented here — dense-rank each key, scale to `precision_bits`,
bit-interleave — because nothing in Iceberg, PyIceberg or duckdb-iceberg has any notion of
it. The SQL is checked against an independent Morton-encoding reference, and against the
query cost it exists to reduce: blocks touched by a filter on the *second* key.

Two mutually exclusive sort options, differing in whether the result can honestly be
labelled:

- `sort_by_table_order=True` orders by the table's **declared** sort order and stamps that
  order's id onto the output files. Truthful by construction. Only identity-transform sort
  fields can be rendered to SQL; a bucket or truncate sort field is refused rather than
  approximated, because ordering by the raw column would not satisfy the order whose id we
  are about to write.
- `sort_expression="..."` applies an arbitrary DuckDB `ORDER BY` and leaves
  `sort_order_id = None`. The rows are ordered, but not by any order the table declares.

Stamping the table's id for an arbitrary expression would be the same defect the native
duckdb-iceberg compactor has: it stamps `sort_order_id` unconditionally while emitting no
`ORDER BY` at all, so its metadata asserts an ordering the data does not have. PyIceberg
errs the other way and hardcodes `sort_order_id=None` on every file it writes.

Iceberg has no clustering concept distinct from sort order, and neither PyIceberg nor
duckdb-iceberg has z-order or Hilbert curves anywhere. Express a z-order as a
bit-interleaving expression in `sort_expression`.

## Why PyIceberg is capped at `<0.12`

`pyproject.toml` pins `pyiceberg[pyarrow]>=0.11.1,<0.12`. **0.11.x is fully supported**;
the cap is a held position while the 0.12 release candidates are tested, not a judgement
about 0.12.

Zamboni is tested against each 0.12 release candidate as it appears. Issues found are
reported upstream and fixed; that is ordinary, and it is how a cap gets lifted rather than
a reason to keep one. Currently open:

| Issue | What it is | Fix |
|---|---|---|
| [iceberg-python#3758](https://github.com/apache/iceberg-python/issues/3758) | `upsert` on a *partitioned* table keeps the replaced row beside its replacement and duplicates an untouched one, silently. Overwrite's new manifest pruning keeps a non-matching manifest verbatim, including the entries being deleted | [#3780](https://github.com/apache/iceberg-python/pull/3780), open |

The issue and its PR are the source of detail; there is no second copy here. Our own
reproduction and the mechanism are in
[docs/upstream-0.12-upsert-regression.md](docs/upstream-0.12-upsert-regression.md).

Two things worth being clear about:

- **This is an ingest hazard, not a maintenance one.** Zamboni's own operations pass on
  0.12 -- what the defect reaches is any *write* path going through overwrite on a
  partitioned table, which is most merge-style ingestion. The cap protects your ingest.
- **The cap is deliberate, not staleness.** The original bound was open-ended, which meant
  the day 0.12 published, any `uv lock --upgrade` would have pulled it in with nobody
  touching this code.

When 0.12 releases and the suite passes against it, the supported range will include it.

Note the capability probes do not catch a defect like this by design: they answer "can this
build do X", and such a build *can* upsert -- it simply does it wrongly. Proving
correctness means writing data and reading it back, which is a test, not a probe. Where a
probe genuinely must ask about behaviour rather than structure, it runs the operation --
see [Capability detection](#capability-detection-not-version-checks).

## Capability detection, not version checks

Every version-dependent decision routes through `capabilities.detect()`, which probes the
installed PyIceberg structurally — does this function exist, what does this signature
accept, what does this source say. `zamboni doctor` prints the result.

This is not defensive over-engineering; it is the shape of the problem. PyIceberg 0.11.1
is the current release, and unreleased main (heading for 0.12) already changes three of the
six probes:

| Probe | 0.11.1 | main |
|---|---|---|
| `operation` injectable into `_SnapshotProducer` | yes | yes |
| `update_snapshot_summaries` accepts `REPLACE` | no | no |
| `_dataframe_to_data_files` takes a `RecordBatchReader` | no | **yes** |
| `_existing_manifests` prunes by predicate | no | **yes** |
| producer derives the delete predicate | no | **yes** |
| equality deletes readable | no | no |

Concretely, on a 0.12 build this package will automatically hand PyIceberg the record-batch
stream instead of bin-packing itself, and the equality-delete blocker will lift on its own
the day scan planning supports them. A version comparison would have had to be revisited by
hand for each of those.

The probes also gate safety. `manifest_pruning_is_safe` requires the delete-predicate
derivation whenever predicate pruning is on; a build with pruning and no derivation would
keep manifests holding removed files verbatim and count their rows twice, so the tool
refuses to run rather than corrupt a table.

## Private PyIceberg APIs

Two are load-bearing, and `committer.assert_supported_pyiceberg()` fails loudly if either
moves:

1. **`Operation.REPLACE`.** `replace` is the spec's operation for compaction and it is not
   cosmetic — incremental and CDC readers use it to decide whether a snapshot changed any
   rows. PyIceberg cannot emit one: `UpdateSnapshot.overwrite()` hardcodes `OVERWRITE`, and
   `update_snapshot_summaries` rejects anything outside `{APPEND, OVERWRITE, DELETE}` in
   0.11.1 *and* on current main. `_ReplaceFiles` lets PyIceberg compute the summary totals
   as an overwrite and relabels the finished summary. Set
   `CompactionConfig(snapshot_operation="overwrite")` to avoid the subclass entirely.
2. **`_OverwriteFiles._existing_manifests`.** In 0.11.1 it scans every manifest and rewrites
   any containing a removed file. Later versions added a partition-predicate manifest
   evaluator plus a `_build_delete_files_partition_predicate` step that derives the
   predicate from the removed files. Both are correct, but a version with the evaluator and
   *without* the derivation would keep those manifests verbatim and double-count their rows.
   The guard refuses to run on such a build.

Removed files are passed as the `DataFile` objects read from the manifests, never
reconstructed, because that method matches them with `entry.data_file in
self._deleted_data_files`.

## Why not Daft, Ray, or Dask

- **Dask** has no usable Iceberg integration — `dask-iceberg` and `daskberg` are alpha and
  read-only.
- **Ray**: `pyiceberg.Table.to_ray()` is `ray.data.from_arrow(self.to_arrow())`, which
  materialises the whole table before Ray sees it. `ray.data.read_iceberg` /
  `write_iceberg` avoid that but the write API is documented as alpha, and a cluster
  runtime buys nothing on a single-node MinIO deployment.
- **Daft** was kept as a candidate backend for one reason: its writes support every
  partition transform including `bucket`. That advantage turned out not to exist —
  `_dataframe_to_data_files` plus `pyiceberg-core` handles bucket partitioning here, with a
  test proving it. Daft also cannot read equality deletes and has no `replace`-snapshot
  primitive, so no Daft backend is shipped.

### Partition evolution

`partition_evolution` condenses aged fine-grained partitions into coarser ones (the
days-to-months case). New data keeps landing in the fine-grained spec: the coarse spec is
added with `AddPartitionSpecUpdate` **without** `SetDefaultSpecUpdate`.

The trap: PyIceberg's snapshot producer hardcodes the added manifest to the table's
*default* spec while grouping deleted entries by each file's own spec, so month files land
in a day-spec manifest — metadata that reads correctly until a predicate prunes on it.
`MultiSpecReplaceFiles` corrects the asymmetry, and `test_evolution.py` asserts
manifest/file spec agreement rather than only row counts.

Compound specs evolve when exactly one field matches the rule's granularity: that field coarsens, the others carry through, and grouping keys on the whole output partition. Two fields of the same granularity are skipped -- which one dates the partition is ambiguous.

## Reclaiming storage

Compaction frees nothing on its own, so two sibling operations do the reclaiming.

**`expire`** implements the Iceberg spec's retention algorithm -- branch and tag heads,
per-branch ancestry, `min-snapshots-to-keep` winning over age, `main` never expiring --
because PyIceberg implements almost none of it and deletes no files either way. It then
deletes exactly the files reachable before the commit and not after. Computing that
difference, rather than listing storage, is what makes it safe: a file no snapshot ever
referenced cannot appear in the diff, so expiry structurally cannot touch a concurrent
writer's in-flight output.

**`remove-orphans`** does look at raw storage, which is why it is the more dangerous of
the two and why it is fenced:

- Storage is listed **before** the reachable set is computed. A file committed between the
  two steps lands in both and survives; the reverse order would delete it.
- A default **3-day age guard** on file mtime, sized to the longest write the warehouse
  performs -- here that is compaction, not ingest.
- Scope is the table's own location plus any `write.data.path` / `write.metadata.path`,
  never a warehouse-wide sweep.
- The run **aborts** if any referenced file is missing from the listing (the listing is
  partial, so its complement means nothing), or if any of the data / manifest /
  manifest-list / metadata reference categories comes back empty on a table that has
  snapshots. `tests/test_orphans.py` monkeypatches each category away in turn and asserts
  nothing is deleted.

**`remove-dangling-deletes`** drops delete files that no longer apply to any live data
file. Compaction creates them by construction: it applies the deletes as it rewrites, and
the new data files carry a higher sequence number, so the old delete files stop applying —
but `_OverwriteFiles._deleted_entries` filters entries to `DataFileContent.DATA`, so
PyIceberg cannot remove them. It is metadata-only and cannot change a query result, because
a dangling delete is one no scan would have applied; the applicability question is answered
by PyIceberg's own `DeleteFileIndex` rather than by a second implementation of the rule.

Removal is limited to **whole delete manifests**, because `ManifestWriterV2` hardcodes
`content()` to `DATA` and there is no delete-manifest writer in PyIceberg. A partially
dangling manifest is reported and left alone; rewriting it would label position deletes as
data. On the demo's merge-on-read run this clears all 32 dangling deletes and takes the
table from 57 manifests to 3.

Both reclaim verbs read one referenced-file set (`reachable.py`) covering all six places Iceberg
metadata points at files -- data and delete files, manifests, manifest lists, metadata
JSON, statistics, and partition statistics. `max-ref-age-ms` is detected and reported but
applied when configured: the ref is dropped so its snapshots can expire. Off by default,
because removing a named tag or branch destroys metadata someone chose to create.

## Manifest rewriting

A query prunes manifests first, using the partition bounds each one records, then data
files. Streaming ingest defeats the first stage: every commit writes one manifest holding
whatever that batch touched, so after a few hundred appends every manifest spans every
partition and the planner opens all of them. Compaction does not fix it — it rewrites data
files and leaves the manifest set alone.

`rewrite-manifests` regroups live entries by partition, in partition order, bin-packed to
`write.manifest.target-size-bytes`. Contiguity is the point: a manifest holding a
consecutive *range* of partitions still prunes, while one holding a scattering of them has
bounds spanning the table. Sequence numbers and `snapshot_id` are preserved exactly —
reassigning a sequence number would silently change which delete files apply, which reads
correctly today and goes wrong later.

PyIceberg has no `rewrite_manifests`. `_ManifestMergeManager` merges by size at append time
and groups only by spec, so it makes manifests bigger without making them prunable.

## metadata.json retention

Two table properties, and the surprising one is measured in
[docs/table-config.md](docs/table-config.md#retention): `write.metadata.previous-versions-max`
trims the metadata *log*, not the files behind it, so setting it alone manufactures orphans
rather than reclaiming bytes. `apply-properties` sets what `table-config.json` declares and
says how many metadata files are already stranded.

## Format version 3

**Blocked for compaction.** V3 carries row lineage: a rewritten row must keep its original
`_row_id` and `_last_updated_sequence_number`. Compaction reads rows through the scan and
writes them with PyIceberg's writer, neither of which carries those fields, so it would
silently reassign them.

PyIceberg 0.11.1 cannot serialise V3 metadata at all — `TableMetadataV3.model_dump_json`
raises — which makes this fail loudly on a SQL catalog. But a REST catalog builds metadata
server-side and never calls it, so nothing upstream would stop it there. Hence an explicit
blocker rather than reliance on the upstream one.

Metadata-only maintenance rewrites no rows and is unaffected: it copies manifest entries
verbatim, `first_row_id` included.

## Not in scope

Rewriting a *partially* dangling delete manifest, and splitting a single partition across
manifests. Both are limited by PyIceberg having no delete-manifest writer; both lift
automatically if one appears, and `zamboni doctor` probes for it rather than assuming.
