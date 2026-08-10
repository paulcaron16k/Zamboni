# User guide

Iceberg tables need maintenance. Writers create snapshots and never clean up
after themselves; streaming ingest leaves small files; failed jobs leave files
nothing references. None of that is a bug — it is the cost of the optimistic
concurrency that lets many writers share a table safely. Somebody has to sweep.

Zamboni sweeps. What makes it unusual is that it can do most of the job with
**no query engine at all**: no Trino, no Spark, no JVM, no cluster. It can also
drive Trino or Spark when you have one, through the same configuration file and
the same commands.

This guide covers four ways to run it. Pick one:

| You are | Read |
|---|---|
| Embedding maintenance in a Python service, probably across many customer warehouses | [Mode 1: the Python API](#mode-1-the-python-api) |
| One machine, a crontab, and tables you want to stop worrying about | [Mode 2: cron and the CLI](#mode-2-cron-and-the-cli) |
| Already running Trino, or willing to start one for the night | [Mode 3: Trino from cron](#mode-3-trino-from-cron) |
| Already running Spark, or you need Z-order at cluster scale | [Mode 4: Spark from cron](#mode-4-spark-from-cron) |

Before any of them, [Secrets](#secrets) — the four shapes leak in different
places, and getting it right in one does not get it right in another.

Two audiences run through all of it. **Administrators** operating production
warehouses will care about secrets handling, per-customer isolation, exit codes
and the memory ceiling. **Home gamers** — one warehouse, a NAS or a cheap VPS,
no cluster and no wish for one — want a single cron line that is safe to leave
alone. Both are first-class here; where the advice differs, it says so.

---

## What each engine can actually do

The most important decision is which engine performs the work, and it is mostly
decided by one row of this table.

| Capability | `local` | `trino` | `spark` |
|---|---|---|---|
| **Z-order clustering** | **yes** | **no** | **yes** |
| Compaction (small files → target size) | yes | yes | yes |
| Sort by declared columns | yes | leading column only¹ | yes |
| Control output file size | yes | **no**² | yes |
| Partition evolution (day → month for old data) | **yes** | no | no |
| Snapshot expiry | yes | yes³ | yes³ |
| Orphan-file removal | yes⁴ | yes⁵ | yes⁵ ⁶ |
| Dangling-delete removal | partial⁷ | **no**⁸ | yes |
| Manifest rewriting | yes | yes⁹ | yes |
| Metadata retention properties | yes | yes¹⁰ | yes |
| Preview before committing (no `--yes`) | **every operation** | none | orphans only |
| Needs a JVM | no | server-side | server-side |
| Needs a cluster | no | yes | yes |

¹ Trino sorts by identity transforms only, and stamps `sort_order_id=unsorted`
when it skips any. ² `file_size_threshold` chooses which files to merge; there
is no output-size control. ³ No `max_ref_age_days`, so the spec's retention step
2 is unavailable. ⁴ Needs a bucket listing, which a remote-signing warehouse
refuses. ⁵ Minimum retention floors apply: Trino's is configurable and defaults
to 7 days, Spark's 24 hours is hard-coded. ⁶ Lists through Hadoop S3A, so it
needs its own credentials — see [Mode 4](#mode-4-spark-from-cron). ⁷ Drops whole
delete manifests only. ⁸ No procedure and no side effect achieves it; run this
one elsewhere. ⁹ No size control. ¹⁰ Translated to Trino's own property names.

`zamboni engines` prints this from the code, with the reason attached to every
limitation. **When this table and that command disagree, the command is right** —
the table above is written by hand and the command is not:

```console
$ zamboni engines
  engine: trino
  compact                  partial      cannot preview
      - no Z-order. Verified against the connector source: zero occurrences of
        zorder/z-order/morton/interleave in all 474 files, and no open issue
        proposing it. Only the leading sort column gets file skipping, so a
        filter on any other column reads every file in every surviving partition
  ...
  layout: sort
```

That `layout:` line is what `zamboni table-config summary` consults when it
warns you that a Z-order you just configured will do nothing on the engine you
chose. Nothing about that warning is written twice.

### Reading the table if you are a home gamer

**Z-order is the row that matters, and `local` has it.** If your queries filter
on more than one column — `WHERE customer = ? AND day = ?` — Z-order is what
lets Iceberg skip files instead of reading the partition. A single-column sort
helps the first column and does nothing for the second. Trino, the engine you
might have assumed was the serious choice, cannot do this at all.

So the honest summary for a small deployment is: **run `local`.** You get every
operation, Z-order, partition evolution, and a preview of every single one
before it touches anything. You give up throughput, which you do not have a
cluster to spend anyway. The one thing to understand before trusting it
unattended is memory — see [the local engine's limits](#the-local-engines-limits).

### Reading the table if you are an administrator

`local` is not merely the fallback. It is the only engine with partition
evolution, the only one that previews every operation, and one of two with
Z-order. Its ceiling is memory, and that ceiling is measured below rather than
asserted.

Trino's gap is real and structural: no Z-order, no dangling-delete removal, no
output-size control. Spark is the most capable engine but the most expensive to
operate. A mixed deployment is legitimate and expected — compaction on Spark,
partition evolution on `local` — because every engine reads the same
`table-config.json`.

---

## The configuration file

All four modes share one file. `table-config.json` says what each table's
layout should be and what maintenance may do to it. It is the same idea as
ice-keeper's table properties, with one deliberate difference: it lives in a
file you can review, diff and put under version control, rather than in the
tables themselves.

Three commands manage it:

```bash
zamboni table-config generate -o table-config.json     # describe the catalog as it is today
zamboni table-config summary table-config.json         # what would this do, including defaults
zamboni table-config validate table-config.json        # does it parse and mean something
```

`generate` is deliberately **descriptive, not aspirational**: it writes out each
table's *current* partition spec with default retention, so the first run
against it changes nothing but file sizes. Edit it to say what you want, then
diff.

`summary` is the one to run before a cron job goes live. `validate` says the
file parses; `summary` says what it will *do*, and marks every value you did not
write:

```console
$ zamboni table-config summary table-config.json
acme.events
  layout     partition [ts:day], ordering zorder
             zorder columns customer_id, event_type  -- local and spark only; trino cannot do this
  expire     on, keep 7 day(s), minimum 2 snapshot(s)
  orphans    on, files older than 3 (default) day(s) -- this deletes storage; the guard is what
             stands between it and a live write
  dangling   on
  manifests  on
  metadata   keep 3 previous version(s), delete after commit True
  evolution  day->month after 90d  -- local engine only
```

Note what it tells you that the file does not: which values are defaults, that
the Z-order you configured will be silently skipped on Trino, and that
`evolution` only happens on `local`. The full schema is in
[table-config.md](table-config.md).

## Secrets

One page, because the four deployment shapes below leak in different places and
getting it right in one does not get it right in another.

**The rule everywhere:** non-secret configuration in `zamboni.yml`, credentials
supplied by whatever already manages secrets on that platform, and **never on a
command line**.

```
zamboni.yml     catalog URI, warehouse, engine, which operations run   → commit this
.env            tokens, S3 keys                                        → chmod 600, never commit
```

### The one that catches everyone: argv

`--token`, `--credential` and `--s3-secret-access-key` accept a value directly,
and **anything on a command line is readable by every local user** — from
`ps aux`, from `/proc/<pid>/cmdline` — for as long as the process runs, and it
stays in your shell history afterwards. Verified, not assumed: a token passed
that way was read straight back out of `/proc`.

Zamboni warns when you do it:

```console
$ zamboni compact acme.events --token "$TOKEN"
warning: --token put a secret on the command line, where any local user can read it
from `ps` or /proc/<pid>/cmdline, and where your shell history keeps it. Use the
matching ZAMBONI_* variable, or a .env file -- see docs/user_guide.md.
```

The flags exist because a one-off interactive run is a legitimate use, so this
is a warning rather than a refusal. In anything automated, use the environment.

`--s3-access-key-id` is deliberately **not** warned about: a key id is an
identifier, not a secret, and warning on it would train you to ignore the
warning for the flag beside it that is.

### Reduce the number of secrets first

Before deciding how to store them, arrange to need fewer. With a
credential-vending catalog (Lakekeeper with `sts-enabled: true`), **the only
secret Zamboni needs is the catalog token** — S3 credentials are vended per
table, per run, and never stored. That is one revocable secret instead of a set
of long-lived object-store keys, and it is worth configuring deliberately.

The exception is Spark's `remove-orphans`, which lists through Hadoop S3A and
needs static keys **on the Spark server** — see
[Mode 4](#mode-4-spark-from-cron). Zamboni cannot supply them and cannot see
them.

### 1. Cron and the CLI → a `.env` file, mode 600

Cron gives a job almost no environment, which is exactly why the dotenv file
exists. A crontab is the wrong place: it is world-readable on many systems and
gets committed by accident.

```bash
install -m 600 /dev/null /srv/zamboni/.env
cat >> /srv/zamboni/.env <<'EOF'
ZAMBONI_URI=https://catalog.example.com/catalog
ZAMBONI_WAREHOUSE=acme
ZAMBONI_TOKEN=...
EOF
```

Zamboni reads `./.env` from the working directory, then `$ZAMBONI_ROOT`, and
**warns if the file is readable by group or other**:

```console
warning: /srv/zamboni/.env is readable by group or other (mode 644); it holds
credentials. chmod 600 it.
```

Real environment variables beat the file, deliberately: a systemd unit or
container that injects secrets properly should not be overridden by a stale
`.env` somebody left in the working directory. So `LoadCredential=` or
`EnvironmentFile=` under systemd works and needs no dotenv file at all.

### 2. In-app, calling the Python API → the caller passes them

Nothing is read from a file implicitly. `CatalogSession.for_lakekeeper` takes
`token=` / `credential=` as arguments, so the secret comes from wherever your
application already gets secrets — a vault client, a mounted file, the
environment — and Zamboni never decides.

```python
session = CatalogSession.for_lakekeeper(
    uri=settings.catalog_uri,
    warehouse=warehouse,
    token=vault.read(f"zamboni/{warehouse}").token,   # your secret manager
)
```

Two things worth knowing:

- **`S3Settings` redacts its secret in `repr()`.** A frozen dataclass otherwise
  prints every field, so the key would appear in any traceback rendered with
  locals, any `logger.debug("%s", settings)`, and any error aggregator. The key
  *id* is kept, because that is what you need when the answer is "wrong
  credentials".
- **Nothing in Zamboni logs a credential**, at any verbosity. If you wrap it in
  your own logging, do not log the session or the catalog properties —
  `as_properties()` necessarily contains the secret, because that is what gets
  sent.

For the multi-warehouse loop, keep one secret per warehouse rather than one
shared one — see [Mode 1](#mode-1-the-python-api). A shared token means a
compromise of any tenant's credentials is a compromise of all of them.

### 3. In-app, shelling out to the CLI → `env=`, never the argument list

If your application runs `zamboni` as a subprocess, pass credentials through the
child's environment. **Do not build them into the command.**

```python
import os, subprocess

# Right: the secret is in the environment, which /proc/<pid>/cmdline does not show.
subprocess.run(
    ["zamboni", "maintenance", "--warehouse", warehouse, "--yes"],
    env={
        **os.environ,
        "ZAMBONI_URI": catalog_uri,
        "ZAMBONI_TOKEN": token,
    },
    check=True,
)

# Wrong: readable by every local user for the life of the process.
subprocess.run(["zamboni", "maintenance", "--token", token, "--yes"])
```

Prefer a list argument to `subprocess.run` over `shell=True`. With a shell, the
command string is what gets executed and quoting mistakes become injection.

Note that `/proc/<pid>/environ` is readable by the *process owner*, so this
protects against other users rather than against someone who is already you.
That is the boundary that matters here; if it is not, the answer is a secrets
manager the child fetches from itself, not a different flag.

### 4. Airflow → Connections or a secrets backend, injected as env

Do **not** template a secret into a `BashOperator` command. Templated values are
rendered into the task's command line and into the Airflow UI's rendered-fields
view, and they land in the task log.

```python
# Right: the secret stays in Airflow's secrets backend and arrives as env.
BashOperator(
    task_id="maintain_acme",
    bash_command="cd /srv/zamboni && zamboni maintenance --warehouse acme --yes",
    env={
        "ZAMBONI_URI": "{{ conn.zamboni_catalog.host }}",
        "ZAMBONI_TOKEN": "{{ conn.zamboni_catalog.password }}",
    },
    append_env=True,
)

# Wrong: the token is rendered into the command, so it appears in the UI and log.
BashOperator(
    task_id="maintain_acme",
    bash_command="zamboni maintenance --token {{ conn.zamboni_catalog.password }} --yes",
)
```

Airflow masks values it knows are secret in task logs, which helps and is not a
substitute: masking is a display filter, and a secret in a command line has
already been handed to the OS.

For the Python API in a `@task`, fetch inside the task rather than at DAG-parse
time — the scheduler parses DAG files continuously, and a secret fetched at
module scope is fetched by the scheduler on every parse.

```python
@task
def maintain(warehouse: str) -> None:
    from airflow.models import Connection
    conn = Connection.get_connection_from_secrets("zamboni_catalog")
    session = CatalogSession.for_lakekeeper(
        uri=conn.host, warehouse=warehouse, token=conn.password
    )
    ...
```

If you run Airflow on Kubernetes, a mounted secret volume read inside the task
is better still: it is rotatable without touching the DAG.

### What to check on any deployment

- No secret appears in `ps aux` while a run is in progress.
- `.env`, if used, is mode 600 and owned by the account that runs the job.
- The job's log contains no credential — run one cycle with `--verbose` and
  look.
- The catalog token is scoped to what maintenance needs, and is revocable
  without redeploying.
- Per-warehouse rather than shared, if you maintain more than one tenant.

---

## Mode 1: the Python API

For a service that maintains many warehouses — one per customer, say — and
wants to schedule, report and alert with its own code.

### The public surface

Everything in `zamboni.__all__` is supported and covered by the compatibility
promise in [releasing.md](releasing.md). Anything else you can reach is
internal and may move in a patch release. The entry points you need:

| Object | For |
|---|---|
| `CatalogSession` | connecting: `for_lakekeeper`, `for_local`, `from_catalog` |
| `get_maintainer(name)` | the engine: `"local"`, `"trino"`, `"spark"` |
| `Operation` | the six operations, as an enum |
| `MaintenanceRequest` | engine-neutral inputs — retention plus overrides |
| `TableConfig` | loading and reading `table-config.json` |
| `available_engines()` | what this install can drive |
| `config_from_table_settings` | turning table-config layout into the compaction config `COMPACT` needs |
| `TableCompactor`, `SnapshotExpirer`, `OrphanCleaner`, … | the local engine's own classes, when you want its richer results |

A maintainer is engine-neutral: the same four lines drive `local`, `trino` or
`spark`, and each raises `UnsupportedOperation` rather than pretending when
asked for something it cannot do.

```python
from zamboni import (
    CatalogSession, MaintenanceRequest, Operation, TableConfig,
    config_from_table_settings, get_maintainer,
)

session = CatalogSession.for_lakekeeper(uri=..., warehouse=...)
config = TableConfig.load("table-config.json")
maintainer = get_maintainer("local")(session, {})

settings = config.for_table("acme.events")
request = MaintenanceRequest(
    retention=settings.retention,
    # `compact` needs this; the reclaim operations do not. Leaving it out
    # raises rather than quietly compacting without the ordering you declared.
    compaction=config_from_table_settings(settings),
)
result = maintainer.execute(Operation.COMPACT, "acme.events", request=request, dry_run=False)
print(result.describe())
```

`MaintenanceRequest` is engine-neutral by design: it carries *intent* — the
retention windows and the layout you want — and each maintainer translates it
into its own vocabulary. The same request object drives all three engines.

### Engine configuration

The second argument to a maintainer is its options dict. Each engine takes its
own keys, and passing another engine's keys is an error rather than a silent
no-op.

```python
# local: nothing. It is the process you are already in.
get_maintainer("local")(session, {})

# trino: an existing coordinator.
get_maintainer("trino")(session, {
    "host": "trino.internal", "port": "8080", "user": "zamboni",
    "catalog": "iceberg",
    "version": "483",        # gates retain_last, which needs Trino >= 479
})

# spark: a Spark Connect server. No JVM in this process.
get_maintainer("spark")(session, {
    "remote": "sc://spark.internal:15002",
    "catalog": "iceberg",
})
```

For Spark, install `zamboni[spark-connect]` (~1.5MB, pure Python) rather than
`zamboni[spark]` (~434MB and a JVM). The two are mutually exclusive: both
provide the `pyspark` package.

### Iterating customer warehouses

The shape that matters in a SaaS deployment: one warehouse per customer, one
config per warehouse, and a failure in one customer must not stop the others.

```python
import logging
import os
from pathlib import Path

from zamboni import (
    CatalogSession, MaintenanceRequest, Operation, TableConfig,
    UnsupportedOperation, config_from_table_settings, get_maintainer,
)

log = logging.getLogger("maintenance")

# Runbook order. The gaps between these are load-bearing -- see runbook.md.
ORDER = (
    Operation.COMPACT,
    Operation.REWRITE_MANIFESTS,
    Operation.REMOVE_DANGLING_DELETES,
    Operation.EXPIRE,
    Operation.REMOVE_ORPHANS,
    Operation.APPLY_PROPERTIES,
)

ROOT = Path(os.environ.get("ZAMBONI_ROOT", Path.home() / ".zamboni"))


def maintain(warehouse: str, engine: str = "local") -> list[str]:
    """One customer. Returns the failures rather than raising, so the caller
    can carry on to the next customer and report all of them at once."""
    failures = []
    # Per-customer config, per-customer credentials. See devops.md.
    config = TableConfig.load(ROOT / "configs" / warehouse / "table-config.json")
    session = CatalogSession.for_lakekeeper(
        uri=os.environ["ZAMBONI_URI"],
        warehouse=warehouse,
        token=os.environ[f"ZAMBONI_TOKEN_{warehouse.upper()}"],
    )
    try:
        maintainer = get_maintainer(engine)(session, {})
        for table in sorted(config.tables):
            settings = config.for_table(table)
            request = MaintenanceRequest(
                retention=settings.retention,
                compaction=config_from_table_settings(settings),
            )
            for operation in ORDER:
                try:
                    result = maintainer.execute(
                        operation, table, request=request, dry_run=False
                    )
                    log.info("%s %s %s: %s", warehouse, table, operation.value,
                             result.describe())
                except UnsupportedOperation as exc:
                    # Not a failure. This engine says it cannot, and says why.
                    log.info("%s %s %s: skipped -- %s", warehouse, table,
                             operation.value, exc)
                except Exception:
                    # One table's failure must not cost the other tables their
                    # maintenance, and one customer's must not cost the others.
                    log.exception("%s %s %s failed", warehouse, table, operation.value)
                    failures.append(f"{warehouse}/{table}/{operation.value}")
    finally:
        session.close()
    return failures


def main() -> int:
    failed = []
    for warehouse in sorted(p.name for p in (ROOT / "configs").iterdir() if p.is_dir()):
        failed += maintain(warehouse)
    if failed:
        log.error("%d operation(s) failed: %s", len(failed), ", ".join(failed))
        return 1
    return 0
```

Three things in there are deliberate and worth keeping:

- **`UnsupportedOperation` is caught separately from `Exception`.** It is an
  engine declaring a limit, not a failure. Treating it as an error would make
  every Trino run alert on dangling deletes forever.
- **One session per warehouse, closed in `finally`.** Sessions hold a DuckDB
  connection and a catalog client.
- **Failures are collected, not raised.** A customer whose catalog is
  unreachable at 02:00 should not stop the other forty from being maintained.

For a preview run — the thing to do first, and after any config change — pass
`dry_run=True`. On `local` every operation previews. On the other engines,
check `maintainer.capabilities().can_preview(operation)` first, or catch
`PreviewUnavailable`.

---

## Mode 2: cron and the CLI

The home-gamer path, and a perfectly good administrator path for a single
warehouse. No cluster, no JVM, one command.

```bash
pip install "zamboni[s3,sql]"
```

### Setup, once

```bash
cd /srv/zamboni
zamboni doctor                                     # is this PyIceberg build usable?
zamboni table-config generate -o table-config.json # describe the catalog as it is
$EDITOR table-config.json                          # say what you want instead
zamboni table-config summary table-config.json     # confirm what that means
```

Then preview everything. **Without `--yes`, nothing is committed** — every
mutating verb previews and says it is previewing. That is one rule with no
exceptions, and it is the reason this tool is safe to try.

```bash
zamboni maintenance --table-config table-config.json
```

### The cron line

```cron
17 3 * * * cd /srv/zamboni && /usr/local/bin/zamboni-nightly >> /var/log/zamboni/cron.log 2>&1
```

That points at a wrapper script rather than at `zamboni` directly, for exactly
two reasons: a dated log file (`date +%F` is a quoting trap inside a crontab)
and `2>&1`, because stderr is where a traceback goes. The wrapper must not loop
the verbs or the tables — `maintenance` owns that order and it is load-bearing.
[devops.md](devops.md) makes the fuller argument; [runbook.md](runbook.md)
covers reading the result when a cycle fails.

A plain cron line with `>> log 2>&1` is equally fine if you do not want the
dated file.

```bash
#!/usr/bin/env bash
# /usr/local/bin/zamboni-nightly
set -euo pipefail

cd /srv/zamboni
exec zamboni maintenance --table-config table-config.json --yes --verbose
```

`--yes` is what makes it commit. Leave it off for a week first: the preview
output tells you exactly what the real run would do, at no risk.

### Cadence

Compaction and expiry nightly is right for most tables. Orphan removal is the
expensive one — it lists your storage — and weekly is usually enough. The
`maintenance` verb runs all six in the correct order; if you want to split
them, `runbook.md` has the order and why each position matters.

There is no `--skip` flag. Which operations `maintenance` runs is a property of
the profile, so a split schedule uses two profiles rather than two flag sets:

```yaml
# nightly.yml -- everything except the storage listing
operations: [compact, apply-properties, remove-dangling-deletes, rewrite-manifests, expire]
```

```cron
17 3 * * 1-6 cd /srv/zamboni && zamboni --profile nightly.yml maintenance --yes
17 3 * * 0   cd /srv/zamboni && zamboni maintenance --yes
```

Or call the individual verbs, which is what `maintenance` does anyway:

```bash
zamboni compact acme.events --table-config table-config.json --yes
zamboni expire  acme.events --table-config table-config.json --yes
```

### Secrets

`.env` beside `zamboni.yml`, mode 600, and nothing in the crontab or the
wrapper. Zamboni warns if the file is group- or world-readable. The full
treatment — including why the `--token` flag exists and should not be used from
a script — is in [Secrets](#secrets).

---

## Mode 3: Trino from cron

Two very different situations share this mode, and the difference matters more
than the commands do.

**You have a corporate Trino.** It is not yours, it runs 24×7, and other
people's queries are on it. Zamboni just connects. The risk is that a
compaction lands on the same coordinator as somebody's dashboard at 09:00, so
schedule accordingly and talk to whoever owns it. Nothing needs starting or
stopping.

```cron
17 3 * * * cd /srv/zamboni && zamboni maintenance --table-config table-config.json \
             --engine trino --trino-host trino.corp --trino-port 8080 \
             --trino-user zamboni --trino-version 483 --yes
```

Set `--trino-version`. `retain_last` — our `min_snapshots_to_keep` — only
exists from Trino 479, and against an older server the setting cannot be
honoured. Told the version, Zamboni reports the loss; not told, it assumes the
older behaviour.

**You are starting a Trino for the night.** Transient Trino is a reasonable
trade: you get a real engine for the hour you need it and pay for nothing the
rest of the day. Remember what you are giving up — Trino cannot Z-order, cannot
remove dangling deletes, and cannot control output file size.

```bash
#!/usr/bin/env bash
# /usr/local/bin/zamboni-trino-nightly
#
# Start Trino, maintain, stop Trino. The trap is the point: a maintenance
# failure must still stop the container, or tomorrow's run finds the port taken
# and fails for a reason that has nothing to do with Iceberg.
set -euo pipefail

COMPOSE_DIR=/srv/zamboni/trino
LOG=/var/log/zamboni/trino-$(date +%F).log

cleanup() {
    echo "stopping trino" >&2
    docker compose -f "$COMPOSE_DIR/docker-compose.yaml" down --timeout 60 || true
}
trap cleanup EXIT INT TERM

docker compose -f "$COMPOSE_DIR/docker-compose.yaml" up -d --wait

# --wait returns when the container is healthy, which is *earlier* than Trino
# serving queries: the coordinator accepts connections before it can plan.
# Poll for a query that actually executes.
for attempt in $(seq 1 60); do
    if docker compose -f "$COMPOSE_DIR/docker-compose.yaml" exec -T trino \
           trino --execute "SELECT 1" >/dev/null 2>&1; then
        break
    fi
    [ "$attempt" = 60 ] && { echo "trino never became ready" >&2; exit 1; }
    sleep 5
done

cd /srv/zamboni
zamboni maintenance --table-config table-config.json \
    --engine trino --trino-host localhost --trino-port 8080 \
    --trino-user zamboni --trino-version 483 \
    --yes --verbose 2>&1 | tee -a "$LOG"
```

The `trap ... EXIT` is the part people leave out. Without it, a failure leaves
Trino running, and the next night's run fails on a bound port — a confusing
error a long way from its cause.

Trino needs its own catalog configuration, pointed at the same catalog Zamboni
uses. Enable vended credentials (`iceberg.rest-catalog.vended-credentials-enabled=true`,
which defaults to *false*) so Trino asks the catalog for per-table credentials
instead of holding its own S3 keys. A worked example is in
`dev-stack/trino/iceberg.properties`.

Because Trino cannot do everything, a realistic split is to run the operations
it is good at there, and the rest locally:

```bash
zamboni compact          --engine trino --table-config table-config.json --yes
zamboni expire           --engine trino --table-config table-config.json --yes
zamboni remove-dangling-deletes --table-config table-config.json --yes   # local: Trino cannot
```

---

## Mode 4: Spark from cron

The most capable engine, and the most work to operate. Choose it when you need
Z-order at a scale the local engine cannot hold in memory, or when you already
run Spark.

Use **Spark Connect**. The client is `pyspark-client` — about 1.5MB of pure
Python — and it starts no JVM, so the machine running cron needs no Java at
all. The alternative, a local `SparkSession`, starts a driver JVM in the cron
process and makes your Java version this tool's problem.

```bash
pip install "zamboni[spark-connect]"
```

```bash
#!/usr/bin/env bash
# /usr/local/bin/zamboni-spark-nightly
#
# Start a Spark Connect server, maintain, stop it.
set -euo pipefail

COMPOSE_DIR=/srv/zamboni/spark
REMOTE=sc://localhost:15002
LOG=/var/log/zamboni/spark-$(date +%F).log

cleanup() {
    echo "stopping spark" >&2
    docker compose -f "$COMPOSE_DIR/docker-compose.yaml" down --timeout 120 || true
}
trap cleanup EXIT INT TERM

docker compose -f "$COMPOSE_DIR/docker-compose.yaml" up -d --wait

# The gRPC port opens before the session can plan. Ask for something trivial
# through the client that will run the real work, so a mismatched client and
# server fails here rather than mid-compaction.
for attempt in $(seq 1 60); do
    if python -c "
from pyspark.sql import SparkSession
SparkSession.builder.remote('$REMOTE').getOrCreate().sql('SELECT 1').collect()
" >/dev/null 2>&1; then
        break
    fi
    [ "$attempt" = 60 ] && { echo "spark connect never became ready" >&2; exit 1; }
    sleep 5
done

cd /srv/zamboni
zamboni maintenance --table-config table-config.json \
    --engine spark --spark-remote "$REMOTE" --spark-catalog iceberg \
    --yes --verbose 2>&1 | tee -a "$LOG"
```

### The Spark configuration that catches everyone

Four settings on the **server**, not the client. With Connect,
`spark.sql.extensions` and `spark.hadoop.*` are read when the server builds its
context, so a client cannot supply them — which also means Zamboni cannot, and
whoever operates the server must have.

```
spark.sql.extensions           org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg      org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type rest
spark.sql.catalog.iceberg.uri  https://catalog.example.com/catalog
spark.sql.catalog.iceberg.header.X-Iceberg-Access-Delegation  vended-credentials
```

And then, **for orphan removal only**:

```
spark.hadoop.fs.s3.impl        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint   https://s3.example.com
spark.hadoop.fs.s3a.access.key ...
spark.hadoop.fs.s3a.secret.key ...
```

This is the one place Spark needs static S3 credentials. Every other operation
reads and writes through Iceberg's FileIO on the catalog's vended credentials;
`remove_orphan_files` lists through Hadoop's S3A filesystem, which knows nothing
about Iceberg. Two consequences, both learned by running it rather than reading
about it:

- **Get it wrong and exactly one operation fails** while the other five pass.
  If `remove-orphans` alone is failing on Spark, this is why.
- **`s3://` is not `s3a://`.** Most catalogs advertise table locations with the
  `s3` scheme and hadoop-aws registers only `s3a`, so without the `fs.s3.impl`
  mapping above you get `UnsupportedFileSystemException: No FileSystem for
  scheme "s3"` from inside Iceberg's `FileSystemWalker`.

Set the server's `spark.sql.session.timeZone` if you care what it is, but you do
not need to make it UTC: Zamboni sends timestamps with an explicit `+00:00`
offset precisely so the server's zone cannot change what a retention window
means.

A complete working example — server, Iceberg runtime, S3A, and the `fs.s3.impl`
mapping — is `dev-stack/docker-compose.yaml` under the `spark` profile.

---

## The local engine's limits

The local engine does the work in the Python process that runs it, using DuckDB
and Arrow. That buys you Z-order, partition evolution and a preview of every
operation with no cluster at all. What it costs is memory, and the number below
is measured rather than estimated.

### Memory: bounded by your largest data file, not by the partition

The local engine streams the source as Arrow record batches rather than
materialising it, bin-packs those batches into output-sized slices, writes one
slice at a time, and pushes any `ORDER BY` into DuckDB so the sort spills to
disk. `MemoryMode.CHUNKED` exists for exactly this.

**For a while it did not work, and this guide said so.** Peak memory grew
linearly with the rewrite group and CHUNKED was indistinguishable from
`IN_MEMORY`. The cause was upstream, and is now worked around: handing PyIceberg
the whole task list made it materialise each data file into a list *and* submit
every task at once, so files that finished early sat in memory waiting for the
consumer. Reading one file per call fixes it.

| Group on disk | unbounded reads | **CHUNKED now** | IN_MEMORY |
|---|---|---|---|
| 224 MB | +822 MB | **+692 MB** | +721 MB |
| 447 MB | +1088 MB | **+840 MB** | +1512 MB |
| 894 MB | +1111 MB | **+784 MB** | +2009 MB |

The important column is the third, and the important property is that it is
**flat**. Quadruple the data and it does not move. So:

> **CHUNKED's peak is set by your largest data *file*, not by the group.** A
> partition larger than RAM compacts. An unpartitioned 100GB table compacts.

Smaller files cost less: the same ~675MB group peaks at +362MB in 7MB files
against +475MB in 28MB files. Most of what remains is one materialised file plus
Arrow's allocator retention and DuckDB's own footprint, not something that
scales with your table.

**`IN_MEMORY` is still linear** — 2.3× to 3.4× measured — which is what it is
for. It is faster, and on a group that fits it is the right choice.

**`AUTO`, the default, picks between them** at `memory_budget_bytes`, which is
**256 MiB**. Groups under that materialise; groups over it stream. That
threshold was 1 GiB until the bounded path started working: crossing it bought
nothing, so it was set high to avoid paying for a slower path. Now the trade is
real — `IN_MEMORY` on a 1 GiB group was measured at ~2.3 GiB of growth, more
than a small host has, while CHUNKED stays flat.

**What CHUNKED costs in time: nothing worth planning around.** Bounding the
reads originally meant reading strictly one file at a time, which serialised the
round trips as well — measured 1.26× slower at 10 ms of latency and 1.39× at
30 ms. A **bounded read-ahead window** gives that back. Measured against the dev
stack's MinIO through Lakekeeper, 228 MB in 96 files, with a proxy injecting
per-request RTT:

| RTT to the bucket | one file at a time | **windowed (default)** | unbounded |
|---|---|---|---|
| 10 ms | 20.8 s | **15.3 s** | 15.9 s |
| 30 ms | 36.2 s | **25.8 s** | 26.3 s |

The window matches unbounded speed, because the entire cost was serialised round
trips and two files in flight is enough to hide them. It gives back some memory
to do it — about 70% of the unbounded peak rather than 60% — and it stays flat
as the group grows, which is the property that matters.

`read_ahead_bytes` (64 MiB) sizes that window **in bytes, not files**, so it
adapts: many small files get real concurrency, which is exactly the case with
the most round trips to hide, while a few large ones fall back towards one at a
time, which is exactly the case where memory binds. Set it to `0` to restore
strictly serial reads on a host where even one extra file in flight is too
much.

So the practical rule for the default configuration:

> **Budget ~1 GB for the local engine, plus a margin for your largest data
> file.** Raise `memory_budget_bytes` if you have RAM to spare and want the
> speed; there is no need to lower it.

Group size still matters — for how *long* a rewrite takes, for how much a failed
run wastes, and for `IN_MEMORY` if you choose it — so the rest of this section
still applies. It is no longer the thing that decides whether compaction runs at
all.

#### What a "group" is, and why it still matters

A **group** is the unit of one rewrite: the set of data files compaction reads
together, sorts together and replaces with new files in a single commit. The
planner builds them like this:

1. Take every live data file.
2. Drop the ones already at or above `target_file_size_bytes` — they need no
   rewriting. (Unless `rewrite_all` is set, which keeps them.)
3. Bucket what remains by **(partition spec id, partition value)**.
4. Drop buckets with fewer than `min_input_files` files, and buckets of one.
5. Every surviving bucket is one group.

There is **no cap on how large a bucket may be**, so an unpartitioned table is
one group. That is no longer a memory problem, but it is still three other
things:

- **Duration.** One group is one unit of work with one commit at the end. A
  10-hour rewrite is 10 hours during which nothing is committed.
- **Cost of failure.** A run that fails commits nothing. The output files it
  wrote become orphans that the next `remove-orphans` sweeps once they pass the
  age guard — so a huge group that fails wastes the whole rewrite.
- **The orphan guard.** `older_than_days` must exceed your longest single write,
  and your longest single write is usually a compaction rather than ingest. A
  bigger group pushes that number up. See
  [runbook-dev.md](runbook-dev.md).
- **`IN_MEMORY` if you select it explicitly**, where the 2.3–3.4× ratio is
  still linear in the group.

`zamboni plan` prints the groups it would build, which is how you see all of
this before it happens:

```console
$ zamboni plan acme.events --table-config table-config.json
acme.events: 2 group(s), 8 file(s), target 134217728 bytes, snapshot 8239698985041148504
  group 0: spec=0 partition=(20671) files=4 bytes=3768 rows=2000
  group 1: spec=0 partition=(20672) files=4 bytes=3768 rows=2000
  skipped spec=0 partition=(20673): 1 candidate file(s) < min_input_files=2
```

(Partition values print as Iceberg stores them — `20671` is a day transform,
days since the epoch, not a date.)

#### How to reduce group size

Still worth doing for the reasons above, in order of how well they work:

**1. Partition the table, or partition it more finely.** The lever with no
downside. Day instead of month divides the largest group by ~30, and a table
partitioned by day with steady ingest has a bounded group size *forever*,
however large the table grows.

**2. Lower `target_file_size_bytes`.** Step 2 above excludes files already at or
above the target, so a smaller target excludes more of them and the group
shrinks. The cost is that you are asking for smaller output files, which is the
problem compaction exists to solve.

**3. Never set `rewrite_all` on a large partition.** It disables step 2
entirely, so every file joins the group including those already at target. It
exists for forcing a re-sort after a layout change.

**4. Raise `min_input_files`.** This does not shrink a group; it stops small
buckets being compacted at all. Useful for skipping pointless work, useless for
the partition that is actually too big — a common misreading.

**5. Move compaction to Spark.** A cluster spreads one group across executors,
which addresses duration rather than memory. Keep `local` for everything else:
expiry, orphans, manifests and properties are metadata operations costing
megabytes regardless of table size, and partition evolution only exists on
`local`. Both engines read the same `table-config.json`, so this is a
per-operation choice, not a migration.

#### Why the fix was the reader and not a group cap

Worth recording, because capping group size is the obvious fix and it is the
wrong one: **it would have quietly degraded every Z-ordered table.**

Z-order interleaves the bits of several columns so that rows near each other in
any of those dimensions land in the same file. The clustering is only as good as
the set of rows the sort can see at once. Sort a whole group and every row is
placed relative to every other — file statistics come out tight, and a query
filtering on any Z-ordered column skips most files. Split that group in half and
sort each half independently and you get two overlapping ranges instead of one
ordered sequence: statistics widen, fewer files skip, and the benefit degrades
roughly in proportion to the number of pieces.

So the two goals pull against each other:

| | wants |
|---|---|
| Bounded memory | the smallest possible group |
| Z-order quality | the largest possible group |

Bounding the *read* escapes the conflict entirely. DuckDB still receives the
whole group as one stream and spills its sort to disk, so the ordering sees
every row it would have seen — the memory bound and the clustering are no longer
competing for the same knob. `test_chunked_still_sorts_across_the_whole_group`
pins that: it forces several output files per partition and asserts the ordering
holds across all of them.

**Partitioning never had the conflict either**, which is why it is lever 1
above. Z-order applies within a partition anyway, since Iceberg already skips
whole partitions by predicate, so daily partitions cost nothing in clustering
quality. Splitting a single partition into arbitrary sub-groups would have cost
a great deal.

#### Practical guidance

- **Home gamers:** the defaults are now safe on a small host. ~1 GB of headroom
  runs the local engine on tables far larger than that. Partition by day or
  month anyway — it bounds how long a rewrite takes and how much a failed one
  wastes.
- **Administrators:** size against your largest *data file* plus about a
  gigabyte, not against the table. If you have RAM to spare and want the speed
  back, raise `memory_budget_bytes` so more groups take the materialising path.
- **Anyone:** if a run is killed by the OOM killer, the log ends mid-operation
  with no traceback. That signature — truncated log, no Python error — used to
  be this section. If you see it now, check whether `memory_mode` is pinned to
  `in_memory` or `rewrite_all` is set on a large partition.

### Other things to know

- **The reachable-file set is proportional to file count.** Orphan removal and
  expiry build a set of every path the table references. Paths, not rows — but a
  table with millions of files will hold millions of strings. This is far
  cheaper than the compaction ceiling above and rarely the binding constraint.
- **It is single-machine and single-process.** No amount of configuration turns
  it into a cluster. Throughput is one host's disk and CPU.
- **Equality deletes block compaction.** PyIceberg cannot read them, so a table
  carrying them is refused rather than compacted into something that resurrects
  deleted rows. Merge-on-read tables written by Flink are the usual source. This
  is a refusal, not a failure — exit code 3.
- **Orphan removal needs a bucket listing.** A remote-signing catalog refuses
  `ListObjectsV2`, so `remove-orphans` cannot run at all and `expire` will commit
  while freeing nothing. Check this *before* your first run; it looks perfectly
  healthy until you try to free a byte. See
  [live-verification.md](live-verification.md).
- **DuckDB spills sorts to disk.** Set `temp_directory` somewhere with room if
  you Z-order large partitions; the sort itself is not held in memory.

### What the local engine does *not* do badly

Worth stating plainly, because "no cluster" invites suspicion:

- It does not load whole tables to expire snapshots, rewrite manifests, remove
  dangling deletes or apply properties. Those are metadata operations and cost
  megabytes, not gigabytes, regardless of table size.
- It does not read data files to find orphans — it compares path sets.
- Its compaction reads and writes exactly one group at a time, and commits once
  at the end. A failure part-way commits nothing; the files it wrote become
  orphans that the next `remove-orphans` sweeps.

---

## Every control, and where it lives

Settings live in three places, and which place is not arbitrary — it is the
distinction between *what the table should look like*, which analysts own and
which is the same on every engine, and *how a run executes*, which an operator
owns and which is specific to the machine doing the work.

| Where | Owns | Read by |
|---|---|---|
| `table-config.json` | layout and retention: partitioning, ordering, what may be deleted | every engine |
| `zamboni.yml` | which catalog, which engine, which operations | the CLI |
| CLI flags / `CompactionConfig` | how one run executes: memory, concurrency, commits | the local engine |
| `.env` | credentials, and nothing else | the CLI |

A flag always beats the file, so a one-off run overrides without an edit.

### `table-config.json` — layout and retention

Full schema in [table-config.md](table-config.md); `zamboni table-config
summary` prints what a given file actually means, including the defaults you did
not write.

| Setting | Default | What it does |
|---|---|---|
| `partition` | none | The partition spec. The single most consequential setting: it bounds rewrite duration, and it is what lets Iceberg skip whole partitions |
| `partition_evolution` | day→month after 90d | Rewrite aged partitions to a coarser transform. **`local` only** |
| `ordering.mode` | `none` | `sort` or `zorder`. **Z-order is `local` and `spark` only** |
| `ordering.sort` | — | Columns for a declared sort order |
| `ordering.zorder.columns` | — | Columns to interleave (`zorder_columns` in Python). Two to four is the useful range |
| `ordering.zorder.precision_bits` | 16 | Bits taken from each column before interleaving (`zorder_precision_bits`). More bits is finer clustering and a longer key |
| `retention.expire_snapshots.max_snapshot_age_days` | Iceberg's 5 | How far back time travel reaches. **Also how long a running query may hold a snapshot** |
| `retention.expire_snapshots.min_snapshots_to_keep` | Iceberg's 1 | Floor, regardless of age |
| `retention.remove_orphan_files.older_than_days` | 3 | **The one that can destroy data if set wrong.** Must exceed your longest single write — usually your own compaction, not ingest |
| `retention.remove_dangling_deletes.enabled` | on | Drop delete files that no longer apply |
| `retention.rewrite_manifests.enabled` | on | Regroup manifest entries so predicates prune |
| `retention.metadata.previous_versions_max` | unset | Trim the metadata log |
| `retention.metadata.delete_after_commit` | unset | Actually delete what the log drops |

### `zamboni.yml` — the profile

Six keys, and unknown ones are refused rather than ignored. Template:
`zamboni.yml.sample`.

| Key | Default | What it does |
|---|---|---|
| `uri` | — | REST catalog endpoint |
| `warehouse` | — | Warehouse name |
| `engine` | `local` | `local`, `trino` or `spark` |
| `root` | `~/.zamboni` | Where per-warehouse configs live: `{root}/configs/{warehouse}/table-config.json` |
| `operations` | all six | Which operations `maintenance` runs, and in this order |
| `tables` | every table in the config | Restrict a run |

### Run controls — flags, or `CompactionConfig` from Python

Every one of these is a flag *and* a field, with the same name and the same
default; `test_cli_defaults_match_the_dataclass` keeps them honest.

| Flag / field | Default | What it does |
|---|---|---|
| `--target-file-size-bytes` | from table properties, else 128 MiB | Output size, and the threshold above which a file is left alone |
| `--min-input-files` | 2 | Skip partitions with fewer candidates than this |
| `--rewrite-all` | off | Rewrite everything, including files already at target. **The most reliable way to run out of memory on a large partition** |
| `--memory-mode` | `auto` | `auto`, `in_memory`, `chunked` |
| `--memory-budget-bytes` | 256 MiB | Group size above which `auto` streams instead of materialising |
| `--read-ahead-bytes` | 64 MiB | How much of a group the streaming path may have in flight. `0` reads strictly one file at a time — bounded tightest, and slower against object storage because it serialises the round trips |
| `--max-read-ahead-files` | 8 | Ceiling on concurrent reads regardless of the byte window |
| `--temp-directory` | system | Where DuckDB spills sorts. Point it somewhere with room if you Z-order |
| `--partial-progress` | off | Commit each group as it finishes. More commits, but a failure keeps the finished ones |
| `--branch` | `main` | Which branch to commit to |
| `--snapshot-operation` | `replace` | How the snapshot is labelled |
| `--dangling-delete-policy` | `report` | `block` refuses to compact rather than touch delete files |
| `--sort-by` | — | Arbitrary DuckDB `ORDER BY`. Output keeps `sort_order_id = None`, because the rows are ordered but not by any order the table declares |
| `--sort-by-table-order` | off | Order by the table's declared sort order, and stamp its id |
| `--yes` | off | **Actually commit.** Without it every mutating verb previews |

---

## Two configurations worth copying

### 1. General data — dimension tables, reference data, anything without a time axis

Tables that are queried by key rather than by range, updated in batches, and not
naturally partitioned. Compaction is doing the classic small-files job.

```json
{
  "version": 1,
  "defaults": {
    "ordering": { "mode": "none" },
    "partition_evolution": { "enabled": false },
    "retention": {
      "expire_snapshots":     { "enabled": true, "max_snapshot_age_days": 7,
                                "min_snapshots_to_keep": 3 },
      "remove_orphan_files":  { "enabled": true, "older_than_days": 3 },
      "remove_dangling_deletes": { "enabled": true },
      "rewrite_manifests":    { "enabled": true },
      "metadata":             { "previous_versions_max": 10,
                                "delete_after_commit": true }
    }
  },
  "tables": {
    "acme.customers": {
      "ordering": { "mode": "sort", "sort": [{ "column": "customer_id" }] }
    },
    "acme.products": {}
  }
}
```

```cron
17 3 * * * cd /srv/zamboni && zamboni maintenance --yes --verbose >> /var/log/zamboni/cron.log 2>&1
```

Why these numbers:

- **`max_snapshot_age_days: 7`, `min_snapshots_to_keep: 3`.** A week of time
  travel and at least three snapshots regardless. The minimum is the useful
  half — it survives a quiet weekend where age alone would expire everything but
  the newest.
- **No partitioning, and that is fine here.** These tables do not have a
  time axis to partition on, and inventing one to satisfy the maintainer would
  make every query read every partition. The consequence is that the whole table
  is one rewrite group, which since ZMBNI-1906 is a duration question rather
  than a memory one.
- **`partition_evolution` off**, because there is nothing to evolve.
- **Sort, not Z-order, on `customers`.** One access pattern, one column, so a
  declared sort order is both sufficient and honest — it stamps a real
  `sort_order_id` that other engines can see.

### 2. Process and event data — day partitions, evolving to months

The shape this tool was built for: an append-only event stream, partitioned by
day, queried on a time range plus one or two other columns, where old partitions
are read rarely and should stop being thousands of small daily directories.

```json
{
  "version": 1,
  "tables": {
    "acme.events": {
      "partition": [{ "column": "event_ts", "transform": "day" }],
      "partition_evolution": {
        "enabled": true,
        "rules": [{ "from": "day", "to": "month", "older_than_days": 90 }]
      },
      "ordering": {
        "mode": "zorder",
        "zorder": { "columns": ["customer_id", "event_type"] }
      },
      "retention": {
        "expire_snapshots":    { "enabled": true, "max_snapshot_age_days": 3,
                                 "min_snapshots_to_keep": 2 },
        "remove_orphan_files": { "enabled": true, "older_than_days": 3 },
        "metadata":            { "previous_versions_max": 5,
                                 "delete_after_commit": true }
      }
    }
  }
}
```

```cron
# Nightly: compaction and expiry. Orphan removal lists storage, so weekly.
17 3 * * 1-6 cd /srv/zamboni && zamboni --profile nightly.yml maintenance --yes
17 3 * * 0   cd /srv/zamboni && zamboni maintenance --yes
```

```yaml
# nightly.yml — everything except the storage listing
operations: [compact, apply-properties, remove-dangling-deletes, rewrite-manifests, expire]
```

Why these numbers:

- **Day partitions bound the rewrite.** One day's ingest is one group, so a
  compaction is one day's work however many years the table holds. This is the
  setting that decides how long a nightly run takes.
- **`day → month` after 90 days** is the reason partition evolution exists. A
  three-year daily table is ~1,095 partitions and ~1,095 metadata entries a
  planner walks; folding everything past a quarter into months takes that to
  ~36 plus a quarter of days. New data still lands in day partitions — evolution
  changes where *old* data lives, not where writes go. **`local` only**, so if
  you compact on Spark, run this operation on `local`.
- **Z-order on `customer_id, event_type`.** The partition predicate already
  handles time; Z-order handles the other two dimensions, which a single-column
  sort could not. **Not available on Trino** — `zamboni table-config summary`
  will tell you so.
- **`max_snapshot_age_days: 3`**, shorter than the general case, because
  append-only event tables accumulate snapshots fast and nobody time-travels a
  telemetry table to last Tuesday. Check it against your longest-running query
  first: expiry is what removes the snapshot a slow reader is holding.
- **`older_than_days: 3` for orphans** — Iceberg's default, and it must exceed
  your longest single write. Time your largest compaction before trusting it;
  the guidance is in [runbook-dev.md](runbook-dev.md).

Neither of these enables anything destructive that is off by default, and both
should be run without `--yes` for a week first. `zamboni table-config summary`
prints what either one would actually do — including the two warnings the event
config earns:

```console
$ zamboni table-config summary table-config.json
acme.events
  layout     partition [event_ts:day], ordering zorder
             zorder columns customer_id, event_type  -- not available on: trino
  expire     on, keep 3 day(s), minimum 2 snapshot(s)
  orphans    on, files older than 3 (default) day(s) -- this deletes storage; the
             guard is what stands between it and a live write
  dangling   on
  manifests  on
  metadata   keep 5 previous version(s), delete after commit True
  evolution  day->month after 90d  -- not available on: spark, trino
```

Both samples are checked by `test_the_documented_configurations_are_valid`, so
they load against the current schema rather than the one they were written for.

---

## When something goes wrong

That is [runbook.md](runbook.md): exit codes, how to read a failed cycle's logs,
and how to get a stack trace out of a cron job. The short version:

| Exit | Means |
|---|---|
| 0 | Success |
| 2 | Usage error — fix the invocation |
| 3 | The table is **blocked** (V1, equality deletes, unusable PyIceberg). A refusal, not a failure |
| 4 | A **safety check aborted** the run. **Nothing was deleted.** Investigate before retrying |

Exit 4 is the interesting one and should never be worked around by narrowing
the scope.

For developers running each step by hand, see
[runbook-dev.md](runbook-dev.md).
