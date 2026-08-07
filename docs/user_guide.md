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
limitation. When this table and that command disagree, the command is right.

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

### Secrets

The rule is the same in every mode: **non-secret configuration in
`zamboni.yml`, credentials in `.env`, and neither in the crontab.** A crontab is
world-readable on many systems, appears in `ps` output, and gets committed by
accident.

```
zamboni.yml     catalog URI, warehouse name, which operations to run   → commit this
.env            tokens, S3 keys                                        → chmod 600, never commit
```

```bash
chmod 600 .env
```

Zamboni reads `./zamboni.yml` and `./.env` from the working directory, then
`$ZAMBONI_ROOT/zamboni.yml`. Cron gives a job almost no environment, which is
exactly why the dotenv file exists — see [devops.md](devops.md) for the
multi-tenant layout.

For a credential-vending catalog (Lakekeeper with `sts-enabled`), the *only*
secret Zamboni needs is the catalog token. S3 credentials are vended per table
and never stored. That is worth arranging deliberately: it is the difference
between one revocable secret and a set of long-lived object-store keys. The
exception is Spark orphan removal, which needs its own S3 keys on the server —
see [Mode 4](#mode-4-spark-from-cron).

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

```bash
install -m 600 /dev/null /srv/zamboni/.env
cat > /srv/zamboni/.env <<'EOF'
ZAMBONI_URI=https://catalog.example.com/catalog
ZAMBONI_WAREHOUSE=home
ZAMBONI_TOKEN=...
EOF
```

Zamboni reads `./.env` automatically. Nothing secret needs to appear in the
crontab, in the wrapper, or in `ps`.

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

### Memory: budget ~2× your largest partition

The planner makes **one rewrite group per (partition spec, partition value)**,
with no cap on group size. Compaction then holds roughly twice that group's
on-disk size in RAM:

| Group on disk | Peak RSS growth | Ratio |
|---|---|---|
| 226 MB | +538 MB | 2.4× |
| 450 MB | +975 MB | 2.2× |
| 889 MB | +1840 MB | 2.1× |

Measured on an unpartitioned table with `memory_budget_bytes` set to 64MB —
which is to say, **`MemoryMode.CHUNKED` does not currently bound this**. It
streams record batches and bin-packs them, but peak memory still grows linearly
with the group and is indistinguishable from `IN_MEMORY`. Most of that is
upstream: consuming PyIceberg's `ArrowScan.to_record_batches(tasks)` and
discarding every batch immediately still grows ~1.3× the group's size, because
it reads the tasks concurrently and buffers them. Tracked as ZMBNI-914.

So the rule is:

> **An unpartitioned table is compacted as one group.** A 20GB unpartitioned
> table needs ~40GB of RAM, and there is no setting that changes that today.

**Partitioning is the lever that works.** A table partitioned by day, with 500MB
of new data per day, compacts one day at a time and peaks around 1GB regardless
of how large the table is in total. This is a good reason to partition even when
query patterns do not demand it.

Practical guidance:

- Home gamers: partition by day or month. Anything that keeps a partition under
  a few hundred MB will run comfortably in 2GB of RAM.
- Administrators: size the maintenance host against the *largest partition* you
  expect, not the largest table. `zamboni plan` prints the groups compaction
  would build, one per partition, with their file counts — that is the number to
  double.
- If a table is large and unpartitioned, use Spark for compaction and `local`
  for everything else. Both read the same config.

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
