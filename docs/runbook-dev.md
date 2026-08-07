# Developer runbook

Running each maintenance step by hand, and understanding why the sequence is
what it is. For **operating** a warehouse — cadence, sizing, and what to do when
a nightly run fails — see [runbook.md](runbook.md). For getting started at all,
see [user_guide.md](user_guide.md).

This is the document to read before changing the order of operations or adding
a seventh, because every gap in the sequence below is there for a reason and
three of them are load-bearing.

---

## 0. Before the first run

**Check the warehouse can reclaim storage at all.** This is the one precondition
worth verifying before anything else, because a warehouse that cannot reclaim
looks completely healthy: reads work, writes work, compaction appears to succeed.

```bash
uv run scripts/verify-live.py --port 8181 --warehouse your_warehouse \
    --s3-host <host> --s3-port <port>
```

If `test_the_warehouse_vends_credentials_rather_than_signing` fails, stop. A
remote-signing Lakekeeper warehouse refuses `ListObjectsV2`, `HeadObject` and
multi-object `DELETE`, so `expire` will commit and free nothing while
`remove-orphans` cannot run at all. Measured, not theoretical — see
[live-verification.md](live-verification.md). The fix is a warehouse with
`sts-enabled: true`, or direct S3 credentials.

**Then dry-run everything.** One rule, no exceptions: **without `--yes`, nothing is
committed.** Every mutating verb previews and says so.

```bash
zamboni describe  your.table          # read-only: layout, blockers, warnings
zamboni plan      your.table          # what compaction would rewrite, and what it skips
zamboni expire    your.table          # what retention would expire and delete
zamboni remove-orphans your.table     # what is unreferenced, and what the guard holds back
```

---

## 1. The order, and why each position matters

Run all six as one sequence. The order is not arbitrary — three of the five gaps
between them are load-bearing.

```bash
zamboni compact                 your.table --table-config c.json --yes
zamboni apply-properties        your.table --table-config c.json --yes
zamboni remove-dangling-deletes your.table --table-config c.json --yes
zamboni rewrite-manifests       your.table --table-config c.json --yes
zamboni expire                  your.table --table-config c.json --yes
zamboni remove-orphans          your.table --table-config c.json --yes
```

| Step | Why here |
|---|---|
| `compact` | Rewrites data files. Frees **no storage** — the files it supersedes are still referenced by the snapshot it compacted out of, which is what keeps time travel working |
| `apply-properties` | Anywhere before `remove-orphans`. Trimming the metadata log is what makes stale `metadata.json` unreferenced for the last step to find, so only that ordering constraint is real |
| `remove-dangling-deletes` | After compaction, because compaction is what *creates* dangling deletes: it applies them and writes files with a higher sequence number |
| `rewrite-manifests` | After the delete removal, so the manifests it emptied are gone before we decide how to regroup what remains |
| `expire` | Drops the old snapshots, which is the only thing that unreferences the superseded files. Deletes exactly the difference |
| `remove-orphans` | Last, sweeping what nothing references: abandoned writes, and metadata versions dropped from the log |

`./bin/demo maintenance` runs exactly this sequence — `src/himsdemo/cli.py` is the
worked example, and the two are kept in the same order deliberately.

**The step most often left out is `expire`.** Without it, compaction and manifest
rewriting make queries faster and storage *larger*.

---

---

## 2. Running one step at a time

Every verb takes the same shape, and **without `--yes` nothing is committed**.
That makes each step individually inspectable, which is the point of running
them separately:

```bash
zamboni describe          your.table                          # read-only
zamboni plan              your.table --table-config c.json    # what compaction would rewrite
zamboni compact           your.table --table-config c.json    # preview
zamboni compact           your.table --table-config c.json --yes
```

`plan` is the one worth knowing: it prints the groups compaction would build,
their file counts and sizes, and — more usefully when something looks wrong —
the partitions it is *skipping* and why (`3 candidate file(s) <
min_input_files=5`, `single candidate file`).

Because a group is one (spec, partition) pair, `plan` is also how you find out
what compaction will cost in memory. See the local engine's limits in
[user_guide.md](user_guide.md).

### Driving a single operation from Python

Useful in a debugger, and the same path the CLI takes:

```python
from zamboni import (
    CatalogSession, MaintenanceRequest, Operation, TableConfig,
    config_from_table_settings, get_maintainer,
)

session = CatalogSession.for_local(warehouse_path="/tmp/wh")
settings = TableConfig.load("c.json").for_table("your.table")
maintainer = get_maintainer("local")(session, {})
request = MaintenanceRequest(
    retention=settings.retention,
    compaction=config_from_table_settings(settings),
)
print(maintainer.execute(Operation.COMPACT, "your.table", request=request, dry_run=True).describe())
```

`dry_run=True` is the API-level equivalent of omitting `--yes`.

---

## 2. Cadence

There is no schedule to recommend, because the useful one falls out of your own
retention settings. The arithmetic:

**Compaction** is driven by ingest. One commit produces at least one data file,
one manifest, one manifest list and one `metadata.json`, and nothing is updated
in place. So after *N* commits you have ≥ *N* of each. In the demo, one commit
per hour of activity gave 58 data files in five days — at which point metadata
was **340 KiB describing 218 KiB of data**, and a query for one day opened one
manifest per data file it read. Compact often enough that the count stays in the
tens, not the thousands: `commits_per_day × days_between_runs` is the number to
keep an eye on.

**Expiry and orphan removal are gated by their own windows**, so running them
more often than the window is cheap but reclaims nothing:

| | Default | Reclaims nothing until |
|---|---|---|
| `expire_snapshots.max_snapshot_age_days` | 5 (Iceberg's) | a snapshot is 5 days old |
| `remove_orphan_files.older_than_days` | 3 (Iceberg's) | an unreferenced file is 3 days old |

So a nightly run of the whole sequence is a reasonable default: compaction does
work every night, and the two reclaim steps do nothing for the first few nights
and then steady out. They will report that plainly rather than looking broken:

```
5 file(s) (7.0KiB) left in place: younger than the 3-day age guard
```

**Run it on one schedule, not six.** The steps are ordered for a reason and
splitting them across cron entries reintroduces the gap the order closes.

---

---

## 3. Sizing the orphan guard

The single most consequential number here, because it is the one that can destroy
data if set wrong.

**The rule:** `older_than_days` must exceed the longest single write the warehouse
performs. A file that has been written but not yet committed is indistinguishable
from an orphan — there is no flag on it, nothing in the catalog refers to it, and
the age guard is the only thing standing between it and deletion.

**The trap:** the longest write is usually *your own compaction*, not your ingest.
A tap writing hourly micro-batches commits in seconds. A compaction rewriting a
large partition can run for a long time and writes every output file before it
commits anything — which is exactly the window the guard protects.

**Measure it rather than guess.** Time your largest compaction:

```bash
time zamboni compact your.biggest.table --table-config c.json --yes
```

Then set `older_than_days` to comfortably more than that, and never less than
Iceberg's default of 3. If your longest compaction is 4 hours, 3 days is already
17× headroom; if it is two days, 3 is too tight.

**What the guard does not protect.** Long-running *readers*. A query holding a
file list while the file is deleted underneath it fails, and file mtime is the
wrong clock for that — the file may be days old and still in use. Snapshot
retention is the mechanism there: keep `max_snapshot_age_days` longer than your
longest query.

**`--reclaim-now` / a zero guard.** Only on a warehouse nothing else is writing.
It prints a warning for that reason. The demo uses it so storage visibly falls;
production should not.

---

---

## 6. The dev stack

Everything above can be exercised against a real REST catalog and object store
without touching a production warehouse:

```bash
cd dev-stack
cp .env.sample .env
docker compose up -d --wait && uv run bootstrap.py
uv run pytest tests/test_dev_stack.py
```

Trino and Spark are optional profiles, off by default because each is a JVM:

```bash
docker compose --profile trino up -d trino
docker compose --profile spark up -d --wait spark    # Spark Connect on 15012
uv run pytest tests/test_dev_stack.py -m spark
```

The dev stack's own README documents the two credential paths, the `s3://` vs
`s3a://` mapping, and why the Spark session timezone is deliberately not UTC.

---

## 7. Reproducing a report

Quote all three versions. Which operations this tool will even attempt is
decided by probing the installed PyIceberg rather than by comparing version
numbers, so "zamboni 0.1.0" alone does not identify a behaviour:

```console
$ zamboni --version
zamboni 0.1.0 (pyiceberg 0.11.1, python 3.13.14)
$ zamboni doctor
```

`zamboni doctor` prints the full capability probe. `zamboni engines` prints what
each engine supports and, more usefully, what each refuses and why.
