# Event-driven maintenance: deciding *when*

Zamboni is invoked on a schedule and has no basis for deciding a table needs work.
Once invoked it decides plenty — which operations are configured, which the engine
supports, which partitions are candidates, whether a rewrite is safe. It cannot
decide the first question: **does this table need anything at all?**

This is the design for answering that. The full investigation, including the survey
of what other Iceberg maintenance systems do, is in the
[architecture document](https://claude.ai/code/artifact/45390568-1735-4c6c-ae7b-b9269008718a);
this file records the decisions so they survive in the repository.

**Status: designed, not built.** Tracked as ZMBNI-106.

---

## 1. The sensors already exist

Nothing here needs new instrumentation. Three layers are already present and unused.

**The health model is written down.** [runbook.md](runbook.md) §3 defines the signals
with "concerning when" thresholds. **Three of them come from metadata alone** —
average file size against target, delete-file pressure, and records per file —
and metadata adds two the runbook did not have: snapshots retained and
metadata-log entries, both expiry pressure. The rest need a deeper tier: files
per partition, manifests per data file and metadata bytes against data bytes
need the manifests, and unreferenced files needs a storage listing.

That is a cost ladder rather than a shortfall, and the due-check only needs the
cheapest rung. `TableHealth` reports which signals it could not see rather than
omitting them, because "no problems found" and "no problems visible from here"
are different claims.

**Iceberg publishes the telemetry.** A metadata-only `load_table` returns snapshot
summaries carrying `total-data-files`, `total-files-size`, `total-delete-files`,
`total-position-deletes`, `total-equality-deletes`, `added-data-files`,
`deleted-data-files` and `changed-partition-count`.

**Both writers already stamp provenance.** Zamboni writes `zamboni.operation`,
`zamboni.added-data-files`, `zamboni.removed-data-files`; `target-iceberg` writes
`target-iceberg.stream`, `.load-method`, `.rows` and `.operation`. So "when did
maintenance last run, and what has written since" is answerable from table metadata
alone, with no external state to lose.

Measured on the dev stack, one table, 631 files, 159 snapshots:

| Operation | Cost | Reads |
|---|---|---|
| `load_table` | **20–38 ms** | table metadata only |
| `profile_table` | 391–402 ms | manifest lists and manifests |
| `list_storage` | ~50 ms | one recursive prefix listing |
| `reachable_files` | ~2,035 ms | metadata, manifest lists, manifests |

At ~25 ms per table a 1,000-table warehouse evaluates in ~25 seconds. **Deciding
whether to work is nearly free; only the work is expensive.** That number is why
the phases are ordered as they are — events buy latency, not information.

---

## 2. Three deployment models

```
  1 · crontab ──┐
  2 · service ──┼──▶ maintain() ──▶ six operations
  3 · embedded ─┘     one run loop      unchanged
```

| | Who decides when | Config from | Process model |
|---|---|---|---|
| **1 · cron** | crontab | files on disk | one short-lived process per run |
| **2 · service** | Zamboni's scheduler and events | files on disk, reloaded | long-running + worker pool |
| **3 · embedded** | the integrator | the integrator's own state | the integrator's process |

All three reach the same `maintain()`, so the exit-code contract, the consent rule
and the [§6.6 invariants](design.md) are inherited rather than reimplemented. The
service is an addition, not a replacement.

---

## 3. Inside the service

```
  config ──┬──▶ scheduler ──┐
           │                ├──▶ candidate queue ──▶ due-check ──┬──▶ maintain()
           └──▶ NATS consumer ┘    (coalesced)                   └──▶ skipped, counted
```

**Both inputs converge on one decision point.** A schedule tick says "consider every
table in this warehouse"; an event says "consider this one now". Neither decides:
both enqueue, and the same due-check answers. Event-driven and scheduled maintenance
therefore cannot drift apart, and an event can never cause work the schedule would
not eventually have done.

**The queue coalesces.** Fifty events for one table during a load produce one
candidate; a debounce window lets a burst settle. A table already being maintained
is not re-enqueued.

### Threads or processes

**asyncio for the front half, a bounded process pool over tables for the back half.**

| Stage | Where the time goes | GIL |
|---|---|---|
| catalog and object-store requests | sockets | released |
| manifest and manifest-list decode | Python avro decode | **held** |
| reachable-set arithmetic | Python sets over file lists | **held** |
| DuckDB sort and Z-order | C++, internally threaded | released |
| Parquet write | Arrow C++ | released |

The weight is on the contended half — `reachable_files` at ~2,035 ms is largely
fetching and decoding 159 manifest lists in Python. Threads would overlap the
fetches and then queue on the decode.

**The unit of parallelism is the table**, which falls out of the existing design:
`maintain()` already loops tables independently, with per-table outcomes and the
exit code the worst of them. A process pool changes no semantics and adds failure
isolation — an OOM takes one worker, not the fleet.

**Memory forces processes, not CPU.** Each worker holds its own DuckDB connection
and memory budget, so the footprint is `workers × memory_budget_bytes`. That argues
for a small pool of well-fed workers: a rewrite that cannot hold its group spills,
and spilling is slower than waiting for a free worker.

**Work items must be serialisable.** A `CatalogSession` holds a DuckDB connection
and a catalog client and cannot cross a process boundary, so a worker receives
config — warehouse, table, table-config — and builds its own session.

**Free-threaded Python would change this** and is worth watching rather than
depending on. Keeping the parallel unit at the table maps onto either model.

---

## 4. Configuration

Two files, split by who writes them.

**(a) The fleet file — written by the provisioner, read by Zamboni.** Warehouses and
their tables; conceptually a dump of the provisioning system's database. Nothing in
it is a secret.

```yaml
warehouses:
  - name: acme
    uri: https://catalog.internal/catalog
    schedule: "0 2 * * *"        # backstop; events drive the rest
    tables:
      events:   {compaction: {target_file_size_bytes: 134217728}}
      sessions: {retention: {expire_snapshots: {older_than_days: 7}}}
```

**(b) `zamboni.yml` + `.env` — written by the operator.** Storage endpoints and
credentials, catalog auth, engine choice, spill directory, the NATS address. Needs a
`service:` block, not a new format.

### Reloading

**Primary: mtime polling, every 30–60 s.** In Kubernetes a ConfigMap update is
projected into the container and the file changes on disk; no signal is deliverable
without `kubectl exec`, which is not an automation path. Two details: projection
swaps a symlink, so stat the resolved path; and the kubelet sync period means an
update lands within about a minute regardless, so polling faster buys nothing.

**Secondary: `zamboni config-reload` sends `SIGHUP`** via a pid file, for
non-container deployments. `SIGHUP` because reload-on-HUP is the convention a
sysadmin expects.

Rejected: a touch-file sentinel (a path to configure, no capability mtime lacks);
`inotify` (a dependency, and unreliable over NFS and some projected-volume drivers);
an admin message over NATS (couples config reload to the event bus being up).

**A config that fails validation leaves the running config in place and is
reported.** A reload must never be able to stop maintenance by being wrong — a real
risk when the file is machine-generated from another system's database.

---

## 5. The event contract

One subject, one payload, any Iceberg writer. CloudEvents, because Lakekeeper
already emits that envelope.

**Subject**: `iceberg.table.write.completed` — *completed*, not *committed*.
Lakekeeper's per-commit events fire **during** ingestion, when maintenance must not
run. A writer fires once, when it has finished, which is when maintenance should.

| Field | Meaning |
|---|---|
| `catalog`, `namespace`, `table` | table identity |
| `snapshot_id` | the last snapshot this run produced |
| `writer` | e.g. `target-iceberg/0.9.2` |
| `load_method` | `append-only` \| `upsert` \| `overwrite` |
| `operation` | `full-refresh` when the table was replaced |
| `rows` | rows this run committed |
| `data_files_added`, `bytes_added` | what the run produced |
| `partitions_changed` | count, and the values when bounded |
| `started_at`, `completed_at` | the write window |

**Delivery semantics.** At-least-once and unordered; consumers deduplicate on
`snapshot_id`. **Advisory** — a dropped event costs latency, never correctness,
because every field is independently derivable from table metadata.
**Fire-and-forget** — an emit failure is logged and the ingestion succeeds.
**Bounded** — no record samples, no config dumps.

**The governing principle: the event is a hint; the table is the truth.** A missed,
duplicated or out-of-order event degrades to the polling path.

**Not a command.** It states what happened; the consumer decides what that warrants,
against policy the consumer owns. A writer must never be able to order a deletion.

---

## 6. Targeting

**Table-level** is nearly free: compare the current snapshot against the last one
carrying a `zamboni.*` stamp. No new snapshots means nothing to do.

**Partition-level** is half-built. `skip_partitions_newer_than_windows` already holds
back partitions inside a recency floor; its inverse — compact *only* what changed
since the watermark — does not exist. The snapshots since the watermark name their
manifests, so the changed set is derivable from metadata, and `changed-partition-count`
says cheaply whether it is worth deriving. The two compose: changed-since-watermark
selects, the recency floor removes what is still hot.

### The line that must not move

Targeting decides **whether** and **what**, never **how safely**.

- **Orphan removal cannot be partition-targeted.** It subtracts a reachable set from
  a listing, and §6.6's completeness invariant requires every referenced file to be
  present in that listing. A partition-scoped listing breaks the check that caught a
  real keying bug instead of deleting every live file.
- **Expiry cannot be event-driven alone.** Retention is a function of time, not of
  writes.
- **A targeted compaction still previews, still refuses, still aborts.**
- **A missed event must not mean a table is never maintained.** The periodic sweep
  stays as the floor; events raise the ceiling.

Every health-driven system surveyed still has a schedule underneath. The schedule
stops being the *decision* and becomes the *backstop*.

---

## 7. Running as a service

**HTTP is not required.** Kubernetes offers four probe mechanisms and `exec` is as
first-class as `httpGet`. `zamboni service-status --probe=liveness` reads a state
file the service writes atomically — started at, last scheduler tick, last sweep per
warehouse, config generation, NATS connected, workers busy, last error.

| Probe | Checks | Never checks |
|---|---|---|
| liveness | the scheduler loop ticked within N intervals | NATS or catalog reachability — a restart fixes neither |
| readiness | config loaded and valid; NATS connected if configured | in-flight work — busy is not unready |
| startup | first config load and capability probes finished | — |

**Graceful shutdown is the requirement with teeth.** `SIGTERM` then `SIGKILL` after
`terminationGracePeriodSeconds`, default 30 s, which is shorter than a large
compaction. The service must stop accepting candidates, let in-flight tables finish,
and exit; the grace period must exceed the longest expected table. A hard kill is
*safe* — an interrupted rewrite commits nothing and the next `remove-orphans` sweeps
the files — but a service killed mid-compaction every deploy never finishes a large
table.

**Size the pool from the cgroup, not the host.** `os.cpu_count()` reports the node's
CPUs, not the container's quota. Memory matters more: `workers × memory_budget_bytes`
is what the limit must cover.

**One replica**, `strategy: Recreate`, until a claim protocol exists. A
`RollingUpdate` briefly runs two pods, which is the case there is no interlock for.

**A writable spill directory** — an `emptyDir` at `/tmp` on a read-only root
filesystem, or `temp_directory` pointed somewhere mounted.

### One deployment detail that will bite

Kubernetes projects Secret and ConfigMap volumes at mode `0644`, and Zamboni refuses
a credential file that is group- or other-readable. A projected `zamboni.yml` or
`.env` fails at load with the `chmod 600` message — correctly, but confusingly:

```yaml
volumes:
  - name: zamboni-config
    secret:
      secretName: zamboni-credentials
      defaultMode: 0600        # Zamboni rejects 0644 on a file holding secrets
```

---

## 8. Telemetry

### What Iceberg already defines

**Iceberg defines a metrics standard**: the `MetricsReporter` / `MetricsReport` API
since 1.1.0, carrying `ScanReport` and `CommitReport`, transported by
`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` with a
`ReportMetricsRequest` body. `RESTMetricsReporter` is the **default** reporter when
Java uses a REST catalog.

**There is no OpenTelemetry semantic convention for Iceberg.** An OTel-backed
reporter has been proposed within the Iceberg project — emergent, not something to
comply with today.

**PyIceberg implements none of it.** Verified against 0.12.0: no metrics module, and
the REST catalog never mentions `ScanReport`, `CommitReport` or the metrics endpoint.
Tracked upstream as `iceberg-python#847`, open. So a Java Spark job reports commit
metrics through Lakekeeper by default and everything on PyIceberg is silent.

### Use the defined names

`CommitMetricsResult` defines the vocabulary, and Zamboni's operations map onto it
with almost no slack:

| Operation | Iceberg counters, already defined |
|---|---|
| `compact` | `added-data-files`, `removed-data-files`, `added-files-size-bytes`, `removed-files-size-bytes`, `added-records`, `removed-records`, `total-data-files`, `total-files-size-bytes` |
| `rewrite-manifests` | `manifests-created`, `manifests-kept`, `manifests-replaced`, `manifest-entries-processed` — an exact match |
| `remove-dangling-deletes` | `removed-delete-files`, `removed-positional-delete-files`, `removed-equality-delete-files`, `removed-dvs` |
| every operation | `total-duration` (timer), `attempts` (counter) |

**Build the report from the snapshot summary, not from our own counters.**
That is what Java does: `CommitMetricsResult.from(commitMetrics, snapshotSummary)`
reads all but two of its counters straight out of the summary the commit wrote.
Deriving them from `CompactionResult` instead would produce numbers that agree
with Iceberg's by coincidence and drift the first time either side changed what
it counted. Reading the summary means `removed-data-files` means exactly what it
means on a Java Spark job against the same table.

Verified against PyIceberg 0.12.0: **the summary keys it writes are Java's
keys**, so the mapping is total. What is *not* total is the spelling —
**nine of the twenty-four counters are renamed between the summary property and
the metric name**:

| summary property | metric name |
|---|---|
| `deleted-data-files` | `removed-data-files` |
| `deleted-records` | `removed-records` |
| `added-files-size` | `added-files-size-bytes` |
| `removed-files-size` | `removed-files-size-bytes` |
| `total-files-size` | `total-files-size-bytes` |
| `added-position-delete-files` | `added-**positional**-delete-files` |
| `removed-position-delete-files` | `removed-**positional**-delete-files` |
| `added-position-deletes` | `added-**positional**-deletes` |
| `removed-position-deletes` | `removed-**positional**-deletes` |

Copying the summary key through as the metric name looks right and is wrong on
nine counters, which is why this is a declared table rather than a loop.

Three consequences worth stating:

- **`operation` is Iceberg's, not ours.** The `CommitReport` field carries
  `replace` or `overwrite` — Iceberg's enumeration, which a consumer groups by.
  Zamboni's verb goes in the report's free-form `metadata` under
  `zamboni.operation`, and the metric names stay unprefixed and standard. The
  `zamboni.` prefix remains right for a *snapshot summary* key, where the
  namespace is shared.
- **Manifest counts are the one real gap, and Zamboni fills it.**
  `manifests-created` / `-kept` / `-replaced` and `manifest-entries-processed`
  are defined metrics that Java fills from summary properties **PyIceberg does
  not write**. Zamboni's rewriter counts them itself, so supplying them closes a
  gap rather than duplicating the summary. `manifests_before` / `manifests_after`
  are deliberately *not* mapped: they are totals, where Iceberg's three are a
  partition of the manifests one commit touched. A conversion, not a rename.
- **Reclaim has no Iceberg equivalent.** `remove-orphans` produces no snapshot and
  expiry's deletion half is outside the commit, so those keep their own names — but
  use Iceberg's **primitives**, `CounterResult {unit, value}` and
  `TimerResult {time-unit, count, total-duration}`, so a future reclaim report type
  is a mapping rather than a re-model.

**One Zamboni operation can be several Iceberg commits.** Compaction commits one
snapshot per rewrite group unless asked for a single commit, and Iceberg's unit
is the commit — so an operation produces a *list* of reports. `total-duration` is
the operation's, so it is attached only when there was exactly one commit;
copying it onto three would treble it for anyone summing. `attempts` is never
set: PyIceberg retries internally up to `commit.retry.num-retries` without
surfacing a count, and a hardcoded `1` would be a measurement nobody took.

### The reporter seam

**One report type is the currency; every destination is a transform over it** —
Iceberg's own architecture, a single-method `report(MetricsReport)`.

```
  operation result ──▶ CommitReport ──┬──▶ REST reporter → catalog /metrics
                       (Iceberg shape) ├──▶ OTel reporter
                                       ├──▶ as_dict() → integrator counters
                                       └──▶ noop (default)
```

`as_dict()` becomes a third reporter over the same report rather than a parallel
hand-maintained dict, so the integrator-facing counters and the telemetry cannot
drift.

| If upstream lands… | What changes here |
|---|---|
| PyIceberg metrics reporting (`iceberg-python#847`) | the report *builder* delegates to PyIceberg's; the REST reporter deletes in favour of PyIceberg's default |
| the proposed Iceberg OTel reporter | our OTel reporter is replaced by it, or contributes its instrument names |
| an Iceberg reclaim report type | our reclaim report maps onto it |

In every row **the seam survives and an implementation collapses**, which is the test
of whether the abstraction is in the right place.

### OpenTelemetry: API in the base, SDK in an extra

OTel splits **API** from **SDK**: `opentelemetry-api` is exporter-free and a **no-op
unless an application configures an SDK**. Libraries depend on the API; applications
choose the SDK.

| | Dependency | Result |
|---|---|---|
| base package | `opentelemetry-api` | instrumented, emits nothing on its own |
| embedded in an integrator | the integrator's SDK | telemetry appears in their pipeline, **no integration code** |
| the service | `zamboni[otel]` → SDK + OTLP | exports on its own |
| cron | nothing | no cost, no output |

This beats a registered-callback API: a published standard rather than a Zamboni
invention, and no adapter for anyone to write.

**Prometheus does not force an HTTP server** where an OTel Collector exists — push
OTLP, and the collector is scraped. Without one, a listener is needed, and that does
not mean a web framework: `prometheus_client.start_http_server()` is one thread over
`http.server`. The boundary is the shape, not the library — **two fixed paths, GET
only, no router, no bodies, no path parameters.**

### Reuse published conventions elsewhere too

| Concern | Use |
|---|---|
| NATS consumer | `messaging.client.consumed.messages` (`{message}`), `messaging.process.duration` (`s`), attributes `messaging.system=nats`, `messaging.operation.name`, `messaging.destination.name` |
| service identity | `service.name`, `service.version`, `service.instance.id` |
| commit-shaped facts | Iceberg's counter names mirrored under `iceberg.`, hyphens to underscores |
| reclaim and decision facts | ours — nothing defined upstream |

OTel's naming rules apply throughout: durations in **seconds**, units in the
instrument's unit field and **not** in the name, `{file}` and `{record}` as singular
annotations.

### The loop that closes this design

Telemetry that nobody reads is a component to operate, not a capability. The
plan this document proposes is **decided by a number that does not exist yet**,
so the route from a production run back to that number is part of the design
rather than an afterthought for whoever deploys it.

```
        run                      collect                    read              decide
  ┌──────────────┐          ┌───────────────┐        ┌────────────────┐   ┌───────────┐
  │ 1 cron + CLI │──json──▶ │ .jsonl on disk│──────▶ │  zamboni runs  │──▶│  monthly  │
  └──────────────┘          └───────────────┘        │  (per warehouse│   │  review   │
  ┌──────────────┐                                   │   breakdown)   │   │ devops §7 │
  │ 2 service    │──OTel──▶ ┌───────────────┐   ┌───▶└────────────────┘   └─────┬─────┘
  └──────────────┘          │  collector /  │───┘                               │
  ┌──────────────┐          │  IWS telemetry│                                   ▼
  │ 3 IWS embeds │─as_dict▶ └───────────────┘                          ┌────────────────┐
  └──────────────┘                                                     │ ZMBNI-106 gate │
                                                                       │ + standing     │
                                                                       │   health       │
                                                                       └────────────────┘
```

Deliberately, the **bottom rung needs no infrastructure at all**: a crontab
line, a file, and one command that reads it. Model 1 is how this will first
reach production, and a feedback loop that only works once a collector is
deployed is a feedback loop that does not exist during the period the gate is
being measured.

Two distinct questions come out of the same data, and conflating them is how a
monitoring plan turns into noise:

| | question | horizon | what it feeds |
|---|---|---|---|
| **one-off** | what share of scheduled work has no input? | a few weeks | the gate below: is event-driven triggering worth building? |
| **standing** | is maintenance keeping up? | forever | failures, exit codes, sweep duration. The reason the loop outlives the gate |

The skip share is **not an alert**. A high, stable figure means the cheap
watermark check is doing its job. What alerts is the standing column: failed
runs, exit 4, and a sweep duration trending toward the gap between cron firings.

### The decision record this initiative is waiting on

| | |
|---|---|
| **Decision** | build phases 4-6 (scheduler, NATS consumer, partition targeting), or close the initiative with phases 1-3 shipped |
| **Owner** | Paul |
| **Evidence needed** | `zamboni runs` over a production fleet, several nightly cycles, per warehouse |
| **Baseline** | 38% of scheduled work had no input, measured locally over 8 runs / 2 warehouses / 5 tables. **Not production**, and recorded only so the first production figure has something to be surprising against |
| **Decide by** | first monthly review with production data — [devops.md §7](devops.md) |

**What would make the answer "build it":** a large skip share *and* evidence
that the latency between a write and its maintenance matters — streaming tables,
or a warehouse where the nightly window is already too tight. The skip share
alone does not justify it, because phases 1-2 already capture that saving
without any event plumbing. **Phases 4-6 are a latency argument, not a waste
argument**, and the evidence has to be read that way or the gate will be
answered wrongly by a number that looks impressive.

---

## 9. Not a platform

Deliberately excluded: an HTTP API beyond liveness and metrics, a UI, OpenAPI, users,
roles, a job database. Zamboni is a maintenance engine. Adding those duplicates a
platform's job, and each is a component to secure, version and operate for no
capability the two config files do not already provide.

---

## 10. Risks

| Risk | Mitigation |
|---|---|
| Service wedges and nobody notices | liveness the deployment can probe; a last-successful-sweep timestamp per warehouse, and "no sweep in N intervals" is the alert |
| A skip hides work that was needed | skip only on "no new snapshots", a fact not a judgement; threshold-based skipping comes later and keeps the periodic floor |
| Partition targeting weakens §6.6 | orphan removal excluded by design; "targeted orphan removal" is a **non-goal** |
| Ingestion grows a dependency on NATS | advisory and fire-and-forget; the test is that killing NATS leaves ingestion and maintenance working, only slower |
| A long-running process holds credentials | the OAuth2 client-credentials path self-refreshes; a bearer token does not |
| Two instances maintain one table | single-instance until a claim protocol exists |
| Event storm during a backfill | the queue coalesces per table and debounces |

## 11. Open questions

1. **What fraction of scheduled runs currently do nothing?** ~~The due-check phase
   measures it~~ — the due-check phase *computes* it per run, which is not the
   same thing as anyone knowing it. Collecting it is `maintenance --json` and
   `zamboni runs`; deciding on it is the decision record in §8. Open until a
   production fleet has reported.
2. **How many tables per warehouse, realistically?** The ~25 ms/table figure is one
   dev-stack table. 50 tables makes polling free; 10,000 changes the case for events.
3. **Is per-partition detail cheap enough?** Iceberg's
   `write.summary.partition-limit` puts partition values in the summary up to a
   bound — worth testing whether setting it on ingested tables makes changed-set
   targeting nearly free.
4. **Do streaming tables exist on this stack?** The latency case rests on them. If
   everything is batch, the event's value is the end-of-write signal alone.
