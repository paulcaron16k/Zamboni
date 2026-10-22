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
  A · crontab ─────────────┐
  B · zamboni serve ───────┼──▶ maintain() ──▶ six operations
  C · the Lakehouse App ───┘     one run loop      unchanged
```

| | Who decides when | Config from | Process model |
|---|---|---|---|
| **A · cron** | crontab | files on disk | one short-lived process per run |
| **B · `zamboni serve`** | the runtime: schedules and events | files on disk, reloaded | long-running + worker pool |
| **C · the Lakehouse App** | the same runtime, embedded | the application's own database | the application's process + worker pool |

All three reach the same `maintain()`, so the exit-code contract, the consent rule
and the [§6.6 invariants](design.md) are inherited rather than reimplemented. The
service is an addition, not a replacement.

**Model C used to mean "the integrator decides when".** As of the
[maintenance runtime design](maintenance-runtime.md) it means an application
that embeds the *same* runtime `zamboni serve` uses, so scheduled and triggered
maintenance behave identically whether a fleet is driven by a config file or by
an application's database. That document is the contract such an application
implements.

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

Implemented by `zamboni.scheduler` (ZMBNI-120). What it settles, each tested in
`tests/test_scheduler.py`:

- **The queue holds `(warehouse, table)` keys, not config.** The `WorkItem` a
  worker receives is built from the fleet when it is *taken*, so a reload changes
  every decision not yet started and none in flight, and a warehouse a reload
  removed drops out of the queue without being cancelled. The key includes the
  warehouse because a table identifier is not unique across a fleet.
- **A table in flight is not re-enqueued** by an offer whose operations the run
  in flight already does — a tick during a scheduled run, a pressure check
  during any run: that run *is* the work asked for. **An event re-runs it once
  when it finishes** (#110, 2026-10-06), because the run may have read its
  watermark before the write the event reports; so, since #163, does any offer
  asking for an operation the run is not doing, such as a tick during an
  event-driven run. The due-check makes a re-run that finds nothing new cheap.
  Tested by `test_a_schedule_tick_or_pressure_during_a_run_is_dropped` and,
  over every pair of reasons,
  `test_an_offer_during_a_run_reruns_the_table_only_if_it_adds_work`.
- **A missed firing fires once, late** — a suspended host does not lose the
  night's run, nor replay six of them. **A fresh start waits for the next
  firing**, as cron does, so a rollout is not a fleet-wide sweep.
- **A reload keeps an unchanged warehouse's pending firing.** Re-arming everything
  from now would push acme's 02:00 run to tomorrow whenever globex was
  provisioned at 01:59.
- **The loop sleeps at most 60 s**, which bounds how late a firing can be after a
  wall-clock jump and how stale the liveness tick (#122) can get.

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

Implemented by `zamboni.pool` (ZMBNI-121). What it settles, each tested in
`tests/test_pool.py`:

- **A worker runs the CLI's own `maintenance` path** for one table, in-process,
  rather than a second implementation of it. Resolution order, session, engine,
  reporter, exit codes and the `--json` record are the code a cron line runs.
  What crosses the boundary is an argv of strings, the item's table config as a
  file, and the run record back — never a `CatalogSession`, and never a secret
  on a command line.
- **Long-lived workers, not a process per table.** Importing what a worker needs
  measured 1.7–2.1 s and ~174 MB (`/usr/bin/time`, three runs, 2026-10-01): for
  1,000 tables on 4 workers that is ~8 minutes of every sweep spent importing,
  for tables whose due-check takes ~25 ms. Workers are recycled after
  `RECYCLE_AFTER_TABLES` (50 — chosen, not measured).
- **Spawned, not forked.** A forkserver with the imports preloaded forks in
  ~17 ms (measured), but `pyarrow` and `duckdb` start native threads at import —
  1 OS thread before, 8 after — and forking a threaded process can leave a lock
  held by a thread that no longer exists.
- **A worker dying takes one table.** Not `ProcessPoolExecutor`, which breaks
  the whole pool when any worker dies. The table reports exit 1 — Python's own
  code for an uncaught crash, so it reads like a crashed CLI run — and a
  replacement worker takes the next one.
- **Every table starts from the environment the worker was given**, so a
  credential rotated in `.env` reaches the next table rather than the next
  worker; `load_env` lets the real environment win, and keys a previous table
  loaded would otherwise stay.
- **Sized from the cgroup, not `os.cpu_count()`.** The CPU quota and the memory
  limit are each the smallest up the cgroup tree (a pod's limit sits on a parent
  of the container's cgroup), v2 and v1. Workers are
  `min(CPUs, 0.8 × memory / (256 MiB baseline + memory_budget_bytes))`, and each
  gets `CPUs / workers` DuckDB threads through the new `--threads` flag.
  **Each worker's DuckDB gets its share of the plan** (ZMBNI-147):
  `0.8 × memory / workers − 256 MiB baseline`, through
  `--duckdb-memory-limit-bytes`, built by `WorkerConfig.from_pool` from the
  same `PoolSize` that chose the worker count. Left to its default, DuckDB
  plans for 80% of the memory it detects — and measured with DuckDB 1.5.5, it
  detects the process's *own* cgroup limit (819.1 MiB of a 1 GiB `memory.max`)
  but not a parent's (24.9 GiB of a 31.2 GiB host with the 1 GiB limit one level
  up). Either way N workers would each plan for most of the whole. With no
  cgroup limit the plan uses host RAM rather than declining to plan.
- **The feed takes from the queue only when a worker is free**, so a waiting
  table still coalesces and is still resolved against the newest config.
  Waiting for a worker is raced against stop: an earlier draft, blocked there,
  took and dropped a table after stop, and the test that caught it stays.

**Free-threaded Python would change this** and is worth watching rather than
depending on. Keeping the parallel unit at the table maps onto either model.

---

## 4. Configuration

Two files, split by who writes them.

**(a) The fleet file — written by the provisioner, read by Zamboni.** Warehouses and
their tables; conceptually a dump of the provisioning system's database. Nothing in
it is a secret.

```yaml
version: 1
warehouses:
  - name: acme
    uri: https://catalog.internal/catalog   # optional; zamboni.yml's otherwise
    schedule: "0 2 * * *"                   # UTC; the backstop, events drive the rest
    table_config:                           # a table-config.json body, inline
      namespaces:
        raw:
          tables:
            events:   {target_file_size_bytes: 134217728}
            sessions: {retention: {expire_snapshots: {max_snapshot_age_days: 7}}}
  - name: globex
    schedule: "30 3 * * *"
    random: false                           # exactly 03:30; spreading is on otherwise
    pressure: {delete_files: 20}            # this key over the defaults; false is off
    table_config: globex/table-config.json  # or a path, relative to this file
```

Implemented by `zamboni.fleet` (ZMBNI-118). The decisions it encodes, each
tested in `tests/test_fleet.py`:

- **The fleet file is the one source of which tables exist.** An integrator
  holding the same state in a database constructs `FleetConfig` in code — the
  same validated object, not a second store — and nothing writes the file back.
  Decided on #106, 2026-09-30.
- **It composes with `table-config.json`.** `table_config` is a path to an
  existing file or the same body inline, parsed by `TableConfig` either way; the
  tables maintained are that config's tables, not a second list. An inline body
  takes its `warehouse` from the entry, and a referenced file must name the same
  one.
- **Validated at construction, and an empty fleet is refused.** The file is
  generated, and a generator that fails open writes an empty list; accepting it
  on reload would stop every warehouse by being wrong.
- **`warehouses` is a list, not a mapping**, because PyYAML keeps the last of
  two duplicate keys silently. A warehouse listed twice is refused.
- **Schedules are crontab(5) five-field expressions in UTC**, with crontab's own
  rule that a restricted day-of-month *or* day-of-week matches. A schedule that
  can never fire (`0 0 30 2 *`) is refused rather than left to never run.
- **Firings are spread by default** (ZMBNI-144). A firing moves by up to
  ±`RANDOM_PCT` (5) percent of the gap to the next one, capped at
  ±`RANDOM_LIMIT_MINUTES` (30): a daily 02:00 lands between 01:30 and 02:30, a
  `*/5` moves by at most 15 s. `random: false` — fleet-wide at the top level, or
  per warehouse — fires on the exact minute, at DevOps' own risk.

  **On by default because the default is the worst-case mode.** The case to
  survive is a fleet provisioned from one template, every warehouse at
  `0 2 * * *`, against an object store shared with everything else scheduled
  on the hour. A default that is safe only when someone remembered to stagger
  the schedules is not safe under that case.

  **A fresh offset every night**, as systemd's `RandomizedDelaySec` draws one
  per iteration, so no pair of large warehouses is stuck colliding nightly. It
  is *derived* — a SHA-256 of the warehouse name and the nominal firing — not
  drawn, so a restart or reload at 01:45 does not re-roll tonight's 02:00; and
  it is not Python's `hash()`, which is salted per process and would. A firing
  moved early is still that firing: the next is armed from the nominal time,
  so it never fires twice.

  **Why it is needed even with a bounded pool:** the pool (#121) caps what *one*
  `zamboni serve` runs at once, so a fleet firing together becomes a backlog
  rather than a storm against that process. It does nothing for several
  deployments sharing one object store, for cron deployments
  ([devops.md §1](devops.md) shows the crontab equivalent), or for everything
  else in the estate scheduled on the hour.

**Delete pressure** (#175) brings a merge-on-read table's compaction forward
when deletes pile up between schedules. Each delete or update leaves a delete
file that every later reader opens beside the data files, so readers slow down
until compaction folds the deletes back in. Implemented by `zamboni.pressure`;
each decision is tested in `tests/test_pressure.py` and `tests/test_fleet.py`:

```yaml
pressure:                   # top level: the fleet's; per warehouse: over it
  delete_files: 10          # live delete files, of any kind
  delete_ratio: 0.10        # delete files per data file
  deleted_rows_ratio: 0.15  # position-deleted rows per row
  check_every_minutes: 15
```

- **On by default, at those values.** They are chosen starting values, not
  measurements (decided 2026-10-08). DevOps change them in the fleet file:
  `pressure: false` switches it off, `true` takes the defaults (or the fleet's),
  and a block sets only the keys it names. A threshold set to `null` is off; all
  three off is refused, because that means `false`.
- **Any one threshold met triggers**, at its value and not only above it. The
  table is offered with reason `pressure`, which runs `compact` and
  `remove-dangling-deletes`; the due-check still decides within them. A
  pressure offer is the weakest reason: a queued event or schedule run keeps
  its own operations, and one made while the table is running is dropped.
- **The signal is the snapshot summary**, the same one metadata load as
  `zamboni health`. A counter the summary did not report meets no threshold:
  "not reported" is neither zero nor over.
- **Equality deletes make a table ineligible.** Compaction refuses them, so an
  offer would be refused at every check; and `total-delete-files` counts both
  kinds together, so position delete files cannot be counted on their own.
  An unreported equality-delete count is treated the same way.
- **Measured in a worker, decided in the service.** Each check is
  `zamboni health --table-config … --json`, run through the pool like a table.
  The service reads the record and applies the thresholds without importing an
  engine (`test_the_service_process_imports_no_engine_to_decide`). Checks run
  one at a time. A warehouse is first checked one interval after it appears, at
  start or by reload, so a restart does not burst a check per warehouse.
  End to end: `test_a_real_worker_measures_a_merge_on_read_table_over_the_default_thresholds`.

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

Implemented by `zamboni.reload` and `zamboni config-reload` (ZMBNI-119). What it
settles, each tested in `tests/test_reload.py`:

- **A change is detected through the symlinks** — the inode, mtime and size of
  what the path resolves to — over the fleet file *and* every table config it
  references. A test swaps a ConfigMap-style `..data` link with an identical
  mtime, so only the inode and resolved path can tell.
- **A change must settle across two polls before it is loaded.** A writer caught
  rewriting the file in place can leave a *valid* file — a list of warehouses
  cut short — and adopting it would stop maintaining every warehouse after the
  cut. ConfigMap projection is atomic, so there this costs one 30 s poll inside
  the kubelet's own ~1 minute sync; anywhere else, write a temporary file and
  rename it. **SIGHUP loads at once**, because sending it says the file is done.
- **An invalid file keeps the running config**, is logged once, and is not
  re-parsed until it changes again. Its error is kept for the state file (#122).
- **`config-reload` refuses to signal a pid that is not zamboni.** A pid file
  outlives its process, pids are reused, and SIGHUP's default action is to
  terminate — so a stale pid file would otherwise kill an unrelated process.
  The check reads `/proc/<pid>/cmdline`; where `/proc` is absent it refuses.
- **The event subscription** (#119's last acceptance line) needs no
  re-subscribing: the subject comes from the profile, not the fleet file, and
  the feed maps each event against the fleet as reloaded at that moment, so a
  table added by a reload is matched from the next event. Changing `nats:`
  itself takes a restart, as any profile change does.

---

## 5. The event contract

One event, any Iceberg writer: `write.completed`, emitted **once per table when
the writer has finished with it** — *completed*, not *committed*. A catalog's
per-commit events fire during ingestion, when maintenance must not run; a writer
that has finished is when it should.

**The contract is [docs/write-completed.md](write-completed.md)**, version 1.0
(ZMBNI-123), with its JSON Schema published as `zamboni.get_write_completed_spec()`.
Build a producer against that, not against this section. An earlier draft of
this section had its own field table — `catalog`, a dotted namespace,
`operation: full-refresh`, a fixed subject — which the contract replaced: the
table is matched by **warehouse name** and a **list** of namespace levels, a
replaced table is `full_refresh: true`, the CloudEvents `type` is
`writeCompleted`, and the NATS subject is configured per environment. Why it has
that shape is [maintenance-runtime.md §5](maintenance-runtime.md).

What the contract keeps from the design:

- **Advisory.** A dropped event costs latency, never correctness, because every
  field is derivable from table metadata. Emitting never fails or slows the
  write.
- **At-least-once and unordered** on JetStream, at-most-once on core NATS. The
  consumer deduplicates on `(warehouse, table, snapshot_id)`; the event's
  deterministic `id` lets the server deduplicate too.
- **Bounded** — no record samples, no configuration, never a credential.

The CloudEvents envelope was chosen to match Lakekeeper's own events, so one
subscriber stack reads both. Nothing in the contract depends on Lakekeeper, but
what this repository says about its events was checked (ZMBNI-182) — in the
source of v0.13.1, the dev stack's pin (the event code is unchanged through
v0.13.5), and against live events from the dev stack, whose Lakekeeper publishes
on `lakekeeper.events`:

| Claim | Where it is settled |
|---|---|
| CloudEvents 1.0, **structured-mode JSON** | `CloudEventsPublisherBackgroundTask::publish` builds it with `EventBuilderV10`; `NatsBackend::publish` (lakekeeper-events-nats) sends `serde_json::to_vec(&event)`. Captured: `tests/fixtures/lakekeeper-0.13.1-updateTable.json`, checked by `test_lakekeepers_event_has_the_envelope_the_docs_say_ours_shares` |
| table commits are typed **`updateTable`**, camelCase like the rest (`createTable`, `dropTable`, `undropTabulars`) | `CloudEventsPublisher::transaction_committed` |
| **one per commit**: per table per committed request, after it succeeds, carrying the request (requirements and updates), not the resulting metadata | the same function; a single-table commit takes the same path (`commit_table` → `commit_tables_inner`). Live: `test_lakekeeper_publishes_one_update_table_per_commit_in_the_captured_shape` |
| the **id** is fresh per publication | `Uuid::now_v7()` — time-ordered, nothing derived from the write, so a consumer cannot deduplicate Lakekeeper's events by id. Ours is deterministic for that reason (`test_lakekeepers_event_is_the_commit_request_with_a_fresh_id`) |
| one subject, no hierarchy | one `LAKEKEEPER__NATS_TOPIC`, published to with core `client.publish` — never a JetStream publish, so no server acknowledgement |
| **at-most-once** | see below |

At-most-once holds, and the loss is in two places, measured with
`scripts/lakekeeper-event-loss.py` against the dev stack (property-update
commits on 4–16 tables at once, 100–190 commits/s on a developer machine):

- **In Lakekeeper, only while NATS is unreachable.** Events queue in a
  1,000-slot channel (`serve.rs`) and async-nats's 2,048-command client buffer
  (its default `sender_capacity`), and are sent on reconnect: 200 commits during
  a ~2 s outage all arrived. Past those ~3,048, each event is dropped after a
  50 ms wait (`CloudEventsPublisher::new`), logged as "Failed to emit event", and
  that wait is added to the commit: 4,000 commits through a 39 s outage stored
  3,049 and logged 951 drops. With NATS up, 4,000 commits lost none. At one
  commit a second, the buffers cover about 50 minutes of broker outage.
- **At the subscriber, whenever it is not connected.** Core NATS delivers only to
  whoever is subscribed at that moment: in the long outage the server got
  3,049 events and a subscriber that reconnected afterwards saw none of them.
  This is the loss that matters in practice — every restart of a core-NATS
  consumer drops what was published meanwhile — and it is the same for
  `write.completed` on core NATS, which is why `zamboni serve` prefers JetStream
  (`jetstream: auto`) and the schedule is the backstop for either.

**The governing principle: the event is a hint; the table is the truth.** A missed,
duplicated or out-of-order event degrades to the polling path.

**Not a command.** It states what happened; the consumer decides what that warrants,
against policy the consumer owns. A writer must never be able to order a deletion.

**Implemented** as [docs/write-completed.md](write-completed.md) (the contract,
ZMBNI-123) and `zamboni.events.feed` (the consumer, ZMBNI-124). Each received
message is parsed, mapped to a managed table by warehouse name, deduplicated on
`(warehouse, table, snapshot_id)`, debounced per table (5 minutes quiet, capped at
30 so a table written constantly is still maintained), and offered with
`reason=event`, which runs only `compact`, `rewrite-manifests` and
`remove-dangling-deletes`. Every message is acknowledged once handled — offered,
held for its quiet period, a duplicate, unmapped or malformed — and negatively
acknowledged only on an internal error, so a bad message is never redelivered
forever. A write reported while its table is being maintained re-runs that table
once when it finishes. A configured source that is disconnected makes the service
**unready, never unalive**; maintenance continues on the schedule.

The transport is `zamboni.events.nats` (`zamboni[nats]`), configured by a `nats:`
block or `ZAMBONI_NATS_*` and operated as [devops.md §8](devops.md), "Events
from NATS". `jetstream: auto` asks the account (`$JS.API.INFO`); on JetStream it
is a durable pull consumer, on core a subscription. The promise in §10's risk
table — killing NATS leaves maintenance working, only slower — is, for the
maintenance half (the ingestion half is the publisher's to prove),
`test_killing_nats_under_zamboni_serve_costs_readiness_and_nothing_else`, run in
CI against the dev stack's broker.

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

**One replica**, `strategy: Recreate`. `serve` holds its table claims in a
`LocalClaimStore` of its own, which excludes nothing across pods, so a
`RollingUpdate` briefly running two is still the case there is no interlock for.
Several replicas take a `ClaimStore` both can reach, which an application
embedding the runtime supplies ([maintenance-runtime.md §6](maintenance-runtime.md#6-claims)).

**A writable spill directory** — an `emptyDir` at `/tmp` on a read-only root
filesystem, or `temp_directory` pointed somewhere mounted.

Implemented by `zamboni serve`, `zamboni service-status` and `zamboni.service`
(ZMBNI-122), which since #162 assembles `serve` from the library runtime in
`zamboni.runtime` ([maintenance-runtime.md §3](maintenance-runtime.md)); the
deployment procedure is [devops.md §8](devops.md). What it
settles, each tested in `tests/test_service.py` (the sweeps, since #166 kept by
the runtime, in `tests/test_runstate.py`):

- **The state file** is written atomically from the runtime's `state()` on
  every tick, firing, table started or finished, reload and phase change:
  pid, phase (`starting` → `running` → `stopping` → `stopped`), last tick,
  config generation, workers and busy, `nats_connected` (`null` until #110),
  last error, the last refused reload, and per warehouse the last firing, the
  last result and the **last completed sweep**.
- **A sweep is the tables one firing actually asked for.** A table already in
  flight when the schedule fires is not in it — the queue refused that offer,
  so no result would ever close it. A first version merged a firing into the
  open sweep, and under a schedule faster than a sweep the sweep never closed;
  a test runs that schedule.
- **Probes check the pid is alive first**, then: startup — phase is running;
  readiness — running on a config (a refused reload leaves it valid; busy is
  not unready); liveness — a tick within 180 s, three missed 60 s ticks, and
  nothing external.
- **Workers ignore SIGTERM and SIGINT.** systemd's default
  `KillMode=control-group` and a terminal's Ctrl-C both reach the workers, and
  would kill tables mid-rewrite instead of letting the parent drain. So a worker
  is retired, or on a second signal SIGKILLed — never terminated.
- **`zamboni service-status` answers in 0.18 s** (measured, 2026-10-08), inside
  the 1 s exec-probe default. It took 1.4–1.5 s until ZMBNI-154, because every
  `zamboni` command imported PyIceberg, DuckDB and Arrow first; the console
  script now answers the probe verbs without them, and a test pins it. The
  manifest keeps `timeoutSeconds: 10` as margin.

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
  committing op ──▶ CommitReport  ──┬──▶ REST reporter → catalog /metrics
                    (Iceberg shape) ├──▶ OTel reporter
  no commit ─────▶ NoCommitReport ───┼──▶ Logging / Collecting reporters
                    (our shape,     └──▶ noop (default)
                     their primitives)
```

`NoCommitReport` is there because Iceberg's report describes a *commit*, and
`expire`, `remove-orphans` and `apply-properties` make none — nor do the other
three when they find nothing to do. Without it those runs would bypass the seam
entirely.

**`as_dict()` is deliberately *not* regenerated from the report.** The original
plan was for it to become a third reporter, which would have renamed every key
an integrator reads — a breaking change to the one surface added specifically so
that an integrator need not track our wording (ZMBNI-32). The anti-drift
property is obtained instead by *checking* that the two agree: Zamboni's count
of what a compaction rewrote and PyIceberg's count from the `DataFile` objects
it wrote are independent, so their agreeing is evidence, where a shared code
path would have agreed with itself and proved nothing.

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

| Concern | Use | Status |
|---|---|---|
| commit-shaped facts | Iceberg's counter names mirrored under `iceberg.`, hyphens to underscores | **shipped** (ZMBNI-129) |
| reclaim and decision facts | ours — nothing defined upstream | **shipped** |
| NATS consumer | `messaging.client.consumed.messages` (Counter, `{message}`), `messaging.process.duration` (Histogram, `s`), attributes `messaging.system=nats`, `messaging.operation.name`, `messaging.destination.name` | **shipped** (ZMBNI-181), under `serve --metrics otel` |
| what the consumer decided | `zamboni.event.outcome` on both, and `zamboni.events.offered` (Counter, `{table}`) — ours, the conventions have no notion of it | **shipped** (ZMBNI-181) |
| service identity | `service.name`, `service.version`, `service.instance.id` | `service.name` **shipped** (ZMBNI-188): `zamboni` unless `OTEL_SERVICE_NAME` says otherwise; the other two only through `OTEL_RESOURCE_ATTRIBUTES` |

The two messaging metrics were checked against
`open-telemetry/semantic-conventions@main`, and again when they were built
(6afd3ad, 2026-10-13). Two caveats the phrase "published convention" hides:
both metrics and every `messaging.*` attribute are marked **Development**, not
Stable, so they may still move; and **`nats` is not one of the convention's
well-known `messaging.system` values** — it allows a custom value where none
applies, and `nats` is ours.

Implemented as a `FeedObserver` the feed is handed (`zamboni.events.feed`), so
the feed imports no telemetry and the #157 runtime reuses it, and
`zamboni.otel.OTelFeedObserver` behind `serve --metrics otel` (ZMBNI-181). What
it settles, each tested in `tests/test_otel.py`, `tests/test_feed.py` and
`tests/test_nats.py`:

- **One count per delivery**, as the convention says, so a redelivered message
  counts again. The receive operation is `fetch` on JetStream (nats-py's pull)
  and `receive` on core; on JetStream the durable consumer is
  `messaging.consumer.group.name`.
- **`zamboni.event.outcome` on both metrics** — `pending`, `coalesced`,
  `duplicate`, `unmapped`, `rejected`, `error` — which is what tells "no
  events" from "events, all unmapped". The conventions have no attribute for
  what a consumer decided, so it is ours and namespaced.
- **`error.type` only where processing failed**, and only on the processing
  metric — receiving never failed. The documented list the convention asks
  for: `zamboni.events.EventError` for a malformed message, the exception's
  class for an internal error.
- **An observer can never fail the feed.** Anything it raises is logged — with
  its traceback the first time, at debug after that — and swallowed; every
  message is still settled exactly once.
- **Without the flag, nothing is imported.** A `serve` process that did not
  ask for telemetry loads no `opentelemetry` module.
- End to end, against the dev stack's NATS and a stand-in collector:
  `test_a_live_serve_exports_the_consumer_metrics_to_a_collector`.

OTel's naming rules apply throughout: durations in **seconds**, units in the
instrument's unit field and **not** in the name, `{file}` and `{record}` as singular
annotations, and UCUM for real units — bytes are `By`.

**Where OTel and Iceberg both have a rule about the same thing, OTel wins.**
Iceberg's `total-duration` is a field name; OTel's rule for a duration is
`<thing>.duration` in seconds, so the commit timer is `iceberg.commit.duration`
rather than `iceberg.total_duration`. The counters, where OTel has no opinion,
keep Iceberg's names exactly.

### The loop that closes this design

Telemetry that nobody reads is a component to operate, not a capability. The
plan this document proposes is **decided by a number that does not exist yet**,
so the route from a production run back to that number is part of the design
rather than an afterthought for whoever deploys it.

```
        run                      collect                    read              decide
  ┌──────────────┐          ┌───────────────┐        ┌────────────────┐   ┌───────────┐
  │ A cron + CLI │──json──▶ │ .jsonl on disk│──────▶ │  zamboni runs  │──▶│  monthly  │
  └──────────────┘          └───────────────┘        │  (per warehouse│   │  review   │
  ┌──────────────┐                                   │   breakdown)   │   │ devops §7 │
  │ B serve      │──OTel──▶ ┌───────────────┐   ┌───▶└────────────────┘   └─────┬─────┘
  └──────────────┘          │  collector /  │───┘                               │
  ┌──────────────┐          │  app telemetry│                                   ▼
  │ C app embeds │─as_dict▶ └───────────────┘                          ┌────────────────┐
  └──────────────┘                                                     │ ZMBNI-106 gate │
                                                                       │ + standing     │
                                                                       │   health       │
                                                                       └────────────────┘
```

Deliberately, the **bottom rung needs no infrastructure at all**: a crontab
line, a file, and one command that reads it. Model A is how this will first
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
| **Taken** | build, ahead of the evidence, by the owner — recorded on #106: phase 4 on 2026-09-30, phase 5 to make `zamboni serve` feature-complete for 0.6.0. The evidence still decides whether a deployment should *run* events, which is configuration, not code |

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
| Two instances maintain one table | `serve` is single-instance; replicas of an embedding application share a `ClaimStore` (maintenance-runtime.md §6) |
| Event storm during a backfill | the queue coalesces per table and debounces |

## 11. Open questions

1. **What fraction of scheduled runs currently do nothing?** ~~The due-check phase
   measures it~~ — the due-check phase *computes* it per run, which is not the
   same thing as anyone knowing it. Collecting it is `maintenance --json` and
   `zamboni runs`; deciding on it is the decision record in §8. Open until a
   production fleet has reported.
2. **How many tables per warehouse, realistically?** The ~25 ms/table figure is one
   dev-stack table. 50 tables makes polling free; 10,000 changes the case for events.
3. ~~**Is per-partition detail cheap enough?**~~ **Answered** (ZMBNI-125).
   `write.summary.partition-limit` works and writes `partitions.<path>` keys
   beside `partition-summaries-included: true`, so the changed set comes
   straight out of the summaries and no manifest is opened — measured against
   PyIceberg 0.12.0 on 2026-09-27.

   **But its default is `0`**, so a table that has not set it carries
   `changed-partition-count` and nothing else. The cheap path is real and
   **opt-in**, which is why both derivations exist: summaries where the table
   records them, and otherwise the manifests *added by* the snapshots since the
   watermark — still metadata, and a small subset rather than the whole-table
   walk `profile_table` does.
4. **Do streaming tables exist on this stack?** The latency case rests on them. If
   everything is batch, the event's value is the end-of-write signal alone.
