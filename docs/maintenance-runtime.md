# One maintenance runtime, three ways to run it

**Status: design, approved 2026-10-07; being built.** The plan of work is at the
end (§12). Where this document and the code disagree, the code has not caught up
yet or this document is wrong, and either way it is a defect.

**Built so far: R1 (#162), R2 (#163), R4 (#167) and R6 (#166), unreleased.**
`zamboni.runtime.MaintenanceRuntime` exists with the §3 control surface,
`start_in_thread` and `RuntimeHandle`, and `zamboni serve` is an assembly of it.
Every reason in §3.1 is accepted, offers merge as §3.2 says, and `offer` takes a
`not_after` deadline (§3.3). Observers and the full `state()` are as §8
describes, and `serve`'s state file and run log are two of those observers.
A table runs only while its claim is held (§6). `RuntimeSettings` has only the
two claim settings so far (§3.5), and nothing yet offers with a deadline but an
application's own `offer` call: a schedule's window arrives with
`WindowSchedule` (R3).

**What this settles.** Zamboni decides *how* to maintain a table safely. Since
[event-driven maintenance](event-driven-maintenance.md) it also decides *when*,
inside `zamboni serve`. This document moves that second half — schedules,
triggers, the queue, claims, the worker pool — into a **library runtime**, so
that a cron line, a headless service and a full application built on Zamboni all
get exactly the same behaviour. An application embedding Zamboni stops
re-implementing a scheduler beside it.

It is written so that a consumer can implement its side **exactly**. Every
interface below has stated semantics, every invariant is one a conformance test
can check, and §9 is the integration contract in full.

---

## 1. Three deployment models

| | **A · cron** | **B · `zamboni serve`** | **C · the Lakehouse App** |
|---|---|---|---|
| What it is | one `zamboni maintenance` per crontab line | a headless service driven by config files | an application that embeds Zamboni as a library and adds a web UI and an API: creating warehouses, dashboards, database administration, credentials |
| Who decides *when* | crontab | the runtime | the runtime |
| Where the fleet comes from | `table-config.json` per warehouse | the fleet file, reloaded on change | the app's own database, pushed to the runtime |
| Triggers | the crontab line | schedule, `write.completed` events | schedule, events, **plus** manual runs, reclaim after a drop, delete-file pressure |
| Concurrency | one process per line | the runtime's worker pool | the runtime's worker pool |
| More than one replica | not applicable | **no** — one replica, `strategy: Recreate` | **yes**, through a shared `ClaimStore` (§6) |
| If maintenance fails | the cron line fails; nothing else is affected | the service is the only thing affected | **the UI stays up**: the runtime runs in a separate maintenance tier, never in the web tier (§9.2) |
| Operator surface | the log, `--json`, `zamboni runs` | the state file, `service-status`, `--json` | the app's UI, built on runtime state and observer events |
| Owns | nothing persistent | two files | users, credentials, the warehouse registry, run history |

**"The Lakehouse App" is a role, not a product.** It names any application that
embeds the runtime to provide a lakehouse — warehouses, tables, access, and the
maintenance that keeps them healthy — behind a UI. This document defines what
such an app must implement; it does not name or describe any particular one.
Zamboni stays a public package any consumer can build on, and no consumer's
names appear in its documentation.

**All three reach the same `maintain()`**, so the exit-code contract, the consent
rule and the [§6.6 invariants](design.md) are inherited, not re-implemented. Cron
remains fully supported: it is the model with no moving parts, and the one
[devops.md](devops.md) recommends until a fleet outgrows it.

---

## 2. The layers

```
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ C · Lakehouse App      UI · API · registry DB · credentials · run history │
  │                        implements: FleetProvider (push), ClaimStore,      │
  │                        Observer, a worker entry                           │
  ├──────────────────────────────────┬────────────────────────────────────────┤
  │ B · zamboni serve                │                                        │
  │  fleet file · state file ·       │   (assemblies: each builds a runtime   │
  │  run log · NATS · SIGTERM        │    from its own parts)                 │
  ├──────────────────────────────────┴────────────────────────────────────────┤
  │ zamboni.runtime  — MaintenanceRuntime                                     │
  │   Scheduler (cron, window) · EventFeed · CandidateQueue · claims ·        │
  │   WorkerPool · deadlines · reasons → operations · state                   │
  ├───────────────────────────────────────────────────────────────────────────┤
  │ zamboni.events   — the write.completed contract, parsing, mapping,        │
  │                    dedup, debounce          (zamboni.events.nats: [nats]) │
  ├───────────────────────────────────────────────────────────────────────────┤
  │ zamboni          — maintain(), the six operations, health, watermark      │
  └───────────────────────────────────────────────────────────────────────────┘
          A · cron calls the bottom layer directly, through the CLI
```

A Lakehouse App runs the runtime in its **maintenance tier** and never in its
web tier; the web tier talks to it through the app's database (§9.2).

Each layer depends only on the ones below it. `zamboni.runtime` never imports a
transport; `zamboni.events.nats` is the only module that imports a NATS client,
and only under the `zamboni[nats]` extra.

---

## 3. `MaintenanceRuntime`

The runtime is one object owning one asyncio event loop's worth of work: the
scheduler loop, one feed loop per event source, the take-and-run loop, and claim
renewal. Its parts are injected; Zamboni ships a default for every one.

```python
class MaintenanceRuntime:
    def __init__(
        self,
        *,
        fleet: FleetProvider,
        pool: PoolSize,
        worker: WorkerConfig,                    # includes the worker entry, §7
        claims: ClaimStore | None = None,         # §6; None: a LocalClaimStore of its own
        events: Sequence[EventSource] = (),      # §5
        observers: Sequence[Observer] = (),      # §8
        settings: RuntimeSettings | None = None,  # None: every default, §3.5
        clock: Callable[[], datetime] = now_utc,
    ) -> None: ...

    async def run(self) -> None:
        """Run until stopped. Returns after in-flight tables finish."""

    # -- the control surface: safe to call from ANY thread --------------------
    def offer(self, warehouse: str, tables: Sequence[str] | None = None, *,
              reason: Reason) -> OfferReceipt: ...
    def update_fleet(self, fleet: FleetConfig) -> int: ...   # returns generation
    def state(self) -> RuntimeState: ...                     # a consistent snapshot
    def request_stop(self, *, force: bool = False) -> None: ...

def start_in_thread(runtime: MaintenanceRuntime, *, name: str = "zamboni-runtime") -> RuntimeHandle:
    """Run `runtime` on a dedicated thread with its own event loop (§9.2)."""

class RuntimeHandle:
    def stop(self, *, grace: timedelta) -> bool: ...   # True if drained within grace
    @property
    def runtime(self) -> MaintenanceRuntime: ...
```

**The control surface is the only thing another thread may touch.** Each method
hands its work to the runtime's loop (`call_soon_threadsafe`) and returns without
waiting for maintenance. Nothing else on the runtime is thread-safe, and nothing
else needs to be. The control surface is for code in the **same process** as the
runtime; another process — a web tier — reaches it through requests (§9.2).

`zamboni serve` becomes an assembly: a `FleetWatcher` as the provider, a NATS
source when `nats:` is configured, the state file and run log as observers, and
signal handlers calling `request_stop`. Its behaviour does not change.

As built in R1: the file-backed provider is `zamboni.reload.FileFleetProvider`;
an application that pushes every fleet change through `update_fleet` passes
`StaticFleet(fleet)`. Before `run` starts and after it returns there is no loop,
and a control-surface call is applied in the caller's thread under a lock — so an
offer made before start is queued and runs once the runtime does, and an offer
after stop is refused. `state()`, `update_fleet` and `offer` wait for the loop to
*apply* the call, which is what lets `offer` return an `OfferReceipt` saying, per
table, whether it was queued, merged into existing work, or not in the warehouse's
table config. `request_stop` does not wait. A runtime runs once.

### 3.1 Reasons, and what each runs

Every candidate carries the reason it was offered. **The reason decides which
operations run; the due-check still decides whether each one has anything to
do.** No reason bypasses a refusal, a preview, or an abort.

| Reason | Offered by | Operations | Window / deadline |
|---|---|---|---|
| `schedule` | a firing | all six, runbook order | the firing's deadline (§4) |
| `event` | `write.completed` (§5) | `compact`, `rewrite-manifests`, `remove-dangling-deletes` | none |
| `pressure` | delete-file pressure over threshold | `compact`, `remove-dangling-deletes` | none |
| `manual` | `offer()` from an operator action | all six | none — a manual run is not held to the window |
| `reclaim` | `offer()` after tables were dropped | `expire`, `remove-orphans` | none |

The `event` and `pressure` sets are the write-driven operations: `expire` and
`remove-orphans` answer to the clock, not to writes, and stay with the schedule
(`WRITE_DRIVEN` in `zamboni.maintenance` records why).

The table is declared once, as `zamboni.scheduler.REASON_OPERATIONS`, and the
tests derive from it rather than copying it. A reason it does not name is
refused where it enters — an offer, a candidate, a work item, a worker's argv —
because a lookup that defaulted would read it as "every operation" (ZMBNI-195).

### 3.2 Coalescing

The queue holds one entry per `(warehouse, table)`. A second offer for a queued
table **merges** into it rather than queueing twice:

- **operations**: the union, run in runbook order. The sets do not nest —
  `reclaim` and `event` share no operation — so keeping one offer's set would
  drop the other's;
- **deadline**: none if either has none, otherwise the later of the two;
- **reason** reported: the first of `manual`, `schedule`, `reclaim`, `event`,
  `pressure` among those merged; the others are kept as `also`. This is a
  reporting order only — what runs is the union above.

An offer for a table **in flight** is not queued behind it. If the run in flight
already does every operation the offer asks for, the offer is dropped: a tick
during a scheduled run, a pressure check during any run that compacts. Otherwise
it marks the table *dirty*, and when the run finishes the table is offered once
more with every such offer merged. An `event` always marks it dirty, because the
run may have read the table before the write it reports (#110, 2026-10-06); any
other reason does so only for operations the run is not doing (#163). The
due-check makes a dirty re-run that finds nothing new cost one metadata load.

A queued entry is resolved against the fleet **when a worker takes it**, so an
update changes every decision not yet started and none in flight, and a table
removed by an update leaves the queue silently.

### 3.3 Deadlines and stopping

A candidate may carry `not_after`. **A worker never starts a table after its
deadline**; the entry is dropped and reported as `deadline` (exit 0 — a window
closing is not a failure). A table already running finishes: maintenance is
never interrupted to honour a window, because stopping a rewrite mid-flight buys
nothing and wastes the work.

The deadline is checked with a worker already free — the last moment before the
table would start — so a table offered in time that waited out its window behind
others is dropped, not started late. It must carry a time zone; a naive one is
refused at the offer. `zamboni serve` writes a drop to its run log as a line with
`"dropped": "deadline"`, the deadline, exit 0 and no unit of work in any counter,
the way #201 leaves work a stop never started out of the counts, and the state
file's `last_sweep` counts it under `dropped`.

A **stop** — `request_stop`, or `maintain(should_stop=…)` for a caller without
a runtime (#201) — reaches further than a deadline, into a running table, so it
needs rules for what a table stopped part way looks like. One set, for both:

- **It is acted on only between whole commits:** before a table, before an
  operation, and between compaction's rewrite groups on the local engine. Never
  inside a commit, and never inside one engine statement on Trino or Spark.
- **Compaction stopped between groups commits what it already rewrote**, marked
  unfinished (`zamboni.compact.incomplete`, #196), so the next run compacts the
  rest without waiting for a write.
- **Once compaction has committed on a table, the stop waits for that table's
  `remove-dangling-deletes` and `rewrite-manifests`.** Stopped before them, the
  next run reads compaction's commit as "nothing written since" and skips them;
  on a quiet table, indefinitely. Waiting costs those two operations; the
  alternative, never skipping them while compaction's stamp is newest, re-runs
  them on every pass of a table where they commit nothing.
- **What completed is reported, and a stop is not a failure.** The report
  carries every outcome that finished plus `stopped` (the reason and each
  table and operation never started). `exit_code` is unchanged, as for a
  deadline drop, and work never started is not counted in `RunCounters`.
- **An exception from an observer is a stop, not a crash**, and is logged. A
  runtime observer is a *destination* (§8) and must not stop the runtime;
  `maintain()`'s single `observer` callback was its callers' only stop lever
  before `should_stop`, which is why the two differ.

A deadline only stops a table from starting; a stop also takes effect between
operations within a running table. `request_stop(force=True)` is the exception
to all of the above: like `zamboni serve`'s second stop signal today
(`Service.request_stop`), it kills in-flight workers, and relies on every
Iceberg commit being atomic rather than on these rules.

### 3.4 Priorities

A queue taken first-come-first-served makes "run now" wait behind a nightly
sweep. So every entry has a **class**, and a free worker takes the highest
class first:

| Class | Reasons | Within the class |
|---|---|---|
| **high** | `manual`, `reclaim` | first offered, first taken |
| **medium** | `event`, `pressure` | first offered, first taken |
| **low** | `schedule` | **oldest watermark first** — a table never maintained, then the one maintained longest ago |

- **Ageing.** An entry's class rises by one for every `priority_age_step` it has
  waited, so a busy event stream cannot starve the schedule.
- **Reserved capacity.** `reserved_high_slots` workers take only high-class
  entries, so an operator's "run now" starts as soon as one of them is free
  rather than after the sweep. The reservation is capped at `workers - 1`: one
  worker always serves the other classes.
- **Coalescing keeps the higher class** (§3.2).
- **No pre-emption.** A running table is never stopped to make room for a
  higher class; it finishes, and the next free worker takes the higher entry.

### 3.5 `RuntimeSettings`

| Setting | Default | Meaning |
|---|---|---|
| `debounce_quiet` | 5 min | an event-offered table waits this long after its last event |
| `debounce_cap` | 30 min | …but never longer than this after its first |
| `claim_lease` | 10 min | lease length per table claim (§6) |
| `claim_renew_every` | `claim_lease / 3` | renewal cadence |
| `reserved_memory_bytes` | 0 | memory the host process keeps for itself before the pool is sized (§9.4) |
| `reserved_cpus` | 0 | CPUs the host process keeps for itself before the pool is sized |
| `max_sleep` | 60 s | the longest the scheduler sleeps in one step |
| `table_timeout` | `schedule`, `manual`, `reclaim`: 6 h; `event`: 1 h; `pressure`: 2 h | wall-clock limit for one table, then SIGKILL (§7.1) |
| `stall_timeout` | 1 h | no progress report from a running table for this long, then SIGKILL (§7.1) |
| `recycle_after_tables` | 50 | a worker is replaced after this many tables; `1` gives a fresh process per table |
| `quarantine_after` | 3 | consecutive failures of one table before it backs off (§7.1) |
| `quarantine_backoff` | 1 h, doubling, at most 24 h | how long a quarantined table is skipped |
| `reserved_high_slots` | 1 | workers kept for high-class entries (§3.4) |
| `priority_age_step` | 2 h | waiting time that raises an entry one class (§3.4) |
| `worker_nice` | 10 | scheduling priority of workers; the runtime process is left at 0 |
| `worker_oom_score_adj` | 900 | makes a worker the kernel's preferred OOM victim (§7.1) |

Built so far: `claim_lease` and `claim_renew_every` (R4). The constructor
refuses a `claim_renew_every` at or above two-thirds of `claim_lease` — every
table would be killed before its first renewal
(`test_a_renewal_cadence_the_lease_cannot_honour_is_refused_at_construction`).

`reserved_memory_bytes` and `reserved_cpus` (R5) are arguments to
`zamboni.pool.plan_pool` and `size_pool`, not `RuntimeSettings` fields: the
runtime is handed a pool that is already sized (`pool=PoolSize`), so a setting
on it would be one it could only ignore. They come off the top of the cgroup's
limits before the pool is planned
(`test_the_reserved_memory_comes_off_the_top_of_the_plan`,
`test_the_reserved_cpus_come_off_the_top_of_the_plan`); one that leaves no room
still gets one worker and the "does not fit" warning
(`test_a_reservation_that_leaves_no_room_still_gets_one_worker_and_a_warning`).
`zamboni serve` has no flag for them yet; an application embedding the runtime
calls `plan_pool` itself and passes them there.

---

## 4. Schedules

`FleetWarehouse.schedule` is one of two types. Both produce **firings**: a
moment to offer the warehouse's tables, and a deadline.

```python
@dataclass(frozen=True)
class Firing:
    nominal: datetime      # what the schedule names
    due: datetime          # when it is actually offered (spread applied)
    not_after: datetime | None
```

### 4.1 `CronSchedule` — unchanged

Five-field crontab(5), evaluated in **UTC**, spread by default (±5% of the gap
to the next firing, capped at ±30 minutes; a fresh offset per firing derived from
the warehouse name and the nominal time). No deadline. See
[event-driven-maintenance.md §4](event-driven-maintenance.md#4-configuration).

### 4.2 `WindowSchedule` — new

For warehouses whose owners say "maintain me overnight, in my time zone".

```yaml
schedule:
  window:
    timezone: America/Toronto     # IANA name; required
    start_hour: 1                 # 0-23, local wall-clock
    end_hour: 5                   # 0-23, local; == start_hour means the whole day
    min_interval_hours: 18        # per table; see below
```

**Semantics — implement exactly:**

1. **The window is half-open, `[start, end)`, in local wall-clock time**, and may
   wrap midnight (`start_hour: 22, end_hour: 4`). `start_hour == end_hour` is a
   24-hour window.
2. **A firing happens when a window opens.** `nominal` is the opening instant;
   `not_after` is the closing instant. Spread, when `random` is on, is
   **forward only**: `due = nominal + offset`, with `offset` in
   `[0, min(5% of the window length, 30 min))`, derived exactly as for cron. A
   window must never start early.
3. **Daylight saving.** Local times are resolved with `zoneinfo`:
   - an opening or closing hour that **does not exist** that day (the spring
     gap) resolves to the first instant after the gap;
   - an hour that **occurs twice** (the autumn overlap) resolves to its
     **first** occurrence (`fold=0`);
   - so a window can be an hour shorter or longer on the two transition days,
     and never vanishes.
4. **`min_interval_hours` is per table, from the table itself.** A firing offers
   every table of the warehouse; a table whose
   [maintenance watermark](event-driven-maintenance.md) is younger than the
   interval is dropped at offer time and reported as `not due`. The runtime keeps
   no "last maintained" store: the watermark is the record, written into the
   table's own snapshot summary by every Zamboni commit.
5. **Validation at construction**: unknown timezone, hours outside 0-23, a
   negative interval, or a `min_interval_hours` **shorter than the window** are
   refused (the last would allow two runs in one window, which is never what was
   meant).
6. **A window already open at start** fires immediately (its `due` is in the
   past). This differs from cron's "a fresh start waits for the next firing"
   deliberately: a window is a period during which work is wanted, not an
   instant, and a restart inside it should not forfeit it.

Cron stays UTC-only; the time-zone behaviour lives in the type that needs it.

As built in R3 (`zamboni.fleet.WindowSchedule`, tested in
`tests/test_window_schedule.py`), with three things this section left open:

- **A window the spring gap would empty keeps its usual length.** `[2, 3)` in
  America/Toronto on the spring day resolves both ends to 03:00 EDT; it opens
  then and closes an hour later
  (`test_a_window_the_gap_would_empty_keeps_its_usual_length`). A gap longer
  than an hour is resolved to its end, not by `fold=0`, which lands an hour
  late in Antarctica/Troll
  (`test_an_hour_late_in_a_two_hour_gap_still_resolves_to_the_gap_s_end`).
- **`min_interval_hours` defaults to the window's length**, the shortest value
  point 5 accepts. A non-finite interval is refused with the rest.
- **The interval is checked in the worker, not at offer time.** The runtime
  process never loads a table, and reading the watermark is a metadata load,
  so the item carries the interval (`WorkItem.min_interval`) and the worker
  passes it to `maintain(min_interval=MinimumInterval(...))` — the same load
  as the write-driven due-check, so it costs nothing more. A table inside it
  reports every operation `not due` and runs nothing. The interval holds back
  only what the *schedule* asked for: an event merged into the scheduled run
  keeps its operations (`MinimumInterval.due_anyway`), and a merged `manual`
  run is not held back at all
  (`test_the_interval_holds_back_only_what_the_schedule_asked_for`). Any doubt —
  a watermark that cannot be read, a table never maintained — is due. The
  watermark moves only when Zamboni commits, so a table whose run found nothing
  to commit is due again at a restart inside the same window; that run costs
  its `expire` and orphan scan again, and commits nothing twice.

Zone data comes from the host's database, or else from the `tzdata` package,
which is a dependency for images that ship none
(`test_a_window_needs_no_zone_database_on_the_host`).

---

## 5. Events

### 5.1 The contract

`iceberg.table.write.completed`, CloudEvents 1.0, structured-mode JSON — the
envelope and wire shape Lakekeeper's events use, so one subscriber stack reads
both (checked against Lakekeeper v0.13.1 and live events in ZMBNI-182 —
[event-driven-maintenance.md §5](event-driven-maintenance.md)). **Zamboni defines and publishes it** as a JSON Schema,
`zamboni.events.get_write_completed_spec()`, versioned `major.minor` with a minor
increment strictly additive. Each producer publishes the schema of what it emits,
and a spec-diff test in the producer's repository catches drift.

| Field | Required | Meaning |
|---|---|---|
| `specversion` | yes | `"1.0"` |
| `type` | yes | `"writeCompleted"` |
| `source` | yes | `uri:<producer>:<host>` |
| `id` | yes | **deterministic**: `<warehouse>/<namespace…>/<table>@<snapshot_id>` |
| `data.warehouse` | yes | the warehouse **name** — the matching key |
| `data.namespace` | yes | a **list** of levels, never a dotted string |
| `data.table` | yes | table name |
| `data.snapshot_id` | yes | the last snapshot the writer committed |
| `data.completed_at` | yes | end of the write window, UTC |
| `data.table_uuid`, `data.warehouse_id`, `data.catalog_uri` | no | informational only (§5.2) |
| `data.writer`, `load_method`, `full_refresh`, `rows`, `data_files_added`, `bytes_added`, `partitions_changed`, `started_at`, `snapshot_count`, `streams` | no | what the write was; never used to decide |

**Completed, not committed.** A writer emits once, when it has finished with the
table for this run. A catalog's per-commit events fire during ingestion, when
maintenance must not run — Lakekeeper's `updateTable` fires once per committed
request, so a load committing in batches emits one per batch.

**The event is a hint; the table is the truth.** Every field is derivable from
table metadata. A lost event costs latency; a duplicated one costs a metadata
load; neither can cause work the schedule would not have done.

### 5.2 Mapping an event to a table

- **Match on `warehouse` name, exactly**, against the fleet; then
  `namespace` + `table` against that warehouse's table config.
- An event naming a warehouse or table the fleet does not manage is **dropped
  and counted** (`unmapped`), never an error.
- `catalog_uri` is never matched: the same catalog is reached by different URLs
  from different places. `warehouse_id` is not in the Iceberg REST specification
  — the `warehouse` parameter is "a location or identifier" and `prefix` is
  server-chosen — so it is one catalog's convention, not a key. `table_uuid`
  **is** standard (`table-uuid`, required from format v2) and is used only to
  report that a table was recreated since the write.
- **Environments are separated by subject**, not by payload: each deployment
  subscribes to its own prefixed subject.

### 5.3 `EventSource`

```python
class ReceivedEvent(Protocol):
    payload: bytes
    async def ack(self) -> None: ...
    async def nak(self) -> None: ...

class EventSource(Protocol):
    name: str
    def __aiter__(self) -> AsyncIterator[ReceivedEvent]: ...
    connected: bool          # for readiness; True for sources with no connection
```

**Ack rules — implement exactly:** ack once the event is **handled**: offered,
coalesced, deduplicated, dropped as unmapped, or rejected as unparseable (a bad
message is not redelivered forever). Nak only on an internal error before
handling. A source with no acknowledgement (core NATS) implements both as no-ops.

Zamboni ships `NatsEventSource` (`zamboni[nats]`):

- **`jetstream: auto | true | false`.** `auto` asks the account with a
  `$JS.API.INFO` request: an answer is JetStream, a 503 is core. The server's
  connect `INFO` (`"jetstream": true` only when enabled) was the first design and
  is not consulted: JetStream can be enabled for the server and not the account,
  so the account's answer is the one that decides — and nats-py keeps `INFO`
  private. Verified against nats-server 2.15.0 with nats-py 2.16 (2026-10-12).
  `true` against a core account refuses rather than falling back.
- On JetStream: a durable pull consumer per subject, at-least-once, starting at
  new messages when first created; the server deduplicates on `Nats-Msg-Id`.
  `max_deliveries` bounds redelivery after an internal error and applies when the
  consumer is created. With no `stream` named and none capturing the subject,
  one is created (`zamboni_<subject>`, kept 7 days) — the same best-effort
  provisioning the other services sharing these settings do. On core NATS:
  at-most-once — anything published while the consumer is not connected,
  including across its own restarts, is gone — and the schedule is the
  backstop.
- A broker that is down is retried forever; the source reports `connected`
  false meanwhile, which fails readiness and nothing else.
- **Setting names** are the set shared across the systems that use NATS beside
  Zamboni, without the env prefix: `servers`, `jetstream`, `username`,
  `password`, `token`, `tls`, `creds_file`, `max_deliveries`, `stream`, plus
  `subject`. NATS is enabled only when `servers` and `subject` are both set.

Any other broker is an `EventSource` away. A consumer with its own broker
abstraction writes a small adapter and passes it in.

### 5.4 The feed

One `EventFeed` per source turns received events into offers: parse → map → dedup
on `(warehouse, table, snapshot_id)` (bounded, in memory) → debounce (§3.5) →
`offer(reason="event")` → ack. All of it is in `zamboni.events`, with no
transport in it, so it is tested with synthetic events.

---

## 6. Claims

**A claim says "this replica is maintaining this table now."** Its job is to
stop two runtimes maintaining one table at once. That matters for safety, not
only for waste: orphan removal on one replica can delete the files of a
compaction still in flight on another, before it commits.

```python
@dataclass(frozen=True)
class Claim:
    warehouse: str
    table: str
    holder: str        # "<host>:<pid>:<runtime-uuid>"
    token: int         # fencing token, strictly increasing per (warehouse, table)
    expires_at: datetime

class ClaimStore(Protocol):
    def acquire(self, warehouse: str, table: str, *, holder: str,
                lease: timedelta, reason: str) -> Claim | None: ...
    def renew(self, claim: Claim, *, lease: timedelta) -> Claim | None: ...
    def release(self, claim: Claim) -> None: ...
```

**Semantics — implement exactly:**

1. **`acquire` is one atomic compare-and-set.** It succeeds if no claim exists
   for the table or the existing one has expired, and fails (returns `None`)
   otherwise. Never read-then-write.
2. **Time is the store's clock**, not the caller's. Expiry is compared against
   the database's `now()`, so replicas with skewed clocks agree.
3. **`renew` and `release` act only on the caller's own claim** — matching
   `holder` *and* `token`. A replica can never extend or clear another's claim.
   `renew` returns `None` if the claim is gone or expired.
4. **The token increases on every successful `acquire`**, across releases too,
   so a stale holder is never mistaken for the current one. The reference schema
   takes it from a sequence; a per-row counter would restart after a release.
5. **Claims are per table.** A warehouse is never claimed as a whole: an event
   for one table must be able to run while a sweep of the rest of its warehouse
   is in progress.

**What the runtime does with them:**

- A worker starts a table only after `acquire` succeeds. A failed acquire drops
  the entry as `claimed elsewhere` (exit 0;
  `test_a_table_claimed_elsewhere_is_dropped_without_running`). A store that
  raises fails the table unstarted, exit 1 — an unclaimed table must not start
  (`test_a_claim_store_that_cannot_answer_fails_the_table_unstarted`).
- The runtime renews every `claim_renew_every` while the table runs.
- **A claim that cannot be renewed stops its table.** A renewal answered `None`
  means the claim is gone, and the worker is killed at once
  (`test_a_claim_reported_gone_kills_its_worker_at_once`). A renewal that raises
  or hangs is retried, at a quarter of the cadence (chosen, not measured); if
  none has succeeded by two-thirds of the lease, the runtime SIGKILLs that
  table's worker — before the lease can expire and another replica can claim it
  (`test_a_claim_not_renewed_kills_its_worker_before_the_lease_expires`). A hard
  kill is safe (a compaction commits once, at the end; its written files become
  orphans for a later sweep); two replicas maintaining one table is not. The
  result's `died` says the claim was lost and why.
- `release` runs however the table ends, including when its worker died
  (`test_a_claim_is_released_however_the_table_ends`).

Zamboni ships `LocalClaimStore` (in-process; what `zamboni serve` uses, single
replica) and a **conformance suite**, `zamboni.conformance.claim_store_conformance`,
which every implementation must pass: concurrent acquires with exactly one
winner, expiry and re-acquire, token monotonicity, foreign renew and release
refused, store-clock expiry. It is not in `zamboni.testing`, which imports
private PyIceberg classes an implementer of a claim store has no use for. Each
check is a plain function raising `ConformanceError` with the rule it found
broken, so the suite runs under any test runner; `CLAIM_STORE_CHECKS` lets
pytest report each by name. `LocalClaimStore` passes it
(`test_local_claim_store_passes`), and every check is shown to reject a store
broken its own way (`test_every_check_rejects_some_broken_store`).

### 6.1 Reference schema (PostgreSQL)

```sql
CREATE SEQUENCE zamboni_claim_token;

CREATE TABLE zamboni_claim (
    warehouse        text        NOT NULL,
    table_identifier text        NOT NULL,
    holder           text        NOT NULL,
    token            bigint      NOT NULL,
    reason           text        NOT NULL,
    acquired_at      timestamptz NOT NULL,
    expires_at       timestamptz NOT NULL,
    PRIMARY KEY (warehouse, table_identifier)
);

-- acquire: one statement; a row comes back only on success
INSERT INTO zamboni_claim AS c
       (warehouse, table_identifier, holder, token, reason, acquired_at, expires_at)
VALUES (:warehouse, :table, :holder, nextval('zamboni_claim_token'), :reason,
        now(), now() + :lease)
ON CONFLICT (warehouse, table_identifier) DO UPDATE
   SET holder = EXCLUDED.holder, token = EXCLUDED.token, reason = EXCLUDED.reason,
       acquired_at = now(), expires_at = now() + :lease
 WHERE c.expires_at <= now()
RETURNING token, expires_at;

-- renew
UPDATE zamboni_claim SET expires_at = now() + :lease
 WHERE warehouse = :warehouse AND table_identifier = :table
   AND holder = :holder AND token = :token AND expires_at > now()
RETURNING expires_at;

-- release
DELETE FROM zamboni_claim
 WHERE warehouse = :warehouse AND table_identifier = :table
   AND holder = :holder AND token = :token;
```

`:lease` is an `interval`. A row is never deleted by anyone but its holder; an
abandoned row simply expires and is taken over by the next `acquire`, which is
also what makes a crashed replica harmless. A failed `acquire` still consumes a
sequence value; gaps in tokens are expected and mean nothing.

**Checked against PostgreSQL 16 (2026-10-07)**, statement for statement as above:
- the first acquire won; a second while held returned no row;
- renew by the holder succeeded; renew and release by another holder, or by a
  stale token after a takeover, returned no row;
- an expired claim was taken over;
- tokens kept rising after a release (1, 3, 4 — 2 was consumed by the refused
  acquire);
- **eight concurrent acquires of one free table produced exactly one winner.**

The conformance suite (§6) turns these into tests every implementation runs,
and `test_the_postgres_reference_claim_store_passes` runs it against the dev
stack's PostgreSQL with the statements above, read from this document by
`tests/claims_postgres.py` — so an edit here is an edit to what is tested.

---

## 7. Workers

The pool is [`zamboni.pool`](event-driven-maintenance.md#3-inside-the-service):
long-lived **spawned** processes, sized from the cgroup's CPU quota and memory
limit, each worker's DuckDB capped to its share, one table at a time per worker,
recycled after a number of tables, a dead worker costing one table.

**What changes: a worker receives the work item, not a command line.**

```python
@dataclass(frozen=True)
class WorkItem:
    warehouse: str
    table: str
    reason: str
    operations: tuple[str, ...]      # from §3.1, already merged
    table_config: TableConfig
    uri: str | None
    commit: bool
    not_after: datetime | None

# A worker entry: "module:function", importable in a fresh interpreter.
def run_table(item: WorkItem, context: WorkerContext) -> dict: ...
```

- **The default entry** builds the `zamboni maintenance <table>` command line and
  runs it in-process — today's behaviour, so `zamboni serve` and cron cannot
  drift.
- **A Lakehouse App supplies its own entry** to build the catalog session its way
  (its credential store, its storage settings, its credential-use policy) and
  then call `maintain()` with `tables=[item.table]` and
  `operations=item.operations`.
- `WorkerContext` carries the per-worker DuckDB thread count and memory limit,
  and the base environment the worker restores before each table.

**Rules for an entry — implement exactly:**

1. Importable by name in a **spawned** interpreter; no reliance on the parent's
   globals or open connections.
2. **No secret in a `WorkItem`.** Items are logged and appear in state; a worker
   reads credentials from its environment or its own store.
3. Return the run record — the shape `maintenance --json` writes — so every
   deployment's results read the same. A raised exception is recorded as exit 1.
4. Honour `item.commit`: **False previews.**
5. Pass `context`'s thread and memory limits to the session it builds.
6. Report progress (`context.progress(...)`) at least at every operation
   boundary, and within a long operation wherever it has a natural unit — the
   default entry reports each rewrite group. A long table that reports nothing
   is indistinguishable from a hung one (§7.1).

As built in R5 (`zamboni.pool`):

- **The entry** is `WorkerConfig.entry`, default `zamboni.pool:run_table`. That
  default builds the command line a cron run of the table would be and runs the
  CLI's `main` in-process, so the argv and the run record are unchanged: the
  argv tests in `tests/test_pool.py` pass as they were, and its real-worker
  tests run that path with only `main` replaced (`tests/pool_entries.py`).
  `run_cli(item, context, main=…)` is the path, for an entry that wants it.
- **How a table ends:** the returned record's `exit_code` is the table's. A
  raised `SystemExit(n)` is exit n with no record — the default entry's answer
  to a usage error, which writes none, as a cron run would. Any other
  exception is exit 1 with its traceback in the output, and so is a return
  that is not a record carrying an integer `exit_code`
  (`test_an_entry_that_returns_no_run_record_fails_its_table`), so a broken
  entry never reads as a success.
- **`item.commit` is the runtime's consent, always.** The queue never sets it;
  the pool stamps `WorkerConfig.commit` (`serve --yes`) on every item it sends,
  over whatever the item carried
  (`test_an_items_own_commit_never_outranks_the_runtimes`).
- **`WorkerContext`** carries the DuckDB threads and memory limit, a scratch
  `workdir`, the profile and env paths, and the base environment (out of its
  repr). It is built in the worker, never sent. `context.progress(note)` goes
  to the runtime over the worker's pipe, and `ItemResult.progress` keeps the
  last note even when the worker then dies
  (`test_progress_reaches_the_runtime_and_survives_the_worker`). The default
  entry reports once, as the table starts; reporting at each operation and
  rewrite group (rule 6) needs a hook through `maintain()` and arrives with
  R7, which is the first thing to read it.
- **A pressure check is not an entry's.** It always runs `zamboni health`
  through the CLI, whatever `entry` is: §7's signature is per table, and the
  check is the runtime's own measurement. An application whose catalog
  session cannot be built from the operator profile has no hook here yet.

### 7.1 When maintenance goes wrong

Isolation of maintenance from whatever hosts it is a requirement, not a
property to hope for, and it is bought with processes. These are the failure
cases and what the runtime does about each. **Each row is a story acceptance
criterion with a test that makes the failure happen.**

| Failure | Detected by | Action | Reported as |
|---|---|---|---|
| **A table runs too long** | wall clock, `table_timeout` by reason | SIGKILL the worker | `timed out`, a failure |
| **Hung or deadlocked** — including inside native code, where no Python runs | no progress report for `stall_timeout` | SIGKILL the worker | `stalled`, a failure |
| **Worker crashes**, including during startup | the pipe closes or errors — **any** `OSError` or `EOFError`, on send or receive | reset the slot; the next table gets a new worker | `died`, a failure. #155 fixes the startup case today |
| **The entry raises** | an uncaught exception (exit 1) — not exits 2, 3 or 4, which are the contract's refusals and aborts, and leave the process sound | **retire the worker**: a process that just raised is not trusted with the next table | exit 1, with the traceback in the run record |
| **Out of memory** | the kernel | workers set `oom_score_adj` high so **the kernel picks a worker**, never the runtime process; DuckDB is capped per worker (#147) | `died` |
| **The runtime's process dies** | each worker waits on its parent's sentinel | **the worker exits at once** | — |
| **A table fails every time** | `quarantine_after` failures in a row | skip it with exponential backoff; a `manual` offer overrides; a success clears it | `quarantined`, visible in `state()` and to observers |
| **CPU contention** | — | workers run at `worker_nice`; the pool is sized after `reserved_cpus` | — |
| **The runtime's own loop stops** | its tick goes stale | the host restarts the runtime (`serve`: liveness; Lakehouse App: its supervisor) | liveness failure |

Notes that decide the implementation:

- **Why the worker must die with its parent, and how.** Verified 2026-10-07: a
  worker survived its parent being SIGKILLed, re-parented and still running two
  seconds later. With claims that is unsafe. The dead parent stops renewing, the
  claim expires, and another replica can claim the table while the orphan is
  still maintaining it — the exact concurrency claims exist to prevent. The
  worker therefore runs a watcher thread that waits on
  `multiprocessing.parent_process().sentinel` and calls `os._exit` when it
  becomes ready. **Verified 2026-10-07**: a spawned worker started from a
  short-lived thread, as the pool starts them, exited within a second of its
  parent being SIGKILLed. Two alternatives are wrong: reading the job pipe from a
  second thread would steal job messages, and `PR_SET_PDEATHSIG` fires when the
  *thread* that spawned the worker exits, which for the pool's short-lived
  threads is almost immediately.
- **OOM steering is unprivileged.** Verified 2026-10-07: a process can raise its
  own `oom_score_adj` (to 900 in the test) without any capability; only lowering
  it below its starting value needs one.
- **A container may be killed whole.** Kubernetes 1.28 and later, on cgroup v2,
  set `memory.oom.group` for each container, according to its release notes —
  **not verified here**. If that holds, one OOM kill takes every process in the
  container, the runtime and anything sharing it included. Separate *processes*
  then do not protect a web server from a worker's OOM; separate *containers*
  do. That is the reason for §9.2's two tiers.
- **Capability detection runs in a worker**, not in the runtime's process. It
  performs a real overwrite on a scratch table to probe the installed PyIceberg
  (`capabilities.detect`), which is work the hosting process should not do.
- **Timeouts are not pre-emption.** A killed table commits nothing — a compaction
  commits once, at the end, and with partial progress only completed groups —
  and its written files are orphans for a later sweep. That is what makes SIGKILL
  a safe response to every row above.

---

## 8. Observing

```python
class Observer:                                   # zamboni.observe; every method a no-op
    def phase_changed(self, phase: str) -> None: ...       # running, stopping, stopped
    def ticked(self, at: datetime) -> None: ...             # every scheduler step: liveness
    def fired(self, warehouse: str, at: datetime, tables: Sequence[str]) -> None: ...
    def offered(self, receipt: OfferReceipt) -> None: ...   # each call to offer()
    def started(self, item: WorkItem, claim: Claim) -> None: ...  # holding its claim, §6
    def finished(self, result: ItemResult) -> None: ...     # ran, failed, died, or dropped
    def fleet_changed(self, generation: int, fleet: FleetConfig) -> None: ...
    def refused(self, kind: str, detail: str) -> None: ...  # "fleet": a candidate not adopted
    def pressure_checked(self, warehouse: str, found: Mapping[str, Any]) -> None: ...

def handed_off(observer, *, name: str | None = None) -> HandedOff: ...
```

Subclassing is optional: the runtime calls whichever of these methods an object
has, so an object defining only `finished` is an observer. `fired` names the
tables a firing asked for, which excludes any already in flight. `started` is
not called for a table dropped at its deadline or claimed elsewhere; `finished`
is, with `result.dropped` set to `"deadline"` or `"claimed elsewhere"`. `refused` is told once per new reason, not on
every poll of a fleet file that is still bad. An unparseable event is the
feed's to report (`FeedObserver`, event-driven-maintenance.md §8), not a
runtime observer's.

Observers are called **on the runtime's loop thread** and must return quickly:
anything slow — a database write, an HTTP call — delays the next firing and the
next table taken. **`handed_off(observer)` makes any observer safe to be slow**:
each call is queued and made, in order, on a thread of its own, and a stopping
runtime waits up to `CLOSE_TIMEOUT_SECONDS` (10 s, chosen rather than measured)
for each one to deliver what it has queued. The runtime never calls an
observer's own `close()`, which may mean anything to the application. **An
observer that raises is logged and ignored**, inline or handed off; a run that
did its work never fails over its reporting, and no observer can stop the
runtime. `test_a_raising_observer_changes_nothing` and
`test_a_handed_off_observer_does_not_delay_a_firing` hold both.

`zamboni serve`'s state file and `--json` run log are two observers
(`zamboni.service.StateFileObserver`, `RunLogObserver`). A Lakehouse App's run
history, dashboard and audit trail are a third.

`state()` returns a `RuntimeState` snapshot (`zamboni.runstate`) carrying
everything the state file does: the phase, generation and workers, the queue
and what is in flight, the last error and the last refused fleet, each event
source's counters and whether every source is connected, and per warehouse the
last firing, last result, last completed sweep and last pressure check. The
state file is `state()` plus what only `serve` knows — its pid, its fleet file,
whether `--yes` was given — and reads byte for byte as it did before the move
(`test_the_state_file_reads_as_it_did_before_the_runtime_kept_it`). A table is
never both queued and in flight in one snapshot
(`test_state_never_shows_a_table_both_queued_and_in_flight`), so a UI can
render the present without keeping its own copy.

---

## 9. The Lakehouse App: the integration contract

What an application embedding the runtime must do, in full.

### 9.1 What it owns, and what it does not

| The Lakehouse App owns | Zamboni owns |
|---|---|
| users, roles, authentication | which operations run, in what order, and which are refused |
| the warehouse registry, and turning it into a `FleetConfig` | schedules, triggers, the queue, deadlines |
| credentials, and building the catalog session (in its worker entry) | the claim protocol (§6) and when to take, renew, release |
| the `ClaimStore` over its database | the worker pool, its sizing, failure isolation |
| run history, dashboards, audit (as an observer) | the run record's shape and meaning |
| the UI and API, including "run now" and "drop with reclaim" | the safety invariants: preview without consent, refusals, aborts |

It does **not** run its own timer, its own subprocess-per-warehouse, or its own
lease over warehouses. Those are the runtime's.

### 9.2 Two tiers

**The web tier never runs maintenance, and never imports `zamboni.runtime`.**
A Lakehouse App is deployed as two tiers from the same image:

```
  web tier (Deployment: N replicas)          maintenance tier (Deployment: M replicas)
  ┌──────────────────────────────┐            ┌──────────────────────────────────────┐
  │ UI and API                   │            │ runtime process                       │
  │  "run now", "drop + reclaim" ├──INSERT──▶ │  ├── runtime loop: scheduler, feeds,  │
  │                              │  request   │  │   RequestSource, claim renewal     │
  │ dashboard  ◀──── SELECT ─────┤  table     │  └── workers (spawned, one table each)│
  │                              │  state     │ observers ──▶ run history, state rows │
  └──────────────┬───────────────┘  rows      └──────────────────┬───────────────────┘
                 └───────────────────── the app's database ──────┘
```

- **A crash, hang, OOM or CPU saturation in maintenance stays in the
  maintenance tier.** The UI keeps rendering from the database; when the
  maintenance tier's state rows go stale it says so ("maintenance unavailable
  since …") rather than failing.
- **Requests cross tiers as rows, not calls.** "Run now" and "drop with reclaim"
  are written to a request table; the maintenance tier reads it through a
  `RequestSource`, which is an `EventSource` (§5.3): a request becomes an
  `offer(reason="manual" | "reclaim")` and is marked handled — the same path,
  dedup and acknowledgement rules as an event. No new mechanism.
- **State crosses tiers as rows.** An observer (§8) writes `RuntimeState` and
  each run record to the database; the dashboard reads them. The web tier never
  needs `state()` directly.
- **The tiers scale and restart independently.** The maintenance tier's
  `terminationGracePeriodSeconds` covers its longest table; the web tier's is
  short.

Reference schema for the request table (PostgreSQL). Unlike §6.1, **not yet
exercised against a database**; R9 does that.

```sql
CREATE TABLE zamboni_request (
    id           bigserial   PRIMARY KEY,
    warehouse    text        NOT NULL,
    tables       text[],                 -- NULL: every table of the warehouse
    reason       text        NOT NULL CHECK (reason IN ('manual', 'reclaim')),
    requested_by text        NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT now(),
    handled_at   timestamptz,
    outcome      text                    -- offered | unmapped | refused: <why>
);

-- RequestSource, each poll: one replica takes each request
SELECT id, warehouse, tables, reason FROM zamboni_request
 WHERE handled_at IS NULL ORDER BY id
 FOR UPDATE SKIP LOCKED LIMIT 50;
-- ...offer each, then in the same transaction:
UPDATE zamboni_request SET handled_at = now(), outcome = :outcome WHERE id = :id;
```

**Inside the maintenance tier's process**, the runtime is the process's main
work; `start_in_thread` exists for hosts that have other work too.

**Single-process mode** — the runtime on a thread inside the web process — is
allowed for development and small installs, and is what `start_in_thread` is
for. What it gives up, stated plainly: a worker's OOM can take the web server
with it (see the container note in §7.1), the runtime's CPU and memory compete
with requests, and a fault in the runtime's own code is in the web process. Set
`reserved_memory_bytes` and `reserved_cpus` for the web server, and run one
process per pod.

### 9.3 More than one replica

Every replica runs its own runtime against the same fleet and the same
`ClaimStore`. Every replica fires; every replica offers; **exactly one wins each
table's claim** and the others drop it as `claimed elsewhere`. No leader
election is needed, and losing a replica loses only the leases it held, which
expire.

### 9.4 Memory

The pool sizes itself from the cgroup's limit. In the maintenance tier that limit
covers only the runtime and its workers, so `reserved_memory_bytes` is the
runtime process's own baseline. In single-process mode it must also cover the
web server. Either way the pool plans against the rest. A pod too small for one
worker plus the reservation still gets one worker, and a warning says the plan
does not fit.

### 9.5 Conformance

An implementation is conformant when:

1. its `ClaimStore` passes `zamboni.conformance.claim_store_conformance`;
2. its worker entry passes `zamboni.conformance.entry_conformance` (importable
   when spawned, previews without `commit`, returns a run record, carries no
   secret in its inputs) — in `conformance`, not `zamboni.testing`, for R4's
   reason: `testing` imports private PyIceberg classes an implementer does not
   need. The checks run the entry in a spawned worker of a real `WorkerPool`;
   the default entry passes them against a table with work to do
   (`test_the_default_entry_passes`), and each is shown to reject an entry
   broken its own way;
3. it never calls the runtime except through the control surface, and its web
   tier never imports `zamboni.runtime` — a test imports the web tier's entry
   point and asserts the module is absent from `sys.modules`;
4. it pushes every registry change through `update_fleet()`;
5. its `RequestSource` passes the event-source checks in `zamboni.testing`
   (handled requests acknowledged once; a request offered on one replica is not
   offered on another).

---

## 10. What does not change

- `maintain()` and the six operations; the exit codes; the consent rule; the
  §6.6 invariants.
- Cron (model A), entirely.
- `zamboni serve`'s files, flags, probes and shutdown behaviour.
- The event is advisory; the schedule is the floor; no trigger can order a
  deletion.

---

## 11. Not decided here

- A `ClaimStore` shipped *by* Zamboni for PostgreSQL (`zamboni[postgres]`). It
  would let `zamboni serve` run more than one replica. Deferred: no consumer
  needs it yet, and the reference SQL plus the conformance suite are enough to
  build one.
- The delete-file-pressure thresholds for `reason="pressure"`. The signal exists
  (the delete-file counts the profiler reports); the policy wants measurements
  from real merge-on-read tables.
- Whether Kubernetes' container-wide OOM kill applies on the clusters in use
  (§7.1). It strengthens the case for two tiers; it does not change the design.
- The stall timeout's default. One hour assumes progress reports at least every
  rewrite group; a table whose single largest group takes longer needs a larger
  value, and no measurement of that exists yet.
- Whether `RuntimeState` is exposed over HTTP by `zamboni serve`. Today the
  answer is no (event-driven-maintenance.md §9: not a platform).

---

## 12. Plan of work

Phase 4 shipped `zamboni serve`. This plan extracts its runtime first, then
builds the event path on it, then lets a Lakehouse App adopt it.

| # | Story | Depends on | Delivers |
|---|---|---|---|
| R1 | **Extract `MaintenanceRuntime`** from `zamboni.service`; `serve` becomes an assembly; `start_in_thread` and the thread-safe control surface | — | §3, with `serve`'s behaviour unchanged and its tests unchanged |
| R2 | **Reasons, operations and coalescing**: `Candidate.reason`, merged operation sets, the dirty re-offer, deadlines | R1 | §3.1–§3.3 |
| R3 | **`WindowSchedule`**: time zones, daylight saving, forward-only spread, per-table minimum interval from the watermark; fleet-file syntax | R2 | §4.2 |
| R4 | **Claims**: the `ClaimStore` protocol, `LocalClaimStore`, renewal and kill-on-loss, the conformance suite, the reference schema | R1 | §6 |
| R5 | **Worker entries**: the work item crosses the boundary instead of argv; `WorkerContext`; `reserved_memory_bytes` in pool sizing; the entry conformance checks | R2 | §7, §9.4 |
| R6 | **Observers and `state()`**: the `Observer` protocol; the state file and run log rewritten as observers | R1 | §8 |
| R7 | **When maintenance goes wrong**: per-reason table timeouts, stall detection from progress reports, retire-on-failure, the parent-death watcher, `oom_score_adj` and `nice`, quarantine with backoff, capability detection moved into a worker | R5 | §7.1 — every row tested by making it happen |
| R8 | **Priorities**: classes, oldest-watermark-first, ageing, reserved high-class slots | R2 | §3.4 |
| R9 | **Requests from another process**: the `RequestSource` contract, its conformance checks, the request-table reference schema run against PostgreSQL, a state-persisting observer reference | R6, E2 | §9.2 |
| B1 | **A worker that dies at startup leaves its slot broken** (#155) | — | fixes merged code now; R7 generalises it |
| E1 | **The write-completed contract** (#123): JSON Schema, `get_write_completed_spec()`, parser, mapping | — | §5.1–§5.2 |
| E2 | **Event feed and NATS source** (#124): `EventSource`, `EventFeed` (dedup, debounce), `NatsEventSource` with `jetstream: auto`, NATS in the dev stack, the kill-NATS test | R2, E1 | §5.3–§5.4 |
| E3 | **Delete-file pressure trigger** | R2 | `reason="pressure"`, once §11's thresholds are measured |
| S1 | **`service-status` without the engine** (#154) | — | the probe answers inside a 1 s timeout |

**Order:** B1 first — it fixes shipped code. Then R1 → (R2, R4, R6 in parallel)
→ (R3, R5, R8) → R7 → E2 → R9. E1 can start immediately. R1-R9 form one epic;
E1-E3 belong to #110.

**Outside this repository**, tracked by their owners: a writer emitting
`write.completed`, with a spec-diff test against E1; aligning NATS setting names
across the systems that share a broker; and each Lakehouse App's adoption of §9,
which starts once R1-R9 are released.

**Release.** `zamboni.runtime`'s protocols are new public API. They ship in a
minor release and are marked **provisional** in that release's changelog — the
first consumer's adoption is what proves them — and become stable in the
release after, with any change in between called out as `BREAKING`.
