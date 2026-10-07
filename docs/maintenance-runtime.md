# One maintenance runtime, three ways to run it

**Status: design, approved 2026-10-07. Not yet built.** The plan of work is at the
end (§12). Where this document and the code disagree, the code has not caught up
yet or this document is wrong, and either way it is a defect.

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
        claims: ClaimStore = LocalClaimStore(),  # §6
        events: Sequence[EventSource] = (),      # §5
        observers: Sequence[Observer] = (),      # §8
        settings: RuntimeSettings = RuntimeSettings(),
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

### 3.2 Coalescing

The queue holds one entry per `(warehouse, table)`. A second offer for a queued
table **merges** into it rather than queueing twice:

- **operations**: the union, run in runbook order;
- **deadline**: none if either has none, otherwise the later of the two;
- **reason** reported: the one whose operation set is larger (`manual` and
  `schedule` over `reclaim` over `event` and `pressure`); the others are kept as
  `also` for reporting.

An offer for a table **in flight** is not queued behind it. It marks the table
*dirty*; when the run finishes, the table is offered once more with the merged
reasons of every offer that arrived meanwhile. The due-check makes a dirty
re-run that finds nothing new cost one metadata load.

A queued entry is resolved against the fleet **when a worker takes it**, so an
update changes every decision not yet started and none in flight, and a table
removed by an update leaves the queue silently.

### 3.3 Deadlines

A candidate may carry `not_after`. **A worker never starts a table after its
deadline**; the entry is dropped and reported as `deadline` (exit 0 — a window
closing is not a failure). A table already running finishes: maintenance is
never interrupted to honour a window, because stopping a rewrite mid-flight buys
nothing and wastes the work.

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

---

## 5. Events

### 5.1 The contract

`iceberg.table.write.completed`, CloudEvents 1.0, structured-mode JSON — the
envelope and wire shape Lakekeeper's events use, so one subscriber stack reads
both. **Zamboni defines and publishes it** as a JSON Schema,
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
maintenance must not run.

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

- **`jetstream: auto | true | false`.** `auto` reads the server's connect `INFO`
  (`"jetstream": true` is present only when JetStream is enabled), then confirms
  with a `$JS.API.INFO` request, because JetStream can be enabled for the server
  and not the account; a plain server answers 503 no-responders. Verified against
  nats-server 2.15.0 (2026-10-06).
- On JetStream: a durable pull consumer, at-least-once, and the server
  deduplicates on `Nats-Msg-Id`. On core NATS: at-most-once, and the schedule is
  the backstop.
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
  the entry as `claimed elsewhere` (exit 0).
- The runtime renews every `claim_renew_every` while the table runs.
- **A claim that cannot be renewed stops its table.** If renewal has not
  succeeded by two-thirds of the lease, the runtime SIGKILLs that table's worker
  — before the lease can expire and another replica can claim it. A hard kill is
  safe (a compaction commits once, at the end; its written files become orphans
  for a later sweep); two replicas maintaining one table is not.
- `release` runs however the table ends, including when its worker died.

Zamboni ships `LocalClaimStore` (in-process; what `zamboni serve` uses, single
replica) and a **conformance suite**, `zamboni.testing.claim_store_conformance`,
which every implementation must pass: concurrent acquires with exactly one
winner, expiry and re-acquire, token monotonicity, foreign renew and release
refused, store-clock expiry.

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

The conformance suite (§6) turns these into tests every implementation runs.

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
class Observer(Protocol):
    def fired(self, warehouse: str, firing: Firing, tables: Sequence[str]) -> None: ...
    def offered(self, receipt: OfferReceipt) -> None: ...
    def started(self, item: WorkItem, claim: Claim) -> None: ...
    def finished(self, item: WorkItem, record: dict) -> None: ...
    def fleet_changed(self, generation: int, fleet: FleetConfig) -> None: ...
    def refused(self, kind: str, detail: str) -> None: ...   # a bad fleet, an unparseable event
```

Observers are called **on the runtime's loop thread** and must return quickly:
anything slow — a database write, an HTTP call — is handed to the observer's own
thread or queue. **An observer that raises is logged and ignored**; a run that
did its work never fails over its reporting.

`zamboni serve`'s state file and `--json` run log become two observers. A
Lakehouse App's run history, dashboard and audit trail are a third.

`state()` returns a `RuntimeState` snapshot — the fields the state file carries
today, per warehouse and per table in flight — so a UI can render the present
without keeping its own copy.

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

1. its `ClaimStore` passes `zamboni.testing.claim_store_conformance`;
2. its worker entry passes the entry checks in `zamboni.testing` (importable
   when spawned, previews without `commit`, returns a run record, carries no
   secret in its inputs);
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
