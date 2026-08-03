# HIMS Discharge Demo — Requirements

A developer-facing demo that shows, over five days of simulated hospital ingestion, what
small-file accumulation does to an Iceberg table and what `icemaint` maintenance does about
it.

**Audience:** developers evaluating the maintenance tooling.
**Shape:** a sequence of commands with the developer free to inspect and query in between.

---

## 1. Narrative

```
./bin/demo clear                 # empty catalog, DAYS_INGESTED=0
./bin/demo next-day              # ingest day 1, then print status automatically
./bin/demo next-day              # ingest day 2 ...
./bin/demo status                # re-print status on demand
./bin/demo query                 # list discharges and events, with timings
./bin/demo maintenance           # run icemaint, then print status automatically
./bin/demo query                 # same queries, compare
...
./bin/demo next-day              # after day 5 -> "No More Data"
```

The intended arc: **ingest → status → query → maintain → status → query**, so the developer
sees file counts and query cost before and after.

---

## 2. Domain model

30-bed hospital, five consecutive days.

### 2.1 Volume

| Parameter | Value | Note |
|---|---|---|
| Beds | 30 | |
| Daily turnover | 30% ± 10% (relative) | 8–10 discharges/day; **assumption**, see §7 |
| Discharges over 5 days | ~45 | |
| Events per discharge | 14 happy path (15 with failure) | |
| Event rows over 5 days | ~630 | |

### 2.2 Timing

| Phase | Window |
|---|---|
| `pending_discharge_logged` | from 08:00 |
| Discharge initiated (`attending_doctor_initiated`) | 09:00–13:00 |
| Intermediate clinical/financial events | scattered statistically between initiation and completion |
| Discharge completed (`patient_displaced_discharged`) | 15:00–18:00 |
| EVS events (`bed_cleaning_*`, `cleaning_priority_assigned`) | **after** the room is empty |

### 2.3 Tables

#### `hims_employees` — master data

Full-table replace (or upsert) each day. **No incremental replication key.**

| Column | Type | Note |
|---|---|---|
| `employee_id` | string | PK |
| `role` | string | `doctor`, `nurse-staff`, `nurse-mgr`, `pharma-mgr`, `billing-mgr`, `transport-staff`, `evs-staff` |
| `full_name` | string | |

#### `hims_discharge` — process instance

| Column | Type | Note |
|---|---|---|
| `process_type` | string | constant `discharge` |
| `process_id` | string | = `discharge_id`; PK |
| `status` | string | FSM state, see §2.4 |
| `patient_id` | string | |
| `attending_doctor_id` | string | FK → `hims_employees` |
| `scheduled_at` | timestamp | **nullable** |
| `created_at` | timestamp | when this row first appeared |
| `updated_at` | timestamp | **replication key** |
| `initiated_at` | timestamp | set on `attending_doctor_initiated` |
| `completed_at` | timestamp | set on `patient_displaced_discharged` |

#### `hims_events` — event stream

| Column | Type | Note |
|---|---|---|
| `event_id` | string | UUIDv7, PK |
| `occurred_at` | timestamp | **replication key** |
| `event_name` | string | see [HIMS_Discharge_Process_Events.md](HIMS_Discharge_Process_Events.md) |
| `actor_id` | string | **nullable** — system-generated events have no actor |
| `process_type` | string | **added**, see §7 |
| `process_id` | string | **added**, see §7 — links the event to a discharge |
| `data` | string | JSON text, event-specific properties. **VARCHAR not VARIANT**, see §7 |

### 2.4 Discharge state machine

```
                       ┌──────────────────────────────┐
                       ▼                              │ (restart next day,
  pending ─→ initiated ─→ nurse_cleared ─→ pharma_cleared   same process_id)
                                                │     │
                                                ▼     │
                                        billing_pending
                                         │          │
                       billing_clearance │          │ billing_clearance_failed
                                         ▼          ▼
                                  billing_cleared  cancelled ──┘
                                         │
                    patient_displaced_discharged
                                         ▼
                                    discharged   ← terminal; EVS events continue
```

Two rules the demo depends on:

- **`discharged` is terminal.** EVS events keep arriving against the same `process_id`
  after the patient has left; the discharge row does not change again.
- **Cancellation is not terminal.** Three discharges hit `billing_clearance_failed`
  (`status = cancelled`) and are **restarted the next day with the same `process_id`**.
  That update to an existing row is the whole reason copy-on-write and merge-on-read differ.

---

## 3. Ingestion

### 3.1 Source files

```
data/healthims/
├── day1/  employees.csv  discharges.csv  events.csv
├── day2/  …
└── day5/  …
```

CSVs are generated once and committed, so the demo is deterministic and reviewable.

### 3.2 Write modes

`./bin/demo mode [cow|mor]` selects how day-over-day updates are applied.

| Mode | Behaviour | Implementation |
|---|---|---|
| `cow` | Updated rows rewrite their whole data file | PyIceberg `upsert` / `overwrite` |
| `mor` | Updated rows get a position delete plus a new row | **Hand-written position delete files**, see §7 |

### 3.3 Micro-batching

Each day is ingested as **several small files, not one**, to reproduce the streaming
small-file condition the tool exists to fix. Default: one batch per simulated hour of
activity (~10 batches/day → ~50 event files over 5 days).

Without this the demo has nothing to compact. See §7.

---

## 4. Storage

| Thing | Location |
|---|---|
| Iceberg catalog | `data/healthims/iceberg_catalog.db` (SQLite) |
| Iceberg warehouse | `data/healthims/iceberg_warehouse/` |
| Demo state | `data/healthims/demo.env` |
| Logical schema | `data/healthims/table_schema.json` |
| Layout config | `data/healthims/table-config.json` |

`demo.env`:

```
WRITE_MODE=cow          # cow | mor
DAYS_INGESTED=0         # 0 = cleared, 1 = day 1 ingested, … 5 = all days
```

---

## 5. Layout optimisation

Declared in `table-config.json`, consumed by `icemaint`.

| Table | Partition | Ordering | Rationale |
|---|---|---|---|
| `hims_employees` | none | none | Tiny; compaction only keeps file count down after daily replaces |
| `hims_discharge` | `day(created_at)` | **z-order(`updated_at`, `process_id`)** | Two independent access paths: incremental reads filter `updated_at`; lookups filter `process_id`. A sort favours whichever is leading; a z-order serves both |
| `hims_events` | `day(occurred_at)` | **z-order(`process_id`, `occurred_at`)** | Partition prunes the day; within it, process-trace lookups filter `process_id` and sub-day slices filter `occurred_at` |

Partitioning `hims_discharge` by `created_at` rather than `updated_at` keeps a row in one
partition for life, so a restart-next-day update rewrites within the original partition —
which is what makes the CoW/MoR contrast visible.

### 5.1 Sort options for daily metric computation

Candidate daily metrics: discharges/day, median time from `initiated_at` to `completed_at`,
EVS turnaround (`patient_displaced_discharged` → `bed_cleaned`), per-role event counts.

| Metric shape | Suggested layout | Why |
|---|---|---|
| Per-day counts and rates | `day()` partitioning already present | Metric grain equals partition grain; whole-partition scan, no pruning needed |
| Time-in-state (join events to their discharge) | z-order(`process_id`, …) on both tables | Join key clusters on both sides, so matching rows sit in fewer files |
| Per-event-name funnel counts | *alternative:* z-order(`process_id`, `event_name`) on `hims_events` | Trades sub-day time pruning for event-name pruning; better when funnels dominate |
| Long-range trend (months back) | `partition_evolution` day → month at 90 days | Old months become single files; recent days stay day-partitioned |

The demo will **ship the z-order layouts in the table above** and mention the
`event_name` alternative in `status` output so the trade-off is visible rather than
buried.

---

## 6. Commands

| Command | Behaviour | Changes state? |
|---|---|---|
| `clear` / `reset` | Drop tables, delete warehouse and catalog, set `DAYS_INGESTED=0` | yes |
| `mode [cow\|mor]` | Show or set `WRITE_MODE`. Refuses to change mode mid-run (must `clear` first) | yes |
| `next-day` | Ingest the next day's CSVs, then print `status`. After day 5 prints **"No More Data"** | yes |
| `status` | Iceberg metadata + stats: snapshots, live and on-disk file counts, sizes, partition/spec layout, delete files, size histogram | no |
| `maintenance` | Run `icemaint` with `table-config.json` -- compact, expire snapshots, remove orphans -- then print `status` | yes |
| `maintenance --reclaim-now` | The same, with the snapshot age and orphan age guard both set to zero, so storage falls within one run. Prints why that is unsafe outside a demo | yes |
| `query` | Run the discharge and event queries in DuckDB, print results and timings | no |

---

## 7. Constraints and assumptions

Each of these was verified against the installed stack, not assumed.

### 7.1 Forced by the toolchain

| Constraint | Consequence |
|---|---|
| **PyIceberg 0.11.1 has no `VariantType`** (types are `Binary…UUID`, no variant) | `hims_events.data` is **VARCHAR holding JSON**, not VARIANT. Queries use DuckDB's `json_extract` |
| **PyIceberg cannot write position deletes** — its `delete()` and `upsert()` are copy-on-write | `mode mor` must **hand-write position delete files** (the technique already proven in `tests/conftest.py::add_position_deletes`). This simulates what Spark or Flink would emit; it is not PyIceberg doing it |
| **Python 3.13 has no `uuid.uuid7()`** (stdlib gains it in 3.14) | UUIDv7 is generated locally: 48-bit millisecond timestamp + random. Time-ordered, which also helps z-order locality |
| **DuckDB cannot ATTACH a SQLite Iceberg catalog** (only REST) | `query` reads through PyIceberg into Arrow, then queries with DuckDB. Same pattern `icemaint` already uses |
| **Equality deletes are blocked** by PyIceberg scan planning | MoR mode uses **position deletes only** |

### 7.2 Schema additions beyond the brief

| Addition | Why |
|---|---|
| `hims_events.process_id` + `process_type` | The brief asks to "z-order to optimize query by process-id", which requires `process_id` to be a **column**. Left inside the `data` JSON blob it could be neither partitioned, sorted, nor z-ordered |
| `hims_events.actor_id` nullable | `bed_cleaning_required` is described in the source document as system-triggered — it has no human actor |

### 7.3 Demo-scale honesty

**~630 event rows will not produce a measurable query time difference.** Compaction's
effect at this volume is on **metadata**, not scan time.

Mitigations, in order of preference:

1. **Micro-batch ingestion** (§3.3) so file counts are realistic — this is the honest fix
   and reproduces the real-world condition.
2. **Report metadata metrics first**: data file count, manifest count, total metadata bytes,
   average file size, size histogram. These change dramatically and truthfully.
3. Report **query wall-clock second**, labelled as indicative, with planning time separated
   from scan time where DuckDB permits.
4. Offer an optional `--scale N` multiplier on patient volume for anyone who wants timings
   that clear the noise floor.

The demo must **not** present a noisy millisecond delta as if it were a benchmark.

### 7.4 Assumptions requiring confirmation

| # | Assumption | Alternative |
|---|---|---|
| A1 | "30% ± 10%" means ±10% **relative** (8–10 discharges/day) | ±10 percentage points (20–40% → 6–12/day) |
| A2 | `process_id` is a **UUIDv7 string** for both tables | Auto-increment integer; the brief allows either. UUIDv7 keeps one type across tables and sorts by time |
| A3 | `table_schema.json` holds **column definitions**; `table-config.json` holds **layout** | Merge both into `table_schema.json` |
| A4 | `hims_employees` uses **upsert** (stable row identity) | Full `overwrite` — the brief offers both; upsert is gentler on the table's history |
| A5 | Demo lives at `Zamboni/bin/demo`, data under `Zamboni/data/healthims/` | A separate top-level project |

---

## 8. Acceptance

The demo is done when a developer can, from a clean checkout:

1. `./bin/demo clear && ./bin/demo next-day` five times and see file counts grow.
2. See `status` report a small-file problem (many files, low average size).
3. Run `query` and get correct discharge and event results, including the three cancelled
   discharges showing their restart, and EVS events attached to `discharged` rows.
4. Run `maintenance` and see file count fall, average size rise, and the layout
   (z-order, partitioning) reflected in `status`.
5. Re-run `query`, get **identical results**, with metadata metrics improved.
6. Repeat all of the above with `./bin/demo mode mor` and see delete files appear in
   `status` before maintenance and be reported as dangling afterwards.
7. Observe that `status` reports **more files on disk than live** after `maintenance`, and
   that the run says why nothing was deleted -- the demo's files are minutes old and both
   reclaim defaults are age-based. Compaction supersedes files; it does not free them.
8. Run `maintenance --reclaim-now` and see files on disk fall to exactly the live count,
   with `query` still returning identical rows.
9. `./bin/demo next-day` after day 5 prints **"No More Data"** and changes nothing.
