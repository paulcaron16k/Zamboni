# Roadmap

Six features beyond the delivered `v0.1.0`. This document defines *what* each one
is and *why it is sequenced where it is*; the work items live in
[tasks.md](tasks.md) as epics ZMBNI-11 … ZMBNI-16.

**Status lives in tasks.md only.** There are deliberately no status columns here.
Two places recording progress is two places to disagree, and this session has
already spent time correcting counts that drifted between documents.

| | |
|---|---|
| Delivered | `v0.1.0` — compaction, ordering, evolution, reclamation, metadata hygiene, against PyIceberg 0.11.1 |
| Theme | Stop being one implementation. Three engines can do this work; Zamboni should be one of them behind a common interface |
| Delivery order | RM-3 + RM-6 → RM-2 → RM-4 → RM-1 → RM-5 |

Feature RM-*n* maps to epic ZMBNI-1*n* — RM-1 is ZMBNI-11, RM-6 is ZMBNI-16.

---

## What is already verified

These were checked before writing this document, because a roadmap built on
assumptions plans the wrong work. Each row says how it was established.

| Fact | How | Consequence |
|---|---|---|
| PyIceberg 0.12 is **not released** — PyPI's latest is 0.11.1, with no rc | PyPI JSON API | RM-1 develops against the `../iceberg-python` checkout, not a version pin |
| That checkout is on `main` at `154288fb` (2026-07-27), **397 commits** past `pyiceberg-0.11.1` | `git log` | Large surface for private-API drift — see RM-1 |
| Trino has **no Z-order and no sort during `optimize`** | Trino Iceberg connector docs | A common interface cannot treat ordering as universally available |
| Trino enforces **retention floors** (`iceberg.expire-snapshots.min-retention`, `remove-orphan-files.min-retention`, both default `7d`) | same | Zamboni's 5-day/3-day defaults are *rejected*, not honoured. Must fail at plan time |
| Spark removes dangling deletes via the `remove-dangling-deletes` option on `rewrite_data_files`; `rewrite_position_delete_files` is a separate procedure that *compacts* delete files; Trino has neither | Iceberg Spark procedures docs, corrected by RM-3 | Dangling-delete removal is Spark-only, and Spark can do what *we* cannot. An earlier draft of this row attributed it to the wrong procedure |
| ice-keeper is 8 399 lines of PyIceberg 0.10.x + PySpark over py4j | `../ice-keeper` checkout | It is effectively a spark-maintainer already; RM-6 feeds RM-5 |
| ice-keeper's abstraction is **SQL-statement-generating** — `ActionStrategy.prepare_statement_to_execute() -> str` | reading `ice_keeper/task/action/action.py` | That seam cannot be ours; see RM-2 |

---

## RM-1 — Build against PyIceberg 0.12

**What.** Move development onto the unreleased 0.12 line, on a branch, and keep
`0.11.1` working until 0.12 ships.

**The capability delta, source-verified against `main` @ `154288fb`.** Zamboni
routes every version-dependent decision through structural probes
(`capabilities.py`), so "what does 0.12 change" has a precise answer:

| Probe | 0.11.1 | main → 0.12 | Meaning |
|---|---|---|---|
| operation injectable | True | True | unchanged; still how a compaction gets labelled `replace` |
| REPLACE summary native | False | **False** | `update_snapshot_summaries` still rejects `REPLACE`; `_ReplaceFiles` stays |
| streaming writes | False | **True** | `_dataframe_to_data_files` now accepts `pa.RecordBatchReader` |
| manifest predicate pruning | False | **True** | `_OverwriteFiles._existing_manifests` prunes via `manifest_evaluator` |
| derives delete predicate | False | **True** | `_build_delete_files_partition_predicate` exists and is called |
| equality deletes readable | False | **False** | the `NotImplementedError` guard is still there |
| delete manifests writable | False | **False** | `ManifestWriterV2.content()` still returns `DATA`; there is no `ManifestWriterV3` |

**Three of seven flip, all favourably, and the dangerous pair lands together.**
Zamboni *refuses to run* when a build prunes manifests by predicate without
deriving that predicate — rows in manifests holding removed files would be
counted twice. Both arrived in the same release, so 0.12 does not trip the
refusal. This was the single most important thing to check before planning, and
it is why RM-1 is a lower risk than its size suggests.

**What 0.12 does not do:** it does not lift either blocker we are waiting on.
`delete_manifests_writable` stays false, so ZMBNI-604 (rewriting a *partially*
dangling delete manifest) remains blocked; and equality deletes stay unreadable.
Anyone expecting 0.12 to close those should read this row first.

**So the value is:** don't rot, gain streaming writes and cheaper commits, and —
mainly — **find out what 397 commits did to the private APIs this package
drives**. Known movers already visible in the log: `_scan_plan_helper` renamed to
`_plan_manifest_entries`, `BaseScan`/`ManifestGroupPlanner` extracted, and
`ManifestEntry.snapshot_id`'s setter fixed for writing to the wrong index —
which `manifests.py` depends on preserving exactly.

**Risk.** Driving private APIs is a deliberate choice (design.md §3) whose cost
is exactly this. The mitigation is already in place: the probes are structural,
so an incompatible build refuses rather than corrupts.

**Branch.** `feature/pyiceberg-0.12`, with a path dependency on
`../iceberg-python`. That dependency makes `uv.lock` unreproducible on any other
machine, so it must **not** reach `main` until 0.12 is on PyPI — which is the
argument for the branch, and the reason RM-1 can proceed in parallel with
anything else without destabilising `main`.

---

## RM-2 — A maintainer interface

**What.** A `Maintainer` abstraction with `LocalMaintainer` (today's
implementation, extracted), plus `TrinoMaintainer` and `SparkMaintainer` stubs.

**Why an interface at all.** The six operations are not Zamboni-specific — they
are Iceberg's. Trino and Spark implement most of them already, better in places
(Spark rewrites position delete files; we can only drop whole manifests) and
worse in others (Trino cannot Z-order). An operator's real question is "maintain
this table", not "maintain this table with PyIceberg". The interface is what lets
the answer be chosen per table.

**Where the seam goes, and two rejected alternatives.** This is RM-3's output,
but the shape is already constrained:

- *SQL-statement generation* — ice-keeper's choice: every action returns a SQL
  string that Spark executes. Coherent, because both of its backends are SQL
  engines. **Rejected for us:** Zamboni manipulates metadata through PyIceberg
  and has no SQL to emit.
- *Metadata manipulation* — Zamboni's own internals. **Rejected:** Trino and
  Spark expose procedures, not manifest writers.
- *Operation-level, with declared capabilities* — the only seam all three can
  implement. Each maintainer translates one operation to its native mechanism and
  declares what it can and cannot do.

**The hard part is not the ABC — it is refusing to paper over the differences.**
A common interface tempts you to accept every option and quietly ignore what a
backend cannot do. Concretely, it must express:

- **Missing operations.** Z-order on Trino. Dangling-delete removal on Trino.
- **Parameter vocabulary.** Trino's `retain_last` is the spec's
  `min-snapshots-to-keep`; its `retention_threshold` has a configured floor that
  will *reject* our defaults. `max-ref-age-ms` has no Trino equivalent at all.
- **Whether a preview is even possible — per operation.** This one bites
  hardest. Zamboni's central safety rule — *without `--yes`, nothing is
  committed*, enforced across all six verbs — is only partly reproducible
  elsewhere. RM-3 established the detail: Spark's `remove_orphan_files` has a
  `dry_run` argument and no other Spark procedure does; Trino has none at all.
  So the capability is `can_preview(operation)`, not `can_preview()`. A
  maintainer that cannot preview must say so, and the CLI must not print "dry
  run" over an engine that is about to delete — that would turn the one rule we
  made exceptionless back into a lie.
- **Guarantee level, not just capability.** Zamboni's orphan removal is fenced by
  five invariants it enforces client-side (design.md §6.6): completeness of the
  referenced set, non-empty reference categories, current metadata never a
  candidate, list-before-reachable ordering, and a mandatory age guard — each of
  which *aborts* the run rather than deleting. Delegating to
  `remove_orphan_files` gives up all five; the engine does its own thing. So
  "both support remove-orphans" is true and misleading. The interface should
  carry the strength of the guarantee, because that is what an operator is
  actually choosing between.

**Extraction is a refactor, not a rewrite.** `LocalMaintainer` should come out of
the current code with no behaviour change, and the 327 existing tests are the
regression net that proves it.

---

## RM-3 — Zamboni vs Trino vs Spark

**What.** The analysis that determines RM-2's interface: each engine's actual
maintenance surface, the common denominator, and — more usefully — the register of
places where the same-sounding operation differs.

**Why it is first.** Designing the interface before knowing the three surfaces
means discovering a mismatch after the ABC has callers. The preliminary findings
above already produced two constraints that would have been easy to miss (Trino's
retention floors, and the absence of any preview mode), and those came from
skimming one page each. A deliberate pass will produce more.

**Delivered** as [engine-comparison.md](engine-comparison.md). It is a document, not code: exact procedure and parameter names from
primary sources, a mapping onto Zamboni's six verbs, the semantic-difference
register, a comparison of which safety invariants survive delegation, and a
recommended seam with the rejected alternatives recorded.

---

## RM-4 — Trino maintainer

**What.** Implement `TrinoMaintainer` over `ALTER TABLE … EXECUTE`.

**Expected mapping,** to be confirmed by RM-3:

| Zamboni verb | Trino |
|---|---|
| `compact` | `optimize` (`file_size_threshold`, `WHERE` on partition columns or `"$file_modified_time"`) — no sort, no Z-order |
| `rewrite-manifests` | `optimize_manifests` |
| `expire` | `expire_snapshots` (`retention_threshold`, `retain_last`, `clean_expired_metadata`) |
| `remove-orphans` | `remove_orphan_files` (`retention_threshold`) |
| `remove-dangling-deletes` | **no equivalent** |
| `apply-properties` | `ALTER TABLE … SET PROPERTIES` |

**Why before Spark.** Four of six verbs map, the surface is small, and the
mismatches are the instructive ones — a missing capability (Z-order), a rejected
parameter (retention floors), and an operation with no counterpart. It exercises
every part of the interface that exists to handle difference, which is exactly
what you want from the first implementation after the abstraction.

---

## RM-5 — Spark maintainer

**What.** `SparkMaintainer` over the Iceberg Spark procedures:
`rewrite_data_files` (including `sort` and `zorder`), `rewrite_manifests`,
`expire_snapshots`, `remove_orphan_files`, and `rewrite_position_delete_files`.

**Last, deliberately.** It is the largest dependency footprint — a JVM, PySpark,
py4j — for the engine that overlaps Zamboni most, so it buys the least new reach
per unit of work. It is also the one place where the interface must express that
a *non-local* maintainer can do something the local one cannot: Spark's
`remove-dangling-deletes` compaction option drops deletes per file rather than
per whole manifest, which is precisely the limitation of ZMBNI-604 — and it
brings a second procedure, `rewrite_position_delete_files`, for compacting delete
files, which neither Zamboni nor Trino has in any form. RM-6 informs this heavily — ice-keeper
is a working Spark maintenance service and its Spark plumbing is prior art.

---

## RM-6 — Zamboni vs ice-keeper

**What.** A comparison against `../ice-keeper`: a real, deployed Iceberg
maintenance service (Airflow-scheduled, nightly) covering expiry, orphan removal
and partition optimization.

**Why it matters more than a competitive survey.** ice-keeper has whole
*categories* of functionality Zamboni does not, and they are operational rather
than algorithmic — the parts you need once maintenance stops being a command and
becomes a service:

- a maintenance **schedule** table and a **journal** of what ran, with status and
  timings, per table
- **partition health diagnosis** driving which partitions get optimized
- **storage inventory reports** as an orphan-detection input — which sidesteps the
  bucket listing that Zamboni's orphan removal depends on, and that a
  remote-signing warehouse refuses outright
- a **config auditor**, and notification on failure

**And it is prior art for RM-2.** Its `ActionStrategy` is the closest existing
thing to the interface RM-2 defines, including the parts that do not transfer:
the SQL-statement seam works only because both its backends are SQL engines. That
is a useful negative result to have in hand *before* designing ours, which is why
this belongs alongside RM-3 rather than at the end.

**Delivered** as [ice-keeper-comparison.md](ice-keeper-comparison.md).

**Not a licensing or adoption decision** — the deliverable is an analysis with a
per-capability recommendation to adopt, adapt, or decline.

---

## Sequencing

```
RM-3  Trino/Spark analysis  ─┐
RM-6  ice-keeper analysis   ─┴─→  RM-2  interface  ──→  RM-4  Trino  ──→  RM-5  Spark
                                                 RM-1  0.12 (branch, parallel)
```

**RM-3 and RM-6 first, together.** Both are analysis, both feed RM-2, and neither
touches code. RM-6 is unscheduled in the original request; putting it here is a
recommendation, on the grounds that its `ActionStrategy` finding is an input to
the interface design rather than a retrospective.

**RM-2 next**, because the class hierarchy is what everything after it plugs into.

**RM-4 before RM-1** — as requested, and it holds up: the first implementation
behind a new abstraction is what proves the abstraction, and doing it against a
stable PyIceberg 0.11.1 keeps one variable fixed.

**RM-1 in parallel, on a branch.** It cannot land on `main` until 0.12 is on PyPI
anyway, so it does not contend for the same code. Doing it *after* RM-2 has a real
advantage the original ordering already captures: the 0.12 migration then happens
inside `LocalMaintainer`, with the interface pinning its observable behaviour.

**RM-5 last**, per the original request.

---

## Open questions

These need answers before the epic they belong to can be finished, and are
recorded here rather than discovered mid-implementation:

1. **Does Zamboni support two PyIceberg lines at once?** RM-1 can either keep
   0.11.1 working alongside 0.12 or move the floor. The probes make both
   technically possible; the cost is a test matrix that doubles.
2. **What does `--engine trino` do about `--yes`?** Either the flag means
   something weaker for non-local maintainers, or non-previewable operations
   require a distinct opt-in. This is a user-visible contract decision, not an
   implementation detail — see [releasing.md §1](releasing.md).
3. **Does the dev stack grow a Trino and a Spark?** RM-4 and RM-5 need live
   verification, and the existing stack pattern (shifted ports, `.env.sample`)
   would extend to both — at a cost in start-up time for everyone.
4. **Is the schedule/journal layer in scope at all?** RM-6 will surface it. It is
   arguably a different product — a maintenance *service* rather than a
   maintenance *tool* — and deciding that early avoids a scope argument later.
