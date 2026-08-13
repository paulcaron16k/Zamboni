# Roadmap — mostly delivered

**Five of the six features below shipped. One is blocked upstream.** This is now
largely a record of a plan and how it turned out, kept for the reasoning rather
than for the schedule: each feature says what it was *for*, and the evidence it
was chosen on. If you want current status, [tasks.md](tasks.md) has it; if you
want what to do next, there is exactly one answer and it is RM-1.

| Feature | Epic | Outcome |
|---|---|---|
| RM-1 — PyIceberg 0.12 | ZMBNI-11 | **Open, waiting on the 0.12 release.** Branch written and verified; see below |
| RM-2 — Maintainer interface | ZMBNI-12 | Delivered 2026-08-03 |
| RM-3 — Zamboni vs Trino vs Spark | ZMBNI-13 | Delivered 2026-08-03 — [engine-comparison.md](engine-comparison.md) |
| RM-4 — Trino maintainer | ZMBNI-14 | Delivered 2026-08-04, verified against Trino 483 |
| RM-5 — Spark maintainer | ZMBNI-15 | Delivered 2026-08-07, verified against Spark 4.0.4 |
| RM-6 — Zamboni vs ice-keeper | ZMBNI-16 | Delivered 2026-08-03 — [ice-keeper-comparison.md](ice-keeper-comparison.md) |

**Status still lives in tasks.md only.** The outcome column above is a pointer,
not a second ledger: it records *that* something landed and when, and nothing
about what remains inside it. Two places recording progress is two places to
disagree.

| | |
|---|---|
| Theme | Stop being one implementation. Three engines can do this work; Zamboni is now one of them behind a common interface |
| Planned order | RM-3 + RM-6 → RM-2 → RM-4 → RM-1 → RM-5 |
| Actual order | RM-3 + RM-6 + RM-2 → RM-4 → RM-5, with **RM-1 still open** |

**The one place the plan was wrong is the interesting one.** RM-1 was sequenced
fourth, ahead of RM-5, on the reasoning that it could proceed in parallel on a
branch. It could, and did -- the branch exists and the migration is verified --
but it cannot *land*, because 0.12 is not released in a usable state. The
sequencing assumed the blocker was our capacity; it was somebody else's release.
RM-5 moved up and shipped instead.

---

## What is already verified

These were checked before writing this document, because a roadmap built on
assumptions plans the wrong work. Each row says how it was established, and rows
that have since been overtaken say so rather than being quietly corrected --
what a plan believed at the time is part of why it chose what it chose.

| Fact | How | Consequence |
|---|---|---|
| ~~PyIceberg 0.12 is **not released** — PyPI's latest is 0.11.1, with no rc~~ **Superseded 2026-08-11: `0.12.0rc1` is on PyPI; the latest *stable* is still 0.11.1** | PyPI JSON API | RM-1 develops against the checkout until a release candidate carries the fix for [#3758](https://github.com/apache/iceberg-python/issues/3758) |
| ~~That checkout is on `main` at `154288fb` (2026-07-27), **397 commits** past `pyiceberg-0.11.1`~~ **Now `32f036c5`, 19 commits past `pyiceberg-0.12.0rc1`** | `git log` | Large surface for private-API drift — and it drifted: two probes now key on symbols that moved (ZMBNI-1109) |
| `0.12.0rc1` reproduces [#3758](https://github.com/apache/iceberg-python/issues/3758); main after rc1 does not | the 25-line reproduction in [upstream-0.12-upsert-regression.md](upstream-0.12-upsert-regression.md), run against both on 2026-08-11 | The supported range includes 0.12 once a release carries the fix ([#3780](https://github.com/apache/iceberg-python/pull/3780)) |
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

### Design: no version-specific maintainer

The question this settles, because it is the obvious first guess: should there be
a `local-0.12` maintainer alongside `local`, selected by the factory?

**No.** The registry's axis is *which engine performs the work* -- PyIceberg plus
DuckDB, Trino, Spark. A library version is not that axis:

- **It is not selectable.** One `import pyiceberg` per process; 0.11.1 and 0.12
  cannot be installed together. `--engine local-0.12` would succeed or fail on
  what happens to be in the virtualenv, so the operator is not choosing anything
  -- which is what a factory selection is for.
- **The mechanism already exists.** `capabilities.py` says it in its first
  paragraph: every version-dependent decision routes through structural probes
  "rather than through a version comparison". A version-named maintainer *is*
  that comparison, at a coarser grain and with a flag attached.
- **It would duplicate the six operations**, which is the argument against a
  shell wrapper in devops.md §1 pointed back at ourselves. A second
  implementation of `expire` is a second thing to keep correct.

**Considered and rejected for a real case too.** If 0.12's streaming writes
justify a materially different compaction path, that is a *strategy* inside
`LocalMaintainer` chosen by a probe -- the way memory modes and rewrite backends
already work -- not a registered engine. The operator should not pick it; the
installed library decides.

**So the change to the maintainer is a deletion, not an addition.**
`LocalMaintainer.capabilities()` currently hardcodes limitations that are
*derived* from probes, including a string asserting "ZMBNI-604, still true on
0.12". Every claim in it corresponds to a probe that is False on 0.11.1:
`delete_manifests_writable` drives the dangling-delete caveat,
`equality_deletes_readable` the compaction blocker,
`streaming_write_supported` the memory behaviour. It must read `detect()`.

That is a defect **today**, not only on 0.12: `zamboni engines` reports a static
claim about a dynamic property, in the one place whose purpose is refusing to
overstate capability. Tracked as ZMBNI-1107.

**And it reframes the support window.** With probe-driven capabilities, running
against both PyIceberg lines needs no version branching in code -- the probes do
it. The only cost is a test matrix where some expected values differ by install,
so ZMBNI-1106 is a question about CI spend rather than about architecture.

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

**Delivered** in `src/zamboni/maintainers/`. Extraction was a refactor, not a
rewrite: `LocalMaintainer` came out of the CLI handlers with no behaviour change,
and the existing tests passing unchanged is what proves it.

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

## Sequencing — as planned

Kept as written. What actually happened is in the table at the top; the one
divergence and its cause are noted there.


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

Recorded here rather than discovered mid-implementation. Three of the four are
now answered; the answers are kept beside the questions because *what was
uncertain at the time* is part of the record:

1. **Does Zamboni support two PyIceberg lines at once?** Still open, and now the
   only genuinely open question here. RM-1 can either keep 0.11.1 working
   alongside 0.12 or move the floor. The probes make both technically possible;
   the cost is a test matrix that doubles. It is a release-timing decision rather
   than a technical one, which is why ZMBNI-1106 is deliberately unstarted.
2. ~~**What does `--engine trino` do about `--yes`?**~~ **Answered by ZMBNI-1206.**
   Neither option in the original framing was taken. Where an engine cannot
   preview an operation, a run without `--yes` **refuses** — it does not execute,
   and it does not print a dry-run notice over an engine that is about to delete.
   Refusing commits nothing, so *without `--yes`, nothing is committed* holds on
   every engine and still has no exceptions. The rule needed no weakening and no
   second opt-in; it needed the third option.
3. ~~**Does the dev stack grow a Trino and a Spark?**~~ **Answered: both, each
   behind its own compose profile so neither starts by default.** The cost
   concern was real and the profiles are the answer to it -- Trino is a JVM and
   the Spark image is 1.57GB, so nobody pays for either unless they ask. Spark is
   a **Connect server** rather than a standalone master, which was not in the
   original framing: it puts the JVM in the container, so `zamboni[spark-connect]`
   is ~1.5MB of pure Python against pyspark's 434MB and the developer's Java
   version stops being this project's problem.
4. ~~**Is the schedule/journal layer in scope at all?**~~ **Answered: no, and
   deliberately.** RM-6 surfaced it as predicted. Zamboni exits with a code and
   something else decides when to run it -- cron, Airflow, systemd -- which is
   why [devops.md](devops.md) argues against a loop inside the tool and why
   `zamboni warehouses` generates a schedule rather than being one. The
   maintenance *service* remains a different product.
