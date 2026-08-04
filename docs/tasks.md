# Zamboni — Task backlog

Project key **ZMBNI**. Epics are ZMBNI-1…10; stories are numbered within their epic
(ZMBNI-1xx under epic 1, and so on).

Every story points at the requirement it satisfies in [plan.md](plan.md) §3 and the design
section that explains it in [design.md](design.md). A story with no FR reference is one that
has not been specified yet — that is deliberate signal, not an omission.

| Status | Meaning |
|---|---|
| `done` | Delivered and covered by a test named in plan.md §3 |
| `inproject` | Started, not finished |
| `todo` | Not started. Where an upstream limitation blocks it, the description says which |
| `cancelled` | Deliberately not doing. The description says why |

> **On `completed-at`.** Git history postdates the build: the repository was initialised on
> 2026-08-03 with the component already written, so everything delivered before that date
> collapses to the initial commit rather than carrying a real finish date. Dates from
> 2026-08-03 onward are the commit dates. Nothing here is a reconstructed timeline.

---

## Epics

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1 | Data-file compaction | Rewrite small files into target-sized ones and commit a `replace` snapshot. FR-1, FR-2, FR-6. design §2.4, §5.2 | done | 2026-08-03 |
| ZMBNI-2 | Query-shaped layout | Declared sort and multi-key Z-order, with honest `sort_order_id`. FR-3. design §2.2 | done | 2026-08-03 |
| ZMBNI-3 | Partition evolution | Condense aged fine-grained partitions without moving where new data lands. FR-4. design §5.3 | done | 2026-08-03 |
| ZMBNI-4 | Declarative configuration | `table-config.json` and its Meltano authoring surface. FR-5. design §4, §5.1 | done | 2026-08-03 |
| ZMBNI-5 | Storage reclamation | Snapshot expiry and orphan-file removal, fenced by hard invariants. FR-7. design §2.4, §5.4, §6.6 | done | 2026-08-03 |
| ZMBNI-6 | Metadata hygiene | Dangling deletes, manifest regrouping, `metadata.json` retention. FR-8, FR-9.1–9.2. design §2.1, §2.3 | inproject | |
| ZMBNI-7 | Format-version coverage | V1 refused, V2 full, V3 metadata-only. FR-6.8, FR-9.3–9.5. design §2.1, §6.1 | inproject | |
| ZMBNI-8 | Environment and dev stack | Locked venv, self-contained executables, Lakekeeper + MinIO stack. design §6.4 | done | 2026-08-03 |
| ZMBNI-9 | Verification and CI | The suite, live verification, and automation. plan.md §4 | inproject | |
| ZMBNI-10 | Documentation | HLD, delivery plan, config spec, runbook, release convention, verification record | done | 2026-08-03 |
| ZMBNI-11 | PyIceberg 0.12 | Develop against unreleased 0.12 on a branch; three of seven capability probes flip, neither waited-on blocker lifts. [roadmap.md RM-1](roadmap.md) | todo | |
| ZMBNI-12 | Maintainer interface | `Maintainer` ABC, `LocalMaintainer` extracted with no behaviour change, Trino and Spark stubs carrying real capability declarations, and `--engine` / `zamboni engines`. [roadmap.md RM-2](roadmap.md) | done | 2026-08-03 |
| ZMBNI-13 | Zamboni vs Trino vs Spark | Delivered as [engine-comparison.md](engine-comparison.md): three surfaces, a twelve-row semantic-difference register, and the seam it forces. [roadmap.md RM-3](roadmap.md) | done | 2026-08-03 |
| ZMBNI-14 | Trino maintainer | `ALTER TABLE … EXECUTE` for five of six verbs, refusing dangling-delete removal. Verified against a live Trino 483 in the dev stack. [roadmap.md RM-4](roadmap.md) | done | 2026-08-04 |
| ZMBNI-15 | Spark maintainer | Iceberg Spark procedures, including the one operation we cannot do locally. [roadmap.md RM-5](roadmap.md) | todo | |
| ZMBNI-16 | Zamboni vs ice-keeper | Delivered as [ice-keeper-comparison.md](ice-keeper-comparison.md). Found ZMBNI-507, a data-loss path in shipped code. [roadmap.md RM-6](roadmap.md) | done | 2026-08-03 |
| ZMBNI-17 | DevOps CLI and operations | One command a cron line can call, config that is not twenty flags, and a fleet story for multi-tenant warehouses. [devops.md](devops.md) | done | 2026-08-04 |

> **On section order.** Epics ZMBNI-1…10 appear below in numeric order; the roadmap epics
> ZMBNI-11…16 appear in *delivery* order (13, 16, 12, 14, 11, 15), which is not their numeric
> order. The numbers follow [roadmap.md](roadmap.md)'s RM-1…RM-6 so the two documents agree on
> identity; the sequence is explained in each section's subtitle.

**Story counts:** 78 done · 1 inproject · 16 todo · 1 cancelled  (96 stories)

---

## ZMBNI-1 — Data-file compaction

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-101 | Rewrite per partition | Group by `(spec_id, partition)`, bin-pack, rewrite, commit one `replace` snapshot per group. Bucket transforms included. FR-1.1–1.4. design §5.2 | done | 2026-08-03 |
| ZMBNI-102 | Size and count thresholds | Skip partitions below `min_input_files` or already at target size; resolve target from config → table property → default. FR-1.5–1.7 | done | 2026-08-03 |
| ZMBNI-103 | Bounded memory | Peak ≈ one output file regardless of table size, via DuckDB spill. FR-6.3. design §3 | done | 2026-08-03 |
| ZMBNI-104 | Merge-on-read correctness | Preserve *live* rows, not physical rows; never resurrect a deleted row. FR-2.1–2.4. design §2.1 | done | 2026-08-03 |
| ZMBNI-105 | Refuse unsafe PyIceberg builds | Structural capability probes gate the commit path rather than version comparisons. FR-6.4, FR-6.9. design §3 | done | 2026-08-03 |
| ZMBNI-106 | All-or-nothing multi-partition commit | A run now rewrites every group, then commits once — matching Iceberg's own default (`partial-progress.enabled=false`), with `--partial-progress` for the previous per-group behaviour. Iceberg is explicit that this is predictability, not correctness: "file groups can be compacted independently". Fixed a latent bug on the way: `MultiSpecReplaceFiles` hardcoded REPLACE, so any evolved table ignored `snapshot_operation="overwrite"`. FR-1.8–1.10. design §6.5 | done | 2026-08-03 |

---

## ZMBNI-2 — Query-shaped layout

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-201 | Declared sort order | Sort output by the table's declared order, stamping `sort_order_id` only when the output genuinely satisfies it. FR-3.6, FR-3.7 | done | 2026-08-03 |
| ZMBNI-202 | Multi-key Z-order | Morton encoding as DuckDB SQL — dense-rank, scale, bit-interleave. Nothing in the stack provides it. FR-3.1–3.5. design §2.2 | done | 2026-08-03 |

---

## ZMBNI-3 — Partition evolution

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-301 | Days into months | Add the coarse spec without making it default, so new data keeps landing daily. Aged from the window *end*, protecting late arrivals. FR-4.1–4.3, FR-4.7. design §5.3 | done | 2026-08-03 |
| ZMBNI-302 | Multi-spec metadata integrity | One manifest per spec, and a fresh partition-field id — reusing the source field's id makes one id mean two things. FR-4.4–4.6 | done | 2026-08-03 |
| ZMBNI-303 | Compound partition specs | A compound spec now evolves when exactly one field matches the rule's granularity: that field coarsens, the others carry through with their field ids, and grouping keys on the whole output partition tuple. The old skip rested on faulty reasoning — it assumed a spec per combination of the other fields, but those combinations are partition values under one shared spec. Two fields of the same granularity are still refused, because `older_than_days` measures from a window end and two fields give two answers. FR-4.8–4.10 | done | 2026-08-03 |

---

## ZMBNI-4 — Declarative configuration

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-401 | `table-config.json` specification | Partition, evolution, ordering, sizing, retention. Unknown keys rejected; contradictions refused at load. FR-5.1–5.6. design §4 | done | 2026-08-03 |
| ZMBNI-402 | Meltano `x-iceberg` import | Generate the config from a Singer catalog. Generated rather than read at runtime because the Singer SDK silently drops unknown keys. FR-5.7–5.10. design §5.1, §6.3 | done | 2026-08-03 |

---

## ZMBNI-5 — Storage reclamation

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-501 | Referenced-file set | All six categories Iceberg metadata points at. Safety-critical: both reclaim operations subtract this from something else. FR-7.1. design §6.6 | done | 2026-08-03 |
| ZMBNI-502 | Snapshot expiry | The spec's retention algorithm, then delete the set difference. PyIceberg implements almost none of it and deletes no files. FR-7.2–7.4, FR-7.9. design §5.4 | done | 2026-08-03 |
| ZMBNI-503 | Orphan-file removal | List-before-reachable ordering, a 3-day mtime guard, and abort-on-doubt invariants. FR-7.5–7.8, FR-7.11. design §6.6 | done | 2026-08-03 |
| ZMBNI-504 | FileIO-agnostic listing | Works on whichever FileIO the deployment forces; the listing form and the delete form differ on object storage. FR-7.12–7.14 | done | 2026-08-03 |
| ZMBNI-507 | Refuse orphan removal on a shared table location | **Data-loss path in shipped v0.1.0.** `storage_roots()` scopes the sweep to the table's own location and its docstring claims sibling files are out of reach "by construction" — but that construction prevents a warehouse-wide sweep, not another table's files living *inside* this table's location. Those get listed, found unreferenced by this table, and deleted. Reachable via an explicit `location` under a shared prefix, a second table whose `write.data.path` points inside the first, or two tables pointed at one location after a `register_table` mistake. ice-keeper guards this explicitly (`check_table_location_is_unique`); we do not. Found by ZMBNI-1601. Should abort like the other reclaim invariants. [ice-keeper-comparison.md §4](ice-keeper-comparison.md) **Fixed.** `colocated_tables()` enumerates every table in the catalog and refuses when any location overlaps ours; the check runs before the listing, since it is cheaper to refuse than to list first. Reproduced before fixing: rename `db.orders` to `db.orders_v2`, recreate `db.orders`, maintain it — all 9 of the renamed table's files were deleted and it became unreadable. FR-7.18 | done | 2026-08-03 |
| ZMBNI-505 | Apply `max-ref-age-ms` | Spec step 2 now applies: a stale non-main ref is dropped in the same transaction as the expiry, so its snapshots stop being pinned. Fixed two detection defects on the way — a ref's own `max-ref-age-ms` was ignored, and the step was skipped entirely when the table set no property, so a ref carrying its own age was never evaluated. Off unless configured. FR-7.15–7.17. design §6.5 | done | 2026-08-03 |

---

## ZMBNI-6 — Metadata hygiene

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-601 | Dangling-delete removal | Drop delete files that apply to no live data file. Applicability delegated to PyIceberg's own `DeleteFileIndex`. FR-8.1–8.5. design §2.4 | done | 2026-08-03 |
| ZMBNI-602 | Manifest regrouping | Group entries by partition in value order so predicates prune at manifest level, preserving sequence numbers exactly. FR-8.6–8.10. design §2.3 | done | 2026-08-03 |
| ZMBNI-603 | `metadata.json` retention | Apply the two `write.metadata.*` properties deliberately. Trimming the log alone manufactures orphans rather than reclaiming bytes. FR-9.1, FR-9.2 | done | 2026-08-03 |
| ZMBNI-604 | Rewrite partially dangling delete manifests | **Blocked upstream.** `ManifestWriterV2.content()` returns `ManifestContent.DATA` unconditionally, so PyIceberg cannot write a delete manifest at all; a mixed manifest can only be dropped whole or left alone. Lifts automatically if a writer appears — the capability is probed, not assumed (`zamboni doctor`). FR-8.4. design §6.5 | todo | |
| ZMBNI-605 | Split an oversized partition across manifests | **Cancelled.** Not an upstream limitation — a deliberate choice. A manifest records lower/upper partition bounds, so splitting one partition across manifests is precisely what stops the manifest evaluator pruning, which is the operation's whole purpose. An oversized partition therefore gets one large manifest. FR-8.8 | cancelled | 2026-08-03 |

---

## ZMBNI-7 — Format-version coverage

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-701 | Refuse format version 1 | V1 has neither sequence numbers nor row-level deletes, and DuckDB will not write it. Blocked with a reason rather than attempted. FR-6.8. design §6.1 | done | 2026-08-03 |
| ZMBNI-702 | Format version 2 | Full support, including position delete files. The version everything else here is verified against. FR-1, FR-2. design §2.1 | done | 2026-08-03 |
| ZMBNI-703 | Refuse V3 row rewriting | V3 carries row lineage, which cannot survive scan-and-rewrite. Our own blocker, because upstream's guard is serialisation-only and a REST catalog never triggers it. Metadata-only operations stay allowed. FR-9.3–9.5 | done | 2026-08-03 |
| ZMBNI-704 | V3 row rewriting with lineage | **Blocked upstream.** Compaction would have to carry `_row_id` and `_last_updated_sequence_number` through the read and write; neither the scan nor PyIceberg's writer exposes them. PyIceberg 0.11.1 also cannot serialise V3 metadata. FR-9.3. design §6.1 | todo | |
| ZMBNI-705 | Equality deletes | **Blocked upstream.** `DataScan._plan_files_local` raises on them, so the rows they remove cannot be materialised and compaction would resurrect deleted rows. Blocked, with the blocker lifting automatically when the probe reports the scan can handle them. FR-6.9. design §6.5 | todo | |
| ZMBNI-706 | V3 deletion vectors | **Blocked upstream.** PyIceberg ships a Puffin reader and no writer, so a V3 merge-on-read table can be profiled but its deletes cannot be constructed for testing the way V2 position deletes are. design §6.5 | todo | |

---

## ZMBNI-8 — Environment and dev stack

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-801 | Locked environment | Everything runs from a `uv`-locked virtualenv; nothing resolves against global site-packages. design §6.4 | done | 2026-08-03 |
| ZMBNI-802 | Self-contained executables | `bin/zamboni` and `bin/demo` as PEP 723 scripts with pinned dependencies and a *relative* editable path, so they survive a directory rename and run from anywhere | done | 2026-08-03 |
| ZMBNI-803 | Dev stack | Lakekeeper (with its built-in UI), Postgres and MinIO on shifted ports. `.env.sample` committed, `.env` gitignored. See [../dev-stack/README.md](../dev-stack/README.md) | done | 2026-08-03 |
| ZMBNI-804 | STS warehouse bootstrap | Idempotent bootstrap creating a credential-vending warehouse. The setting that decides whether storage can be reclaimed at all, and the endpoint that resolves from both host and container | done | 2026-08-03 |
| ZMBNI-805 | Demo on either catalog | `--catalog sqlite` (default, needs nothing running) or `--catalog lakekeeper`. Existence and clearing differ per backend, not just the connection string | done | 2026-08-03 |

---

## ZMBNI-9 — Verification and CI

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-910 | Review the five unreviewed commits | Dev stack, CI, backlog, ZMBNI-908 and ZMBNI-505 had landed with verification but no review pass. Found four defects, all reproduced before fixing: a stale ref reported as dropped while still present, a false soft-delete claim in user-facing output, a queue assertion weaker than the claim it defended, and an untested behaviour change in `S3Settings` | done | 2026-08-03 |
| ZMBNI-911 | Reconcile the --yes posture | One rule for all six verbs: without `--yes`, nothing is committed, and each says it is previewing. `compact` previews instead of exiting 2, and the notice is now unconditional — three verbs printed it only when they found work, so the rule was visible on some runs and not others. FR-6.2 | done | 2026-08-03 |
| ZMBNI-912 | Make the CLI tests hermetic | `test_rest_catalog_requires_uri_and_warehouse` asserted the CLI errors without `--uri`/`--warehouse`, but every catalog flag also reads a `ZAMBONI_*` variable — and dev-stack/README.md tells developers to export two of them. It passed on a clean shell and failed on the shell of anyone who followed the instructions. Now clears them explicitly | done | 2026-08-03 |
| ZMBNI-901 | Unit and integration suite | 314 tests against a SQL catalog over a temp directory. plan.md §4 | done | 2026-08-03 |
| ZMBNI-902 | Safety-by-omission tests | Monkeypatch each reference category away in turn and assert nothing is deleted. Without these, enabling orphan removal by default is unjustified. FR-7.7 | done | 2026-08-03 |
| ZMBNI-903 | Live verification | Every operation against a real Lakekeeper 0.13.1 and MinIO. Found four bugs the local suite could not. [live-verification.md](live-verification.md) | done | 2026-08-03 |
| ZMBNI-904 | Dev-stack tests | 13 tests asserting the stack is configured such that reclamation *can* work, skipping cleanly when it is down. The 13th arrived with ZMBNI-906, covering the one operation only `verify-live.py` had exercised | done | 2026-08-03 |
| ZMBNI-905 | CI workflow | Four jobs written and every command verified locally, but **never executed by GitHub** — this repository has no remote, so nothing runs until one is added and pushed. Expect the first run to surface something; the likeliest is the pinned `172.31.0.0/24` colliding on a runner, which the job checks for by name | inproject | |
| ZMBNI-906 | Retire `scripts/verify-live.py` | Not deleted: the premise that it was redundant was wrong. The tests read `dev-stack/.env` and so cannot be aimed elsewhere, while the script's real value is diagnosing a deployment this repo did not create — which is how the remote-signing finding came out. It is now a 90-line launcher for those tests instead of 348 lines reimplementing them, the tests take an environment override, and the one operation only the script covered has a test. Fixed two leaks it exposed: a namespace stranded when setup errors, and the demo test writing a fixed namespace into a foreign warehouse | done | 2026-08-03 |
| ZMBNI-907 | Release process | Semver with the 0.x caveat, `vMAJOR.MINOR.PATCH` annotated tags, a Keep-a-Changelog `CHANGELOG.md`, and `--version` on both entry points reporting zamboni, PyIceberg and Python — one alone does not identify behaviour, since which operations are attempted is decided by probing the installed PyIceberg. The version is declared once in `pyproject.toml` and derived everywhere else. The substance is what counts as breaking *for this tool*: a lowered `older_than_days` deletes files on the next nightly run with no signature moved, so the destructive defaults are named as public surface. Released as `v0.1.0` rather than `1.0.0`: 0.x costs nothing and can be followed by 1.0.0 at any time, whereas a promise cannot be withdrawn — and CI has still never executed, so "verified" would overclaim. FR-10.1–10.5. [releasing.md](releasing.md) | done | 2026-08-03 |
| ZMBNI-908 | Type checking | mypy over `src` and `scripts`, enforced in the `lint` job and pre-commit. Demonstrated rather than assumed: reintroducing the `FileIO` has no `_initialize_fs` bug that took a live Lakekeeper run to find is now caught statically. Fixed 15 findings, of which 5 were real hazards -- an unguarded `Snapshot \| None`, a shadowed loop variable, a resolver whose type admitted `None`, and `S3Settings` requiring credentials a vending catalog supplies | done | 2026-08-03 |
| ZMBNI-909 | One byte formatter | The same function existed in four modules and had already diverged -- three capped at GiB, one reached TiB -- so a size formatted differently depending on which module reported it. Consolidated into `zamboni.units.human_bytes` with tests | done | 2026-08-03 |

---

## ZMBNI-10 — Documentation

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1001 | High-level design | Why maintenance is necessary, from the on-disk layout up. [design.md](design.md) | done | 2026-08-03 |
| ZMBNI-1002 | Delivery plan | Scope, phasing, 73 requirements each naming its test, residual risk. [plan.md](plan.md) | done | 2026-08-03 |
| ZMBNI-1003 | Configuration specification | [table-config.md](table-config.md) | done | 2026-08-03 |
| ZMBNI-1004 | Live-verification record | What a remote-signing warehouse blocks, and the four bugs the run surfaced. [live-verification.md](live-verification.md) | done | 2026-08-03 |
| ZMBNI-1005 | Dev-stack guide | [../dev-stack/README.md](../dev-stack/README.md) | done | 2026-08-03 |
| ZMBNI-1006 | This backlog | [tasks.md](tasks.md) | done | 2026-08-03 |
| ZMBNI-1007 | Operator runbook | [runbook.md](runbook.md): the six-verb order with why each position matters, cadence derived from the retention windows rather than invented, how to *measure* the orphan guard against your longest compaction, what to watch, all four exit codes, and recovery per failure mode. Writing it surfaced that five verbs dry-run without `--yes` but never said so in `--help` — fixed, with a test | done | 2026-08-03 |

---

## ZMBNI-13 — Zamboni vs Trino vs Spark

First, because designing the interface before knowing the three surfaces means finding the
mismatch after the ABC has callers. [roadmap.md RM-3](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1301 | Enumerate each engine's surface | Read from primary sources on 2026-08-03 — the Iceberg `spark-procedures.md` and Trino connector doc *sources*, not the rendered pages: the rendered Spark page returns only its table of contents to a fetcher, so a summary written from it would have silently lost every argument table | done | 2026-08-03 |
| ZMBNI-1302 | Map onto the six verbs | Five of six verbs are common to all three. Only dangling-delete removal is missing from an engine, and only from Trino. Each engine has one thing the others lack: Trino `drop_extended_stats`, Spark `rewrite_position_delete_files`, Zamboni automated partition-evolution ageing and the spec's `max-ref-age-ms` step | done | 2026-08-03 |
| ZMBNI-1303 | Semantic-difference register | Twelve differences registered. The two that changed the plan: preview is **per operation, not per engine** — Spark's `remove_orphan_files` has `dry_run` and no other Spark procedure does — and Trino's `file_size_threshold` is a *selection* threshold while Spark's `target-file-size-bytes` is an *output* size, so one config key cannot pass through to both | done | 2026-08-03 |
| ZMBNI-1304 | Which safety invariants survive delegation | Delegation gives up all five of Zamboni's client-side invariants, but the guarantees are **different, not ordered**: Trino refuses a retention below its configured floor, which is stricter than Zamboni's overridable guard and makes `--reclaim-now` unrepresentable rather than merely discouraged. Spark's `prefix_mismatch_mode=ERROR` default is the same abort-on-doubt posture as ours | done | 2026-08-03 |
| ZMBNI-1305 | Recommend the seam | Operation-level with declared capabilities and guarantees. The seam is forced: it is the only level all three implement. Rejected alternatives recorded — SQL generation (unavailable to us, we are the only one that is not a query engine) and metadata manipulation (unavailable to them) | done | 2026-08-03 |

---

## ZMBNI-16 — Zamboni vs ice-keeper

Alongside ZMBNI-13, because its abstraction is an input to the interface design rather than
a retrospective. [roadmap.md RM-6](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1601 | Inventory it | 8 399 lines at `cd31a4d`, PyIceberg 0.10.x plus PySpark over py4j, nightly in Airflow, with one action calling Iceberg's Java API directly through the Spark JVM. A *service* — scheduler, work queue, journal and diagnostics — where Zamboni is a tool invoked against one table | done | 2026-08-03 |
| ZMBNI-1602 | Capability gaps, both directions | Recorded in both directions. Theirs: storage-inventory file listing, maintenance schedule, journal, partition-health diagnosis, skip-if-not-recently-modified, widening preconditions, data lifecycle, notification, config audit. Ours: preview on every verb, structural capability probes, format-version handling, `max-ref-age-ms`, config that rejects contradictions at load, no JVM, Z-order without Spark | done | 2026-08-03 |
| ZMBNI-1603 | What its seam teaches ZMBNI-12 | Its `ActionStrategy` seam is `prepare_statement_to_execute() -> str` — every action emits SQL. A useful negative result: the most similar project in the space chose a seam unavailable to us, which is worth knowing before designing ours rather than after. What does transfer is smaller — a first-class `check_should_execute_action`, and journaling wrapped around the action with warning distinguished from failure | done | 2026-08-03 |
| ZMBNI-1604 | Recommend per capability | Adopt: the location-uniqueness check (ZMBNI-507, a defect) and skip-if-not-recently-modified. Adopt the idea: inventory-backed listing, **with** the age-guard widening and the fallback-to-listing that make it safe. Consider: partition-health diagnosis, widening preconditions. Decline: the SQL seam, data-lifecycle deletion, notification. Sharpens open question 4 rather than settling it — most of the gap is that ice-keeper is a service and Zamboni is a tool | done | 2026-08-03 |

---

## ZMBNI-12 — Maintainer interface

The class hierarchy everything after it plugs into. [roadmap.md RM-2](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1201 | The `Maintainer` ABC | `Maintainer` in `src/zamboni/maintainers/`, with the six operations plus a `capabilities()` classmethod — a classmethod because what an engine supports is a property of the engine, not of a connected instance, so `zamboni engines` reports it without a catalog. Operations return the existing result objects via a `Reportable` protocol, so `describe()` output is unchanged | done | 2026-08-03 |
| ZMBNI-1202 | Declared capabilities | `Support` is three-valued and `OperationSupport` **refuses to construct** a PARTIAL or UNSUPPORTED that names no limitation; `MaintainerCapabilities` refuses to construct at all if an operation is undeclared, since silence reads as unsupported to a caller and as an oversight to a reviewer. `can_preview` is per operation, `invariants` carries the guarantee level, and `fulfilled_by` records an operation achieved as part of another | done | 2026-08-03 |
| ZMBNI-1203 | Extract `LocalMaintainer` | `LocalMaintainer` holds what the six CLI handlers held, translated not rewritten. The 331 existing tests are the regression net and all still pass; the handlers now parse arguments, map exceptions to exit codes, and print | done | 2026-08-03 |
| ZMBNI-1204 | Trino and Spark stubs | Both stubs carry **real** capability declarations taken from ZMBNI-13, so `zamboni engines` answers what Trino and Spark would refuse before either is written, and ZMBNI-14/15 have an executable specification rather than a prose one. `execute()` raises naming its story | done | 2026-08-03 |
| ZMBNI-1205 | Config translation | `MaintenanceRequest` carries the declarative settings, never a built compactor or a resolved `RetentionPolicy` — otherwise every other engine would translate *out of* the local vocabulary instead of *from* the config. `validate()` runs at plan time: Trino refuses our 5-day and 3-day defaults against its documented 7-day floors, reported as a usage error naming the server setting | done | 2026-08-03 |
| ZMBNI-1206 | Reconcile the `--yes` contract | **Answered:** where an engine cannot preview, the rule is kept by *refusing*, not by executing and not by printing a dry-run notice over an engine about to delete. Refusing commits nothing, so "without `--yes`, nothing is committed" holds on every engine and still has no exceptions. Closes roadmap.md open question 2 | done | 2026-08-03 |

---

## ZMBNI-14 — Trino maintainer

First implementation behind the abstraction, against a stable PyIceberg. Four of six verbs
map; the two that do not are the instructive part. [roadmap.md RM-4](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1401 | Trino session | `TrinoMaintainer` takes host/port/user/catalog/version via `--trino-*` flags with `ZAMBONI_TRINO_*` fallbacks, alongside the existing `CatalogSession` rather than replacing it. The `trino` client is an optional extra, imported inside `connect()` so `zamboni engines` reports Trino's capabilities on an install that has no client | done | 2026-08-04 |
| ZMBNI-1402 | `compact` via `optimize` | `optimize`, with `file_size_threshold` from `target_file_size_bytes`. Trino's parameter *selects* files rather than sizing output, so it is translated rather than passed through, and the absent output-size control is declared instead of faked. Verified live: 6 data files to 1, 12 rows unchanged | done | 2026-08-04 |
| ZMBNI-1403 | `rewrite-manifests` via `optimize_manifests` | `optimize_manifests`. Takes no arguments — output size comes from `commit.manifest.target-size-bytes`, so `min_input_manifests` has no counterpart and is declared missing rather than silently dropped | done | 2026-08-04 |
| ZMBNI-1404 | `expire` via `expire_snapshots` | `expire_snapshots(retention_threshold, retain_last)`. **`retain_last` was added in Trino 479** — against 476 the statement fails outright with "property 'retain_last' does not exist", found by running it. Gated on `--trino-version`, unknown assumed older, and `validate()` reports the dropped `min_snapshots_to_keep` rather than losing it quietly | done | 2026-08-04 |
| ZMBNI-1405 | `remove-orphans` via `remove_orphan_files` | `remove_orphan_files(retention_threshold)`, with the weaker guarantee declared: none of the five client-side invariants apply. The 7-day floor is enforced by the server and pinned by a live test | done | 2026-08-04 |
| ZMBNI-1406 | Declare what it cannot do | Z-order confirmed absent from the connector source (zero hits in 474 files) and no open issue proposes it. Corrected two declarations that only a running engine could disprove: `optimize` *does* sort, and `apply-properties` needs Trino's own property names because the Iceberg ones are refused even via `extra_properties`. FR-11 | done | 2026-08-04 |
| ZMBNI-1407 | Live verification | Trino 483 in the dev stack behind the `trino` compose profile, and 7 live tests asserting every generated statement is accepted, that compaction actually compacts, and that the retention floor really is enforced. CI's dev-stack job starts it. Two defects were found here and nowhere else | done | 2026-08-04 |

---

## ZMBNI-11 — PyIceberg 0.12

Parallel, on `feature/pyiceberg-0.12`. Cannot land on `main` until 0.12 is on PyPI, because
the path dependency makes `uv.lock` unreproducible elsewhere. [roadmap.md RM-1](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1101 | The branch and the path dependency | `feature/pyiceberg-0.12` against the `../iceberg-python` checkout, with the lock-file consequence documented where someone will hit it | todo | |
| ZMBNI-1102 | Re-probe and record the delta | Three of seven probes flip: streaming writes, manifest predicate pruning, and derives-delete-predicate, all False → True. The last two are the pair whose *split* would make Zamboni refuse to run; they land together, which is why this migration is lower risk than its size suggests. Assert the `doctor` output rather than describing it | todo | |
| ZMBNI-1103 | Audit the private APIs we drive | The real work: 397 commits since 0.11.1. Known movers — `_scan_plan_helper` renamed to `_plan_manifest_entries`, `BaseScan`/`ManifestGroupPlanner` extracted, and `ManifestEntry.snapshot_id`'s setter fixed for writing to the wrong index, which `manifests.py` depends on preserving exactly | todo | |
| ZMBNI-1104 | Adopt streaming writes | `_dataframe_to_data_files` now accepts `pa.RecordBatchReader`, so the writer bin-packs the stream itself. May retire part of the chunked backend; measure before deleting | todo | |
| ZMBNI-1105 | Re-verify on 0.12 | Full suite plus live verification. Neither waited-on blocker lifts — `ManifestWriterV2.content()` still returns `DATA`, there is still no `ManifestWriterV3`, and the equality-delete guard is still present — so ZMBNI-604 and 704–706 stay blocked and should be re-confirmed, not assumed | todo | |
| ZMBNI-1106 | Decide the support window | One PyIceberg line or two. Open question 1 in roadmap.md: the probes make both possible, the cost is a doubled test matrix | todo | |

---

## ZMBNI-15 — Spark maintainer

Last: the largest dependency footprint for the engine that overlaps us most.
[roadmap.md RM-5](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1501 | Spark session | PySpark and py4j as an optional dependency group, so nobody installs a JVM to compact locally. `../ice-keeper` is working prior art | todo | |
| ZMBNI-1502 | `compact` via `rewrite_data_files` | Including `sort` and `zorder` — the one engine besides ours that can Z-order | todo | |
| ZMBNI-1503 | `rewrite-manifests` and `expire` | `rewrite_manifests`, `expire_snapshots` | todo | |
| ZMBNI-1504 | `remove-orphans` | `remove_orphan_files`, with the same guarantee caveat as Trino's | todo | |
| ZMBNI-1505 | Dangling deletes via `rewrite_position_delete_files` | The case that justifies the interface carrying capability in both directions: Spark rewrites *partially* dangling delete files, which is ZMBNI-604 — blocked for us on PyIceberg, available here | todo | |
| ZMBNI-1506 | Live verification | Against a real Spark. Open question 3 | todo | |

---

## ZMBNI-17 — DevOps CLI and operations

The runbook is right and too long to be a daily interface. Six invocations in a fixed order,
each needing catalog flags, is a shell wrapper waiting to be written badly. This epic makes
the tool the wrapper. [devops.md](devops.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1701 | A `maintenance` command | `zamboni maintenance` runs the six operations in the runbook order over every configured table, reusing the single-verb handlers so behaviour cannot drift between the two entry points. Returns the **worst** exit code any operation produced, and stops a table after an exit-4 abort — everything after it reads the same state, so continuing would be more work on a warehouse we have just said we do not trust | done | 2026-08-04 |
| ZMBNI-1702 | `--profile zamboni.yml` | `--profile`, defaulting to `./zamboni.yml` then `$ZAMBONI_ROOT/zamboni.yml`. Unknown keys and unknown operations are refused at load, matching table-config.json: a silently ignored key is a setting the operator believes is in force and is not. `zamboni.yml.sample` is committed | done | 2026-08-04 |
| ZMBNI-1703 | `--env` and `env.sample` | `--env`, defaulting to `./.env`. A real environment variable beats the file, so a container injecting secrets properly is not overridden by a stale dotenv. `env.sample` lists every variable including the ones most users will not need, so there is one place to look. Root `.env` added to .gitignore — it was not covered before | done | 2026-08-04 |
| ZMBNI-1704 | `--status` | `--status` reports file counts and bytes per table before and after. Row counts are compared too, and a change is printed to stderr as an alarm: maintenance must never move them | done | 2026-08-04 |
| ZMBNI-1705 | Usable `--help` | A worked example at the top of `--help` — the cron line, the one `--yes` rule, all four exit codes, and pointers to devops.md and runbook.md. For whoever inherits the cron entry without reading the docs | done | 2026-08-04 |
| ZMBNI-1706 | Per-warehouse configuration layout | `{root}/configs/{warehouse}/table-config.json`, resolved automatically from `--warehouse`, with `ZAMBONI_ROOT` defaulting to `~/.zamboni`. One file per customer in a predictable place | done | 2026-08-04 |
| ZMBNI-1707 | Warehouse discovery | `zamboni warehouses`, one name per line, from Lakekeeper's management API. Warehouse listing is not in the Iceberg REST spec, so an empty result means "cannot tell" and says so rather than presenting an empty fleet as fact. Output is deliberately plain because its job is to be input to a crontab generator | done | 2026-08-04 |
| ZMBNI-1708 | The DevOps guide | [devops.md](devops.md): the cron line, why there is no shell wrapper, the zamboni.yml/.env split, the multi-tenant layout, and why per-warehouse invocation beats one loop | done | 2026-08-04 |

---

## What is actually left

Two kinds of remaining work: finishing `v0.1.0`'s loose end, and the roadmap.

**In flight — ZMBNI-905.** The CI workflow exists and every command in it was run locally,
but GitHub has never executed it: this repository has no remote. That is the single item
standing between "tests pass on my machine" and "tests pass". `v0.1.0` is tagged; `1.0.0`
waits on this going green and on one maintenance cycle against a warehouse we did not build,
because "verified" is what a 1.0 claims. See [releasing.md §4](releasing.md).

**Blocked upstream — ZMBNI-604, 704, 705, 706.** No decision to make. Each carries either a
capability probe or a named upstream limitation, and each lifts on its own when PyIceberg
grows the capability. They are tracked so nobody re-investigates from scratch. Worth noting
that **PyIceberg 0.12 does not lift any of them** — verified against `main`, see ZMBNI-1105 —
so they are not waiting on ZMBNI-11.

**ZMBNI-507 is fixed.** Orphan removal refused nothing when another table shared this table's
location, so another table's files were listed, found unreferenced by this one, and deleted —
reproduced end to end before the fix. Found by ZMBNI-1601 comparing against ice-keeper, which
guards exactly this. The fix is the fourth checked invariant in design.md §6.6. It shipped in
`v0.1.0`, so the next release carries a **SAFETY** entry per
[releasing.md §1](releasing.md).

**The roadmap — ZMBNI-11 … 16.** Six features, defined in [roadmap.md](roadmap.md). The theme
is to stop being one implementation: three engines can do this work, and Zamboni should be one
of them behind a common interface. Delivery order is ZMBNI-13 + ZMBNI-16 (analysis, together)
→ ZMBNI-12 (the interface) → ZMBNI-14 (Trino) → ZMBNI-11 (0.12, parallel on a branch) →
ZMBNI-15 (Spark). **The two analysis epics are done** —
[engine-comparison.md](engine-comparison.md) and
[ice-keeper-comparison.md](ice-keeper-comparison.md) — and between them they corrected three
claims in roadmap.md and produced the defect above. ZMBNI-12 is next and is now specified
rather than sketched: engine-comparison.md §6 lists six concrete requirements on it.

**Closed as a decision — ZMBNI-605.** Splitting one partition across manifests would defeat
the pruning that manifest regrouping exists to create, so it is cancelled rather than
deferred. Recorded explicitly so the question is not reopened as though it were an oversight.
An earlier draft of plan.md §6 wrongly listed it alongside the genuine upstream blocker;
that has been corrected.
