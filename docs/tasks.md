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
| ZMBNI-12 | Maintainer interface | `Maintainer` ABC, `LocalMaintainer` extracted, Trino and Spark stubs, capabilities that refuse rather than degrade. [roadmap.md RM-2](roadmap.md) | todo | |
| ZMBNI-13 | Zamboni vs Trino vs Spark | The analysis that determines the interface seam. [roadmap.md RM-3](roadmap.md) | todo | |
| ZMBNI-14 | Trino maintainer | `ALTER TABLE … EXECUTE` for four of six verbs, refusing the other two. [roadmap.md RM-4](roadmap.md) | todo | |
| ZMBNI-15 | Spark maintainer | Iceberg Spark procedures, including the one operation we cannot do locally. [roadmap.md RM-5](roadmap.md) | todo | |
| ZMBNI-16 | Zamboni vs ice-keeper | A deployed comparable with operational layers we lack, and prior art for the interface. [roadmap.md RM-6](roadmap.md) | todo | |

> **On section order.** Epics ZMBNI-1…10 appear below in numeric order; the roadmap epics
> ZMBNI-11…16 appear in *delivery* order (13, 16, 12, 14, 11, 15), which is not their numeric
> order. The numbers follow [roadmap.md](roadmap.md)'s RM-1…RM-6 so the two documents agree on
> identity; the sequence is explained in each section's subtitle.

**Story counts:** 47 done · 1 inproject · 38 todo · 1 cancelled  (87 stories)

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
| ZMBNI-1301 | Enumerate each engine's surface | Exact procedure and parameter names from primary docs, not memory: Trino `ALTER TABLE … EXECUTE` (`optimize`, `optimize_manifests`, `expire_snapshots`, `remove_orphan_files`, `drop_extended_stats`) and the Iceberg Spark procedures. Preliminary passes are already recorded in roadmap.md's verified-facts table; this is the deliberate version | todo | |
| ZMBNI-1302 | Map onto the six verbs | Which of Zamboni's verbs each engine can express, and the common denominator the interface must cover. Known: Trino maps four of six | todo | |
| ZMBNI-1303 | Semantic-difference register | The same-sounding operation that differs. Known already: Trino has no Z-order and no sort on `optimize`; its `retention_threshold` has a configured floor (default `7d`) that *rejects* our 5-day and 3-day defaults rather than honouring them; `retain_last` is the spec's `min-snapshots-to-keep` under another name; `max-ref-age-ms` has no Trino equivalent | todo | |
| ZMBNI-1304 | Which safety invariants survive delegation | Zamboni's orphan removal is fenced client-side by five invariants that abort rather than delete (design.md §6.6): completeness, non-empty categories, current metadata sacred, list-before-reachable ordering, and a mandatory age guard. A server-side `remove_orphan_files` gives up all five. "Both support remove-orphans" is true and misleading, and the interface has to carry the difference | todo | |
| ZMBNI-1305 | Recommend the seam | With the rejected alternatives recorded: SQL-statement generation (ice-keeper's, unavailable to us) and metadata manipulation (ours, unavailable to them) | todo | |

---

## ZMBNI-16 — Zamboni vs ice-keeper

Alongside ZMBNI-13, because its abstraction is an input to the interface design rather than
a retrospective. [roadmap.md RM-6](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1601 | Inventory it | `../ice-keeper`: 8 399 lines of PyIceberg 0.10.x plus PySpark over py4j, Airflow-scheduled nightly. Actions for expiry, fast expiry via a Java implementation, orphans, rewrite, optimization, lifecycle and config auditing | todo | |
| ZMBNI-1602 | Capability gaps, both directions | Theirs that we lack are operational rather than algorithmic: a maintenance schedule table, a journal of what ran with status and timings, partition-health diagnosis driving which partitions get optimized, **S3 storage inventory reports as an orphan-detection input** — which sidesteps the bucket listing we depend on and that a remote-signing warehouse refuses outright — a config auditor, and failure notification | todo | |
| ZMBNI-1603 | What its seam teaches ZMBNI-12 | `ActionStrategy.prepare_statement_to_execute() -> str` is the closest prior art to our interface, and a useful negative result: the SQL seam works only because both its backends are SQL engines. Worth having in hand before designing ours | todo | |
| ZMBNI-1604 | Recommend per capability | Adopt, adapt, or decline — not a licensing or adoption decision in itself. The schedule/journal layer is the one to decide deliberately, since it is arguably a maintenance *service* rather than a maintenance *tool*. Open question 4 | todo | |

---

## ZMBNI-12 — Maintainer interface

The class hierarchy everything after it plugs into. [roadmap.md RM-2](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1201 | The `Maintainer` ABC | The six operations plus `describe`/`plan`, returning the existing result objects so `describe()` output stays engine-independent | todo | |
| ZMBNI-1202 | Declared capabilities | Per-operation support *and* guarantee level, following `capabilities.py`'s existing posture: refuse rather than silently degrade. An unsupported option must be an error, not an ignored argument | todo | |
| ZMBNI-1203 | Extract `LocalMaintainer` | A refactor with no behaviour change; the 327 existing tests are the regression net that proves it | todo | |
| ZMBNI-1204 | Trino and Spark stubs | Registered so `--engine` can list them, each raising with the reason it is not implemented rather than failing obscurely | todo | |
| ZMBNI-1205 | Config translation | `table-config.json` to each engine's parameter vocabulary, validating floors and unsupported options at plan time rather than at commit time | todo | |
| ZMBNI-1206 | Reconcile the `--yes` contract | The hardest part. "Without `--yes`, nothing is committed" has no counterpart in `ALTER TABLE … EXECUTE` or a Spark `CALL`. A maintainer that cannot preview must say so, and the CLI must not print a dry-run notice over an engine that has no such mode — that would turn the one rule we made exceptionless back into a lie. Open question 2 in roadmap.md | todo | |

---

## ZMBNI-14 — Trino maintainer

First implementation behind the abstraction, against a stable PyIceberg. Four of six verbs
map; the two that do not are the instructive part. [roadmap.md RM-4](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1401 | Trino session | Connection and catalog/schema resolution, alongside the existing `CatalogSession` rather than replacing it | todo | |
| ZMBNI-1402 | `compact` via `optimize` | `file_size_threshold`, and the `WHERE` clause on partition columns or `"$file_modified_time"` as the partition selector. Must refuse a config asking for sort or Z-order | todo | |
| ZMBNI-1403 | `rewrite-manifests` via `optimize_manifests` | Trino clusters manifests by partitioning columns, which is what ours does; confirm the semantics match before claiming equivalence | todo | |
| ZMBNI-1404 | `expire` via `expire_snapshots` | `retention_threshold`, `retain_last`, `clean_expired_metadata`. The min-retention floor must be surfaced as a config error naming the server setting, not passed through to fail server-side | todo | |
| ZMBNI-1405 | `remove-orphans` via `remove_orphan_files` | And report the weaker guarantee honestly: none of our four client-side invariants apply | todo | |
| ZMBNI-1406 | Declare what it cannot do | Z-order, dangling-delete removal, partition-evolution ageing, and preview. Each an explicit refusal with a reason | todo | |
| ZMBNI-1407 | Live verification | Against a real Trino. Depends on open question 3 — whether the dev stack grows one | todo | |

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

**The roadmap — ZMBNI-11 … 16.** Six features, defined in [roadmap.md](roadmap.md). The theme
is to stop being one implementation: three engines can do this work, and Zamboni should be one
of them behind a common interface. Delivery order is ZMBNI-13 + ZMBNI-16 (analysis, together)
→ ZMBNI-12 (the interface) → ZMBNI-14 (Trino) → ZMBNI-11 (0.12, parallel on a branch) →
ZMBNI-15 (Spark). Nothing is started.

**Closed as a decision — ZMBNI-605.** Splitting one partition across manifests would defeat
the pruning that manifest regrouping exists to create, so it is cancelled rather than
deferred. Recorded explicitly so the question is not reopened as though it were an oversight.
An earlier draft of plan.md §6 wrongly listed it alongside the genuine upstream blocker;
that has been corrected.
