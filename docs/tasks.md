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
| ZMBNI-15 | Spark maintainer | Iceberg Spark procedures, including the one operation we cannot do locally. Verified against a live Spark 3.5.9. [roadmap.md RM-5](roadmap.md) | done | 2026-08-07 |
| ZMBNI-16 | Zamboni vs ice-keeper | Delivered as [ice-keeper-comparison.md](ice-keeper-comparison.md). Found ZMBNI-507, a data-loss path in shipped code. [roadmap.md RM-6](roadmap.md) | done | 2026-08-03 |
| ZMBNI-17 | DevOps CLI and operations | One command a cron line can call, config that is not twenty flags, and a fleet story for multi-tenant warehouses. [devops.md](devops.md) | done | 2026-08-04 |
| ZMBNI-18 | Licence and publication | Apache-2.0, the files a public repo needs, and the order in which to flip the switch. Gated on ZMBNI-905: publishing a repo whose CI has never run invites an assumption we cannot back | todo | |
| ZMBNI-19 | User guide and API surface | The documentation two audiences actually need — an administrator running many warehouses and a home gamer running one — plus the public API that makes the first of those possible. Split the runbook by audience. [user_guide.md](user_guide.md) | done | 2026-08-10 |

> **On section order.** Epics ZMBNI-1…10 appear below in numeric order; the roadmap epics
> ZMBNI-11…16 appear in *delivery* order (13, 16, 12, 14, 11, 15), which is not their numeric
> order. The numbers follow [roadmap.md](roadmap.md)'s RM-1…RM-6 so the two documents agree on
> identity; the sequence is explained in each section's subtitle.

**Story counts:** 102 done · 1 inproject · 13 todo · 1 cancelled  (117 stories)

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
| ZMBNI-508 | Cap PyIceberg below 0.12 and harden the probe | `main` declared `pyiceberg>=0.11.1` with an **open upper bound**, so the day 0.12 publishes any `uv lock --upgrade` pulls in a release that corrupts data on partitioned `upsert` — with nobody touching our code. `main` also carried two defects that only manifest on 0.12: the narrow equality-delete probe (a false positive on a safety probe) and the evolution commit path (row duplication). Capped to `<0.12`, and both fixes cherry-picked from `feature/pyiceberg-0.12` where they were verified on both lines. Found by asking whether waiting for 0.12 was safe. FR-6.9 | done | 2026-08-06 |
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
| ZMBNI-913 | Spark in the dev stack and in CI | A Spark **Connect** server behind a `spark` compose profile, `--spark-remote`, and a CI job — the same posture as Trino, and the gap that let the ZMBNI-1507 timezone defect reach review. Connect rather than a standalone master because it puts the JVM in the container: `zamboni[spark-connect]` is `pyspark-client` at ~1.5MB against pyspark's 434MB, and the developer's Java version stops being this project's problem. Three findings only a live server gives, all in the one operation that does not go through Iceberg FileIO: `remove_orphan_files` lists through Hadoop S3A, so it needs static keys **on the server** (`spark.hadoop.*` is read at startup, so a Connect client cannot supply them); Lakekeeper advertises `s3://` while hadoop-aws registers only `s3a`, so without an `fs.s3.impl` mapping it dies on `UnsupportedFileSystemException` while the other five operations pass; and hadoop-aws 3.4.1 drags in 558MB of AWS SDK, which is most of the 1.57GB image. Also fixed a CLI defect found on the way: Spark had **no flags at all** — the maintainer read `remote`, `master` and `catalog` from options nothing ever populated, and `--trino-catalog` was silently configuring Spark. The server's session timezone is `America/New_York` on purpose; a UTC one cannot tell a correct timestamp literal from one missing its offset | done | 2026-08-07 |
| ZMBNI-914 | Stop `--all-extras` bundling the Spark clients | Found while checking whether the SPDX tags disturbed `bin/`. They did not — but `bin/` was **stale anyway**, and had been since ZMBNI-17: it predates `python-dotenv` and `pyyaml` becoming runtime dependencies and the `trino` extra existing. CI's "regenerating them must be a no-op" job would have caught all of it, and CI has never run (ZMBNI-905). ZMBNI-913 then made it incoherent rather than merely stale: `--all-extras` pulled in **both** `pyspark` and `pyspark-client`, which are mutually exclusive — both provide the `pyspark` package, so whichever installed last would win — and pushed a standalone CLI's first-run download past 500MB for a tool whose premise is not needing Spark. Extras are now named explicitly, with the rule written down: an extra belongs in the executable when its absence stops the tool doing its job, not when it enables an optional engine | done | 2026-08-10 |

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
| ZMBNI-1107 | Make `LocalMaintainer.capabilities()` probe-driven | It hardcodes limitations that are *derived* from `capabilities.detect()` — including a string asserting "ZMBNI-604, still true on 0.12" — so `zamboni engines` reports a static claim about a dynamic property. Wrong on any install whose probes differ, in the one place whose purpose is refusing to overstate capability. **A defect today, not only on 0.12**, and the reason ZMBNI-11 needs no `local-0.12` maintainer: the probes are the version mechanism. [roadmap.md RM-1](roadmap.md) **Fixed.** `capabilities()` reads `detect()`. Compaction is UNSUPPORTED on a build `unsupported_reason()` rejects, PARTIAL while equality deletes are unreadable, FULL otherwise; dangling-delete removal drops the whole-manifest caveat when a build can write a delete manifest; streaming writes are declared when present. The warehouse-derived limitation on `remove-orphans` is deliberately *not* probe-driven and pinned as such. Declarations now name the installed PyIceberg version | done | 2026-08-05 |
| ZMBNI-1101 | The branch and the path dependency | `feature/pyiceberg-0.12` against the `../iceberg-python` checkout, with the lock-file consequence documented where someone will hit it | todo | |
| ZMBNI-1102 | Re-probe and record the delta | Three of seven probes flip: streaming writes, manifest predicate pruning, and derives-delete-predicate, all False → True. The last two are the pair whose *split* would make Zamboni refuse to run; they land together, which is why this migration is lower risk than its size suggests. Assert the `doctor` output rather than describing it | todo | |
| ZMBNI-1103 | Audit the private APIs we drive | The real work: 397 commits since 0.11.1. Known movers — `_scan_plan_helper` renamed to `_plan_manifest_entries`, `BaseScan`/`ManifestGroupPlanner` extracted, and `ManifestEntry.snapshot_id`'s setter fixed for writing to the wrong index, which `manifests.py` depends on preserving exactly | todo | |
| ZMBNI-1104 | Adopt streaming writes | `_dataframe_to_data_files` now accepts `pa.RecordBatchReader`, so the writer bin-packs the stream itself. May retire part of the chunked backend; measure before deleting | todo | |
| ZMBNI-1105 | Re-verify on 0.12 | Full suite plus live verification. Neither waited-on blocker lifts — `ManifestWriterV2.content()` still returns `DATA`, there is still no `ManifestWriterV3`, and the equality-delete guard is still present — so ZMBNI-604 and 704–706 stay blocked and should be re-confirmed, not assumed | todo | |
| ZMBNI-1106 | Decide the support window | One PyIceberg line or two. Settled as an architecture question by ZMBNI-1107: with probe-driven capabilities, supporting both needs no version branching in code at all. What remains is CI spend — a matrix where some expected values differ by install. Open question 1 in roadmap.md | todo | |

---

## ZMBNI-15 — Spark maintainer

Last: the largest dependency footprint for the engine that overlaps us most.
[roadmap.md RM-5](roadmap.md)

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1501 | Spark session | PySpark as an optional `zamboni[spark]` extra — ~300MB and a JVM, so emphatically not a default. Session config is deliberately minimal: only the Iceberg extension, plus whatever the operator passes as `conf.*`. A maintenance tool silently setting `spark.sql.catalog.*` would be changing where the data is | done | 2026-08-07 |
| ZMBNI-1502 | `compact` via `rewrite_data_files` | `rewrite_data_files`, with `strategy => 'sort'` and `sort_order => 'zorder(a, b)'` — Spark is the only non-local engine that can Z-order. `sort_by_table_order` selects the strategy without naming an order, because Spark then defaults to the table's own, which is what the setting means. Verified live: 6 data files to 1, 60 rows intact | done | 2026-08-07 |
| ZMBNI-1503 | `rewrite-manifests` and `expire` | `rewrite_manifests` (7 manifests to 1, live) and `expire_snapshots`. Expire takes a **typed literal timestamp** where Trino takes a duration — both forms learned by running them: `date_sub(current_timestamp(), 7)` is rejected by the parser ("mismatched input '(' expecting STRING"), a bare string by the type checker ("cannot cast StringType to TimestampType") | done | 2026-08-07 |
| ZMBNI-1504 | `remove-orphans` | `remove_orphan_files`, and it produced two findings that only a live run gives. Spark **refuses any interval under 24 hours**, hard-coded — a third floor behaviour across three engines — and exactly 1 day is refused too, because the timestamp is computed here and evaluated moments later. And it **lists with Hadoop FileSystem, not Iceberg FileIO**, so it needs its own `spark.hadoop.fs.s3a.*` credentials even though every other operation runs on the catalog's vended ones | done | 2026-08-07 |
| ZMBNI-1505 | Dangling deletes via `rewrite_position_delete_files` | Not a standalone procedure: it is the `remove-dangling-deletes` option on `rewrite_data_files`, which the ZMBNI-13 analysis established and this implements. Both operations emit the same statement, and `maintenance` now **skips the second** — `OperationSupport.fulfilled_by` finally acts rather than documenting. Without it a Spark run would compact twice, the second time to no effect | done | 2026-08-07 |
| ZMBNI-1506 | Live verification | Against a real Spark 3.5.9 driving Lakekeeper and MinIO: all five operations succeed, 6 data files to 1, 60 rows unchanged. Java 11 caps us at Spark 3.x — Spark 4 needs 17 — which the `>=3.5,<5` range permits but a deployment must know | done | 2026-08-07 |
| ZMBNI-1507 | Independent review and revise | Reviewed by a second model, five findings, all real. The identifier one was the serious one: the plain procedure argument was derived by stripping backticks off the *quoted* form, and `quote()` doubles an embedded backtick to escape it — so ``we`ird.ta-ble`` became ``weird.ta-ble``, a different table, targeted with no error by operations that delete files. Also: `remove-dangling-deletes` was hard-coded `true`, overriding both the retention setting and a `block` policy; Z-order was unreachable from the CLI for any non-local engine because table-config ordering was only translated on the local path; the expiry timestamp carried no offset and was read in `spark.sql.session.timeZone` (measured: four hours deeper than asked); and the preview flag was spliced in with `str.replace` after the statement was built | done | 2026-08-07 |

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

## ZMBNI-19 — User guide and API surface

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1901 | Measure the local engine's memory ceiling | The question was whether `LocalMaintainer` does full-table loads. Answer: not of *rows* — expiry, manifests, dangling deletes and properties are metadata-only, and orphan removal compares path sets. But compaction holds ~2x a rewrite group's on-disk size, and the planner makes one group per (spec, partition) with no cap. Measured across three sizes: 226MB->+538MB, 450MB->+975MB, 889MB->+1840MB, growing linearly. **`MemoryMode.CHUNKED` did not bound it and the docstring claimed it did** ("peak memory is roughly one output file"); the claim is now the measurement. Isolated the cause: consuming PyIceberg's `ArrowScan.to_record_batches` and discarding every batch still grows ~1.3x, so most of it is upstream buffering | done | 2026-08-07 |
| ZMBNI-1902 | Define the public API | `__all__` was compaction-only: `TableCompactor` and its config, and nothing for the other five operations. An application wanting to expire snapshots had to import `zamboni.expire` — a private path, which is exactly what a public surface exists to prevent. Now exports the maintainer interface, the config types and the local operation classes, 48 names, with the compatibility promise stated on the list. Found while writing the guide's sample: a `MaintenanceRequest` carrying only `retention` raises on `compact`, so `config_from_table_settings` had to be exported too — the sample in the guide is run verbatim rather than written from memory | done | 2026-08-07 |
| ZMBNI-1903 | `zamboni table-config generate\|validate\|summary` | `generate` describes the catalog **as it is today** — each table's current partition spec with default retention — so the first run against it changes nothing but file sizes and an operator diffs intent against reality. `summary` answers the question `validate` does not: what would this *do*, marking every value that came from a default and naming what an unset knob resolves to rather than printing `None`. It also flags the two settings that silently do nothing on the wrong engine: Z-order on Trino, partition evolution anywhere but local. `validate-config` stays as an alias; removing a verb is breaking under [releasing.md](releasing.md) | done | 2026-08-07 |
| ZMBNI-1904 | The user guide | [user_guide.md](user_guide.md): four modes (Python API, cron + CLI, transient Trino, transient Spark), a capability table that leads with Z-order because that is the row a small deployment should choose on, secrets posture, a multi-warehouse SaaS loop, and the measured memory ceiling with partitioning named as the lever that actually works | done | 2026-08-07 |
| ZMBNI-1905 | Split the runbook by audience | [runbook-dev.md](runbook-dev.md) for running each step by hand — the order, the cadence arithmetic, sizing the orphan guard, the dev stack. [runbook.md](runbook.md) becomes what an operator opens at 08:00: exit codes first, how to get a stack trace out of cron, table status, a health check, and common failures. Reconciled with [devops.md](devops.md), which argues against shell wrappers — it is right about wrappers that reimplement the six-verb order, and the two narrow reasons that remain (a dated log, `set -euo pipefail` before a transient engine starts) are neither | done | 2026-08-07 |
| ZMBNI-1907 | Derive the summary's engine warnings | `table-config summary` warned about Z-order on Trino and partition evolution off local using hardcoded strings — a second source of truth for a fact `zamboni engines` already generates from code, so the day Trino gains Z-order the command would be right and the summary stale. `OperationSupport` had nowhere to put it: Z-order and partition evolution are layout *settings*, not verbs, so they existed only as prose inside COMPACT's `limitations` where no caller can read them. Added `LayoutFeature` and a declared `MaintainerCapabilities.layout` per engine, with `engines_lacking()` answering the question. `zamboni engines` prints it too, so the declaration is readable rather than merely present. The test that matters grants Trino Z-order at runtime and asserts the warning disappears with no CLI edit — a literal would keep passing | done | 2026-08-07 |
| ZMBNI-1906 | Bound compaction memory | Fixed in the reader, not with a group cap. The ceiling ZMBNI-1901 measured came from handing PyIceberg the whole task list: `ArrowScan.to_record_batches` materialises **each data file into a list** before yielding any of it — deliberately, to keep the work inside the executor — and drives that with `executor.map(...)`, which submits every task at once and returns results in order, so tasks that finish early hold their whole file until the consumer catches up. Reading one task per call makes peak scale with the largest **file** instead of the group. Measured with file size held at ~28MB while the group grew 4x, growth went 822/1088/1111MB to 541/527/577MB — flat, which is the property that matters. Costs ~1.5x on read, so it is the CHUNKED path only. **`memory_budget_bytes` lowered 1GiB -> 256MiB**: the old value predates CHUNKED bounding anything, so crossing it bought nothing; now IN_MEMORY on a 1GiB group measures ~2.3GiB of growth against a flat CHUNKED. The group cap was rejected rather than deferred — it would have silently degraded every Z-ordered table, since clustering quality is a function of how many rows the sort sees at once. Bounding the read escapes that: DuckDB still receives the whole group and spills to disk, pinned by `test_chunked_still_sorts_across_the_whole_group` | done | 2026-08-08 |
| ZMBNI-1908 | Measure the read cost against object storage | The ~1.5x in ZMBNI-1906 came from local files with a warm page cache, and the parallelism given up exists to hide *network* latency — so S3 was the case that mattered and was unmeasured. Measured against the dev stack's MinIO through Lakekeeper with vended credentials, 228MB in 96 files, using a TCP proxy that injects per-request RTT: 1.12x at 0ms, 1.26x at 10ms, 1.39x at 30ms, with a 0ms-proxy control confirming the harness moves both columns and not the ratio. **Cheaper on object storage than on local disk** up to somewhere past 30ms, and the memory saving holds at 0.53-0.61x regardless of latency. Two findings for the method: a client-side `S3Settings` endpoint is *ignored* — vended per-table storage config wins, so injecting latency needed a second Lakekeeper warehouse advertising the proxy — and the per-file cost scales at roughly 3.4x RTT, consistent with a few requests per file that used to overlap | done | 2026-08-10 |
| ZMBNI-1909 | Bounded read-ahead | ZMBNI-1908 showed the whole cost of ZMBNI-1906 was serialised round trips, so the fix is a window rather than a rewrite. `read_ahead_bytes` (64MiB) sizes it **in bytes, not files**, because bytes are what the memory contract is denominated in — many small files get real concurrency, which is the case with the most round trips to hide, while a few large ones fall back towards one at a time, which is the case where memory binds. Capped by `max_read_ahead_files` (8) so a group of ten-thousand 4KB files does not open ten-thousand connections. Submission is deliberately not `executor.map` — that is the upstream mistake ZMBNI-1906 exists to avoid, and it would make the window the group again; a read starts only as a finished one is handed over, bounding what is *outstanding* rather than what is running. **Matches unbounded speed**: 15.3s vs 15.9s at 10ms RTT, 25.8s vs 26.3s at 30ms, against 20.8s/36.2s serial. Costs ~70% of the unbounded peak rather than 60%, and stays flat as the group quadruples (692/840/784MB vs unbounded 822/1088/1111MB). `read_ahead_bytes=0` restores the serial read | done | 2026-08-10 |
| ZMBNI-1910 | Document every control in one place, with worked configurations | Auditing the guide for the two ZMBNI-1909 knobs found they were **unreachable**: `read_ahead_bytes` and `max_read_ahead_files` were dataclass fields wired into the backend with no CLI flag, so a release shipped settings an operator could not use. Worse, `--memory-budget-bytes` hardcoded `1 << 30` while the dataclass said 256MiB, so ZMBNI-1906's lowered threshold reached Python callers and **not the CLI** — the fix silently missed the people most likely to need it. CLI defaults now come from `CompactionConfig()` rather than repeated literals, pinned by `test_cli_defaults_match_the_dataclass` and `test_every_operational_knob_is_reachable_from_the_command_line`. Added a reference covering all four places a setting can live (table-config.json, zamboni.yml, run flags, .env) and which owns what, plus two worked configurations — general data, and day-partitioned events with day->month evolution. The event sample was wrong on first draft (`from_transform`/`to_transform`, the Python attribute names, where the file wants `from`/`to`); the schema caught it, and `test_the_documented_configurations_are_valid` now loads every sample so the next one is caught before a reader copies it | done | 2026-08-10 |

## ZMBNI-18 — Licence and publication

Whether to publish is decided; *when* and *in what state* are not. The sequencing
matters more than the checklist: pushing to GitHub is how CI finally runs, and a
private repo gets Actions minutes, so **push private, get CI green, then flip
visibility** — one click, history intact, no exposure while the first run
surfaces whatever it surfaces.

| id | title | description | status | completed-at |
|---|---|---|---|---|
| ZMBNI-1801 | Apply the licence | Apache-2.0. The same licence as Iceberg and PyIceberg, so contributions flow both ways without friction, and it grants patent rights — which matters for a tool whose whole job is manipulating a spec'd format. `LICENSE` carries the standard text only: the copy in the PyIceberg checkout appends attributions for vendored Avro, Thrift and Hive code, and we vendor none of it. Declared in `pyproject.toml` too (`license`, `license-files`, classifiers) so it travels with the wheel rather than only the repo | done | 2026-08-07 |
| ZMBNI-1802 | Decide on per-file licence headers | The story framed this as ASF-boilerplate or nothing and recommended nothing. **Neither** — the middle option resolves the tension. Measured: the full nine-line header is 520 lines, 4.5% of the codebase, and 76% of the smallest module; a one-line SPDX tag is 40 lines, 0.3%. The benefit anyone actually wants from headers is provenance that survives a file being copied out of the repo, and `# SPDX-License-Identifier: Apache-2.0` delivers exactly that at a fifteenth of the cost, in a form scanners already read. The licence's own appendix *recommends* the full notice rather than requiring it, and this is a single-licence repo that vendors nothing. Applied to all 40 files under `src/` and `scripts/` — after the shebang where there is one, and verified not to break the PEP 723 block in `verify-live.py` — with a pre-commit hook that fails on a new file without it | done | 2026-08-10 |
| ZMBNI-1803 | `SECURITY.md` | Written around the thing that makes this project different: the failure mode of a defect is somebody's data. Category 1 is anything that could delete a file something still references, **and it explicitly does not need to be attacker-triggerable to count** — a logic error that makes a live file look unreferenced fires without anyone trying, which is worse. Names the five concrete shapes of that bug, then bypassing a safety check, then credentials. Also says what is *not* a vulnerability, since exit 3 and exit 4 are refusals working as designed and would otherwise generate noise. Response times are stated as best-effort by one person rather than as a contract, because that is true | done | 2026-08-10 |
| ZMBNI-1804 | `CONTRIBUTING.md` | Five rules, each with the evidence that earned it: verify a claim before making it (three of this repo's most useful findings came from checking something everyone believed); assert the property not the implementation; exercise both PyIceberg lines when touching the capability probes; review before committing and act on findings; never let a document assert what the code does not do, and add the mechanical check when one is possible. Writing it caught two of my own claims imported from another project's conventions — a `dev-requirements.txt` that does not exist, and `configure_cursor`/`configure_connection`, which belong to dbt-duckdb rather than here — plus a third that was simply wrong, that pre-commit excludes `tests/` when in fact ruff lints them and only mypy skips them. Rule 1 catching its own author on its own page is the argument for the rule | done | 2026-08-10 |
| ZMBNI-1805 | Publication-readiness pass on the docs | The README is the shop window and currently assumes a reader who already trusts the tool. Needs: what it is and is not, `v0.1.0` and one-author framing kept prominent rather than buried in releasing.md §4, the `<0.12` cap explained where someone will hit it, and no CI badge until ZMBNI-905 is green — a badge that has never run is a claim | todo | |
| ZMBNI-1806 | Repository metadata | Description, topics and the `[project.urls]` block, so the repo is findable by someone searching for the gap it fills. Cheap, and the difference between a repo people find and one they do not | todo | |
| ZMBNI-1807 | Flip to public | Last, and only after ZMBNI-905 is green. One click; the preceding stories are what make it defensible | todo | |

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

**Publication — ZMBNI-18.** Whether to open-source is decided; the licence is applied
(Apache-2.0) and the rest is sequencing. Gated on ZMBNI-905, because publishing a repo whose
CI has never run invites an assumption we cannot back. The order that resolves the
chicken-and-egg: push **private**, let CI run, fix what it finds, then flip visibility.

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
