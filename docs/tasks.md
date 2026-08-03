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
| ZMBNI-10 | Documentation | HLD, delivery plan, config spec, verification record. See ZMBNI-1007 | inproject | |

**Story counts:** 42 done · 1 inproject · 7 todo · 1 cancelled  (51 stories)

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
| ZMBNI-901 | Unit and integration suite | 264 tests against a SQL catalog over a temp directory. plan.md §4 | done | 2026-08-03 |
| ZMBNI-902 | Safety-by-omission tests | Monkeypatch each reference category away in turn and assert nothing is deleted. Without these, enabling orphan removal by default is unjustified. FR-7.7 | done | 2026-08-03 |
| ZMBNI-903 | Live verification | Every operation against a real Lakekeeper 0.13.1 and MinIO. Found four bugs the local suite could not. [live-verification.md](live-verification.md) | done | 2026-08-03 |
| ZMBNI-904 | Dev-stack tests | 12 tests asserting the stack is configured such that reclamation *can* work, skipping cleanly when it is down | done | 2026-08-03 |
| ZMBNI-905 | CI workflow | Four jobs written and every command verified locally, but **never executed by GitHub** — this repository has no remote, so nothing runs until one is added and pushed. Expect the first run to surface something; the likeliest is the pinned `172.31.0.0/24` colliding on a runner, which the job checks for by name | inproject | |
| ZMBNI-906 | Retire `scripts/verify-live.py` | Now largely redundant with `tests/test_dev_stack.py`, which covers the same ground inside the suite. Its `--map-host` flag is also unnecessary against the dev stack. Keep only what the tests do not cover, or delete it | todo | |
| ZMBNI-907 | Release process | Version is hardcoded `0.1.0` in `pyproject.toml` with no tagging or changelog convention. Needed before anyone depends on a version number | todo | |
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
| ZMBNI-1007 | Operator runbook | The README covers invocation; there is no guidance on cadence — how often to compact, expire and sweep, and how to size the orphan guard against a warehouse's longest write. That sizing decision is currently only explained in passing | todo | |

---

## What is actually left

One story is in flight, 7 are open, and one is closed as a decision rather than a gap.

**In flight — ZMBNI-905.** The CI workflow exists and every command in it was run locally,
but GitHub has never executed it: this repository has no remote. That is the single item
standing between "tests pass on my machine" and "tests pass". Nothing else is started.

**Blocked upstream — ZMBNI-604, 704, 705, 706.** No decision to make. Each carries either a
capability probe or a named upstream limitation, and each lifts on its own when PyIceberg
grows the capability. They are tracked so nobody re-investigates from scratch.

**Ours to schedule — three.** In rough order of what would bite first:

| | |
|---|---|
| ZMBNI-906 | `scripts/verify-live.py` now duplicates `tests/test_dev_stack.py` |
| ZMBNI-1007 | No operator runbook: nothing says how often to run any of this |
| ZMBNI-907 | No release or versioning convention |

**Closed as a decision — ZMBNI-605.** Splitting one partition across manifests would defeat
the pruning that manifest regrouping exists to create, so it is cancelled rather than
deferred. Recorded explicitly so the question is not reopened as though it were an oversight.
An earlier draft of plan.md §6 wrongly listed it alongside the genuine upstream blocker;
that has been corrected.
