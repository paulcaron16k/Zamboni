# Zamboni vs Trino vs Spark

What each engine can actually do, where the same-sounding operation differs, and
what that means for the maintainer interface. This is the analysis
[roadmap.md RM-3](roadmap.md) calls for; the interface it recommends is RM-2.

**Method.** Parameter names and defaults are quoted from primary sources, read on
2026-08-03: the Iceberg `docs/docs/spark-procedures.md` source and the Trino
connector documentation source, rather than the rendered pages — the rendered
Spark page returns only its table of contents to a fetcher, which is how a
summary written from it would have silently lost every argument table.

**One correction it produced immediately.** Two claims in roadmap.md were wrong
and are fixed there: Spark's `remove_orphan_files` *does* have a `dry_run`
argument, so preview is per-operation rather than absent; and dangling-delete
removal in Spark is the `remove-dangling-deletes` option on `rewrite_data_files`,
not `rewrite_position_delete_files`, which compacts delete files rather than
dropping inapplicable ones. Both are recorded in §3 and §6.

---

## 1. The three surfaces

### Zamboni (v0.1.0, PyIceberg 0.11.1)

Six mutating verbs plus `describe`/`plan`. Configuration is declarative
(`table-config.json`), not per-invocation arguments.

### Trino — `ALTER TABLE … EXECUTE`

| Procedure | Parameters |
|---|---|
| `optimize` | `file_size_threshold` (default `100MB`). `WHERE` on partition columns or metadata columns such as `"$file_modified_time"` |
| `optimize_manifests` | none; output size governed by the `commit.manifest.target-size-bytes` table property |
| `expire_snapshots` | `retention_threshold` **required**, `retain_last` (default 1), `clean_expired_metadata` (default false) |
| `remove_orphan_files` | `retention_threshold` **required** |
| `drop_extended_stats` | none |

Merge condition for `optimize`, quoted: files below the threshold are merged
"in case any of the following conditions are met per partition: more than one
data file to merge is present; at least one data file, with delete files
attached, is present."

### Spark — `CALL … system.<procedure>`

| Procedure | Key arguments |
|---|---|
| `rewrite_data_files` | `strategy` (`binpack`\|`sort`), `sort_order` (incl. `zorder(c1,c2,c3)`), `options`, `where` |
| `rewrite_manifests` | `use_caching`, `spec_id`, `sort_by` (partition transform names to cluster by) |
| `expire_snapshots` | `older_than` (default 5 days ago), `retain_last` (default 1), `max_concurrent_deletes`, `stream_results`, `snapshot_ids`, `clean_expired_metadata` |
| `remove_orphan_files` | `older_than` (default 3 days ago), `location`, **`dry_run`**, `max_concurrent_deletes`, `stream_results`, `file_list_view`, `equal_schemes`, `equal_authorities`, `prefix_mismatch_mode`, `prefix_listing` |
| `rewrite_position_delete_files` | `options`, `where` |

`rewrite_data_files` options worth naming, because they are the ones with
Zamboni counterparts: `target-file-size-bytes` (512 MB), `min-input-files` (5),
`partial-progress.enabled` (false), `use-starting-sequence-number` (true),
`output-spec-id`, `remove-dangling-deletes` (false), `delete-ratio-threshold`
(0.3), `rewrite-job-order`.

---

## 2. Mapping onto the six verbs

| Zamboni verb | Trino | Spark |
|---|---|---|
| `compact` | `optimize` | `rewrite_data_files` |
| `expire` | `expire_snapshots` | `expire_snapshots` |
| `remove-orphans` | `remove_orphan_files` | `remove_orphan_files` |
| `rewrite-manifests` | `optimize_manifests` | `rewrite_manifests` |
| `apply-properties` | `ALTER TABLE … SET PROPERTIES` | `ALTER TABLE … SET TBLPROPERTIES` |
| `remove-dangling-deletes` | **none** | `remove-dangling-deletes` option on `rewrite_data_files` |

**The common denominator is five of six.** Only dangling-delete removal is
absent from an engine, and only from Trino.

Each engine also has something the other two lack:

- **Trino** — `drop_extended_stats`.
- **Spark** — `rewrite_position_delete_files`, which *compacts* position delete
  files the way `rewrite_data_files` compacts data files. Neither Zamboni nor
  Trino has any equivalent.
- **Zamboni** — automated partition-evolution *ageing*: deciding which aged
  partitions to condense from days to months and rewriting them under a new spec.
  Spark can perform the rewrite half via `output-spec-id` ("data will be
  reorganized during the rewrite to align with the output partitioning") but
  neither engine decides *which* partitions have aged. Zamboni also applies the
  spec's `max-ref-age-ms` retention step, which neither exposes.

---

## 3. The semantic-difference register

The useful half of this analysis. Each row is a case where the same-sounding
operation is not the same operation.

| # | Difference | Why it matters to the interface |
|---|---|---|
| 1 | **Trino enforces retention floors.** `retention_threshold` must be ≥ `iceberg.expire-snapshots.min-retention` / `iceberg.remove-orphan-files.min-retention` "otherwise the procedure fails". Documented default `7d`. Spark has no floor; Zamboni's defaults are 5 days and 3 days | Zamboni's defaults are **rejected** by a default Trino, not honoured. Must fail at plan time naming the server setting, not surface as a server error |
| 2 | **`file_size_threshold` and `target-file-size-bytes` are different kinds of parameter.** Trino's is a *selection* threshold — files below it are merged. Spark's is the *output* size, with `min-file-size-bytes`/`max-file-size-bytes` doing selection | A single `target_file_size_bytes` config key cannot be passed through to both. Trino needs a translation, and there is no Trino control over output size at all |
| 3 | **Preview is per-operation, not per-engine.** Spark's `remove_orphan_files` has `dry_run` (default false); no other Spark procedure does. Trino has none. Zamboni previews all six | The capability is `can_preview(operation)`, not `can_preview()`. Getting this wrong is what would let the CLI print "dry run" over an engine that is about to delete |
| 4 | **`retain_last` is `min-snapshots-to-keep`** under another name, in both engines, default 1 — matching the spec | Vocabulary translation, not a capability gap |
| 5 | **`max-ref-age-ms` has no counterpart** in either engine | Zamboni-only. The interface must let an operation be *partially* supported: expiry works, one retention rule of it does not |
| 6 | **Dangling-delete removal is a compaction option in Spark**, not a standalone operation, and it "will generate an additional commit for the removal" | Zamboni models it as its own verb. Either the interface allows an operation to be fulfilled as a side effect of another, or the Spark maintainer runs compaction twice |
| 7 | **Zamboni's dangling-delete removal is whole-manifest only** (ZMBNI-604, blocked on PyIceberg), whereas Spark's operates per delete file | Same verb, materially weaker result. Belongs in the guarantee level, not the capability flag |
| 8 | **Orphan path normalisation is explicit in Spark** — `equal_schemes` (default `map('s3a,s3n','s3')`), `equal_authorities`, and `prefix_mismatch_mode` whose default is `ERROR - throw an exception` | Zamboni hit exactly this class of bug during live verification, where a non-idempotent path canonicalisation made every live file look unreferenced. Spark's default posture is the same as ours: abort rather than guess |
| 9 | **Spark can skip the directory listing** — `file_list_view` ("dataset to look for files in, skipping the directory listing") and `prefix_listing` | This is the answer to the remote-signing warehouse problem that blocks Zamboni's orphan removal entirely. See [ice-keeper-comparison.md](ice-keeper-comparison.md) |
| 10 | **`use-starting-sequence-number`** (default true) — Spark stamps the rewrite with the sequence number at compaction start | Zamboni preserves sequence numbers exactly in manifest rewriting; the semantics need confirming against each other before either is described as equivalent |
| 11 | **Partition selection differs in kind.** Trino filters with `WHERE` on partition columns; Spark takes a `where` predicate string; Zamboni selects from declarative rules in `table-config.json` | The config-to-engine translation is a real component (ZMBNI-1205), not a formatting step |
| 12 | **`partial-progress.enabled` defaults to false in Spark**, as it does in Zamboni after ZMBNI-106; Trino does not expose it | Agreement worth recording — Zamboni's choice matches Iceberg's own default |

---

## 4. Which safety invariants survive delegation

Zamboni's orphan removal is fenced by five invariants (design.md §6.6) that
**abort the run rather than delete**: completeness of the referenced set,
non-empty reference categories, current metadata never a candidate,
list-before-reachable ordering, and a mandatory age guard.

Delegating to a procedure gives up all five, because they are properties of *our*
implementation. But the honest conclusion is not "delegation is less safe" — it
is that the guarantees are **different, and not ordered**:

| Guarantee | Zamboni | Trino | Spark |
|---|---|---|---|
| Age guard | yes, default 3 days, **overridable to 0** (`--reclaim-now`) | yes, and a **floor you cannot go below** | yes, default 3 days, no floor |
| Abort on path ambiguity | yes (completeness invariant) | not documented | yes — `prefix_mismatch_mode=ERROR` by default |
| Preview before deleting | yes | no | yes, `dry_run` |
| Scope restricted to the table | yes, by construction from its location | not documented | `location` argument, defaults to the table's |
| Refuses when its own view looks wrong | yes, four ways | not documented | one way |

**Trino is stricter than Zamboni in exactly one respect and it is the dangerous
one.** Zamboni lets an operator set the age guard to zero; Trino refuses below
the configured floor. Our runbook already says `--reclaim-now` is for warehouses
nothing else is writing — Trino makes that unrepresentable rather than
discouraged.

So the interface should carry guarantee level *per operation per engine*, and
`describe()` should be able to say which invariants are in force for the engine
about to run. "Both support remove-orphans" is true and, on its own, misleading.

---

## 5. The recommended seam

**Operation-level, with declared capabilities and guarantees.** A `Maintainer`
exposes the six operations; each implementation translates one operation into its
native mechanism and declares, per operation, what it can do and how strongly.

Two alternatives were considered and rejected:

- **SQL-statement generation** — ice-keeper's seam: every action returns a SQL
  string for the engine to run. Coherent when every backend is a SQL engine.
  Rejected because Zamboni manipulates metadata through PyIceberg and has no SQL
  to emit; it is the only one of the three that is not a query engine.
- **Metadata manipulation** — Zamboni's own internals, i.e. make the others look
  like us. Rejected because Trino and Spark expose procedures, not manifest
  writers. This direction is not merely inconvenient, it is unavailable.

The seam is forced: operation-level is the only level all three implement.

---

## 6. Consequences for ZMBNI-12

Concrete requirements this analysis puts on the interface:

1. **Capability is per operation, not per engine** — driven by §3 row 3: preview
   exists for one Spark operation and no others.
2. **Support is not binary.** `expire` on Trino works but cannot honour
   `max_ref_age_days`; `remove-dangling-deletes` on Zamboni works but only for
   whole manifests. The interface needs *supported / partially supported with a
   named limitation / unsupported*, not a boolean.
3. **Config translation validates, it does not just format.** Trino's floors
   (§3 row 1) mean a valid `table-config.json` can be invalid for a given engine.
   That check belongs at plan time.
4. **An operation may be fulfilled by another operation.** Spark does
   dangling-delete removal as a compaction option (§3 row 6).
5. **Guarantee level is part of the contract** (§4), and `describe()` should
   report it.
6. **Parameters translate, they do not pass through** (§3 rows 2, 4, 11).

**Deferred to implementation, deliberately.** Whether Trino's `optimize_manifests`
clustering is equivalent to ours, and whether `use-starting-sequence-number`
matches our sequence-number preservation, are claims that should be verified
against a running engine (ZMBNI-1403, ZMBNI-1407) rather than asserted from
documentation. They are recorded here as open, not as answered.
