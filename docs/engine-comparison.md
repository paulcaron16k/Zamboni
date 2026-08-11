# Zamboni vs Trino vs Spark

What each engine can actually do, where the same-sounding operation differs, and
what that means for the maintainer interface.

**Written before any of the engines existed, and revised now that all three do.**
It began as the analysis [roadmap.md RM-3](roadmap.md) called for, recommending
an interface that had not been built; §6 now records which of its predictions
held. Where a claim has since been checked against a running server, it says so
-- several were wrong, and the corrections are more interesting than the
agreements.

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

Six mutating verbs plus `describe`, `plan`, `doctor`, `engines` and
`table-config`. The shape of the surface is the difference worth noting: Trino
and Spark take **per-invocation arguments**, Zamboni takes a **declarative file**
and derives the arguments from it.

| Verb | What it does | Where its parameters come from |
|---|---|---|
| `compact` | Rewrite small files; sort or Z-order while doing it | `table-config.json` layout; `--target-file-size-bytes`, `--min-input-files`, `--memory-mode` for how the run executes |
| `expire` | The spec's retention algorithm, then delete the files it orphans | `retention.expire_snapshots`; `--max-snapshot-age-days`, `--min-snapshots-to-keep` |
| `remove-orphans` | List storage, diff against the reachable set, age-guard, delete | `retention.remove_orphan_files`; `--older-than-days` |
| `remove-dangling-deletes` | Drop delete files that apply to no live data file | `retention.remove_dangling_deletes` |
| `rewrite-manifests` | Regroup manifest entries by partition so predicates prune | `retention.rewrite_manifests`; `--min-input-manifests` |
| `apply-properties` | Set the metadata-retention table properties | `retention.metadata` |
| `maintenance` | All six, in the runbook order, over every configured table | the profile's `operations` list |

Two consequences of the declarative shape, both visible in the table above.
Layout intent (`partition`, `ordering`, `partition_evolution`) has **no CLI flag
at all** on the engine-neutral path -- it is a property of the table that
analysts own, not of the run. And retention lives in the file rather than in the
invocation, so what may be deleted is reviewable in version control rather than
being an argument somebody typed at 02:00.

`maintenance` is also the API: `zamboni.maintain()` runs the same loop and
returns the same exit code, so an application does not reimplement the order.

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

## 1a. What PyIceberg 0.12 provides

Zamboni's local engine *is* PyIceberg, so the comparison has a fourth column
that moves on its own. Measured on 2026-08-11 by installing each line and
running Zamboni's own capability probes -- `zamboni doctor` -- rather than
reading release notes:

| Probe | 0.11.1 (current release) | 0.12.0rc1 | main @ `32f036c5` |
|---|---|---|---|
| `operation_is_injectable` | yes | yes | yes |
| `replace_summary_supported` | no | no | no |
| `streaming_write_supported` | **no** | **yes** | **yes** |
| `prunes_manifests_by_predicate` | **no** | **yes** | **yes** |
| `derives_delete_predicate` | no | no | no *(see below)* |
| `equality_deletes_readable` | no | no | no |
| `delete_manifests_writable` | no | no | no |

**What 0.12 gives Zamboni.** One thing, and it is real: `_dataframe_to_data_files`
accepts a `RecordBatchReader`, so the writer bin-packs a stream itself and the
CHUNKED path stops doing that by hand -- for unpartitioned tables only, since
partitioned streaming is [apache/iceberg-python#2152](https://github.com/apache/iceberg-python/issues/2152).

**What it does not give.** The two blockers this project has been waiting on are
untouched. Equality deletes are still refused by scan planning, so a table
carrying them is still blocked for compaction (ZMBNI-705). `ManifestWriterV2`
still hardcodes `content: data`, so dangling-delete removal is still limited to
dropping whole manifests (ZMBNI-604). Neither lifts, and both were re-probed
rather than assumed.

**What it took away, and got back.** 0.12 added manifest pruning to the overwrite
path, which is a performance win and a correctness hazard: a manifest the
predicate does not match is kept *verbatim*, including entries the operation is
deleting. Derived wrongly, that double-counts rows. `0.12.0rc1` derives it
wrongly for any non-identity transform -- verified, the 25-line reproduction in
[upstream-0.12-upsert-regression.md](upstream-0.12-upsert-regression.md) returns
`[('a',1), ('a',2), ('b',1), ('b',1)]` on rc1 where the correct answer is
`[('a',2), ('b',1)]`. The fix is on main after rc1 and the same reproduction
returns the correct answer there.

**So the cap stays until 0.12 is released *with* that fix**, not until 0.12 is
released. `pyproject.toml` pins `<0.12` and rc1 is exactly why.

> **A defect this table found in Zamboni, not in PyIceberg.** The
> `derives_delete_predicate` probe looks for
> `_SnapshotProducer._build_delete_files_partition_predicate`. The upstream fix
> derives the filter from the deleted files' *recorded* partition values, in
> `_OverwriteFiles._deleted_files_partition_filters` -- a different name on a
> different class. So the probe returns `False` on a build where the property it
> is testing for genuinely holds, `manifest_pruning_is_safe` comes out `False`,
> and Zamboni refuses to run at all on a fixed 0.12. It fails *safe* -- refusing
> rather than risking double-counted rows -- but it is wrong, and it is the same
> class of bug as the one `_guard_anywhere_in_scan_planning` was written to fix:
> a probe keyed on one symbol that upstream renamed. Tracked as ZMBNI-1109.

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

### 3a. What the missing Z-order actually costs

The largest single gap, so it is worth quantifying rather than asserting. Iceberg
skips a file when its per-column min/max cannot match the predicate, so skipping
quality is *how tightly clustered that column is within each file*.

A lexicographic sort on `(x, y)` clusters `x` perfectly and scatters `y`.
Z-order interleaves the bits of both, giving each moderate clustering. Measured
with Zamboni's own Z-order — files touched by a single-value filter, averaged
over every value:

| files | sort, filter on x | sort, filter on y | z-order, x | z-order, y |
|---|---|---|---|---|
| 8 | 1.0 | 8.0 | 4.0 | 2.0 |
| 32 | 1.0 | 32.0 | 8.0 | 4.0 |
| 64 | 1.0 | 64.0 | 8.0 | 8.0 |
| 128 | 1.0 | **128.0** | 16.0 | 8.0 |

A single-column sort is **optimal for the leading column and useless for every
other one** — filtering on `y` reads every file. Z-order costs the leading column
its perfect pruning and buys ~**√N** on both. The gap widens with table size: 16×
at 128 files, and it keeps growing, because it is N versus √N.

So Trino's limitation is not "compaction is worse". It is that **only the first
`sorted_by` column gets file skipping**, and a query whose selective predicate is
any other column reads every file in every partition it reaches.

**This is independent of partition pruning, and the two are often conflated.**
Partitioning skips whole partitions; sort order skips files within the ones that
survive. A query with no time filter but a filter on a clustered column still
skips files inside every partition — it reads more *manifests*, not more data
files. A query with a time filter but a predicate on a non-leading sort column
prunes partitions and then reads everything inside them. A full scan needs
*neither* mechanism to apply. Z-order earns its keep exactly where the selective
predicate is not the partition column.

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

## 6. What the interface became

This section was written before any of it existed, as six requirements the
analysis put on a future interface. All six shipped, so it is more useful as a
record of which predictions held. `zamboni engines` prints the live version of
this; what follows is what the analysis got right and wrong.

| Predicted | Shipped as | Held? |
|---|---|---|
| Capability is per operation, not per engine | `MaintainerCapabilities.of(operation)`, and `can_preview` is per operation | yes |
| Support is three-valued, not boolean | `Support.FULL/PARTIAL/UNSUPPORTED`, and an `OperationSupport` that is not FULL **refuses to construct** without a named limitation | yes, and stricter than proposed |
| Config translation validates, not just formats | `Maintainer.validate(operation, request)` at plan time; Trino's retention floors reject valid configs there | yes |
| An operation may be fulfilled by another | `OperationSupport.fulfilled_by`; Spark's dangling-delete removal rides on `rewrite_data_files`, and `maintenance` skips the second run | yes |
| Guarantee level is part of the contract | `OperationSupport.invariants`, reported by `describe()` | yes |
| Parameters translate, they do not pass through | `MaintenanceRequest` carries intent; each maintainer renders it | yes |

**One thing the analysis did not anticipate.** Layout capabilities are not
operations, and had nowhere to live. Z-order and partition evolution are
settings in `table-config.json`, so they fitted neither `Operation` nor
`OperationSupport`, and for a while existed only as prose inside a limitation
string -- which meant `zamboni table-config summary` warned about Z-order on
Trino from a hardcoded literal. `LayoutFeature` and a declared
`MaintainerCapabilities.layout` were added later so that warning derives from
the same declarations everything else does.

**And the two claims deferred to implementation were both answered by running
them**, which is why they were deferred rather than asserted:

- Trino's `optimize_manifests` takes no arguments at all, so it is not
  configurably equivalent to ours; output size comes from the
  `commit.manifest.target-size-bytes` table property.
- Spark's `use-starting-sequence-number` defaults true, matching our
  sequence-number preservation. Confirmed against Spark 4.0.4 rather than from
  the documentation.

A third answer arrived unasked: **nested namespaces need mutually incompatible
SQL.** Trino addresses `raw.telemetry.events` as `"ice"."raw.telemetry"."events"`
and rejects the per-level form; Spark requires `` `ice`.`raw`.`telemetry`.`events` ``
and rejects a dot inside a part. Each refuses the other's spelling of the same
table. Nothing in the documentation of either says so.
