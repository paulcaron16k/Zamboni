# The PyIceberg private API this depends on

**What Zamboni reaches into, why each reach is unavoidable, and what stops an
upstream rename from becoming a corrupted table.**

| | |
|---|---|
| Audience | Anyone changing `committer.py`, `evolution.py`, `deletes.py`, `manifests.py` or `capabilities.py` |
| Companion | [design.md §6.2](design.md) — the upstream constraints · [../CONTRIBUTING.md](../CONTRIBUTING.md) rule 3 — testing across both PyIceberg lines |
| Verified | Every row below re-probed against **0.11.1** and against the 0.12 line on 2026-08-29. The latter is `../iceberg-python` at `0bf4d13d` — `pyiceberg-0.12.0rc1-47-g0bf4d13d`, an unreleased post-rc1 tree that declares `version = "0.12.0"`. It is *not* a published release; PyPI's newest final is 0.11.1 |

---

## 1. Why there is any of this

Zamboni's job is to rewrite data files and commit the result as an Iceberg
`replace` snapshot. PyIceberg's public surface cannot express that:

- `UpdateSnapshot.overwrite()` chooses between `Operation.OVERWRITE` and
  `Operation.APPEND` by whether the branch already has a snapshot — `REPLACE` is
  not among the options — and
  `update_snapshot_summaries` rejects anything outside
  `{APPEND, OVERWRITE, DELETE}` — on both lines. `replace` is the spec's own
  operation for "data and delete files were added and removed without changing
  table data; i.e., compaction", and it is not cosmetic: incremental and CDC
  readers use the operation to decide whether a snapshot changed rows.
- `add_files` infers partition values from column statistics, which is wrong for
  any non-order-preserving transform — a `bucket`-partitioned rewrite lands rows
  in the wrong partition.
- `expire_snapshots()` is metadata-only and deletes no files. Orphan removal has
  no public analogue at all.

So the choice is not "private API or public API". It is "private API, or do not
build the tool". What follows is the bill for that decision, itemised.

---

## 2. The inventory

**Four** private symbols imported, **three** upstream base classes subclassed,
**four** private methods overridden, **nine** private instance attributes read,
one inherited private method called without being overridden, and three reaches
that are not attribute access at all (§2.6). Small and concentrated, and every
count below was re-derived against this branch — an earlier revision of this
document carried §2 over from another branch unchecked and got four of these
wrong.

### 2.1 Imported symbols

Four symbols, at five import sites (`git grep -nE "from pyiceberg.*import.*\b_"`).

| Symbol | Imported by | What public API cannot do |
|---|---|---|
| `_OverwriteFiles` | `committer.py:31`, `capabilities.py:115` | Base for `_ReplaceFiles`. Nothing public emits a `replace` snapshot |
| `_SnapshotProducer` | `capabilities.py:115` **only** | Probed for the `operation` constructor argument. `evolution.py` names it in prose and does not import it |
| `_FastAppendFiles` | `testing.py:43` | Register **position delete files**, which PyIceberg cannot write at all |
| `_dataframe_to_data_files` | `backends/duckdb_arrow.py:29`, `capabilities.py:318` | Computes the partition key **from the data**, so `bucket` works. `add_files` infers it from statistics |

`PyArrowFileIO._initialize_fs` is **not** in this table: it is never imported, only
reached as an attribute, and it is counted once in §2.6.

`zamboni.testing` is **not** test-only.  `zamboni/demo/ingest.py:28` imports
`locate_rows` and `write_position_deletes` from it, and `zamboni-demo` is a
shipped console script — so `_FastAppendFiles` is on a user's path whenever the
demo simulates merge-on-read. The module's own docstring says "tests *and the
demo*"; an earlier revision here said "never on a user's path", which was wrong.

### 2.2 Subclassed base classes

`_ReplaceFiles` (in `committer.py`) is the hub: it subclasses `_OverwriteFiles`
and is itself the base for three more producers. So an upstream rename of
`_OverwriteFiles` breaks **four modules at runtime** — `committer.py` where it is
imported, and `evolution.py`, `manifests.py`, `deletes.py` through inheritance —
plus `capabilities.py`, which imports it to probe. (A previous revision said six,
counting `profile.py` and `deletes.py`'s docstring mentions; §2.5 says prose does
not count, so it cannot count here either.)

```
_OverwriteFiles                     (pyiceberg.table.update.snapshot)
└── _ReplaceFiles                   committer.py     -- relabels the snapshot `replace`
    ├── MultiSpecReplaceFiles       evolution.py     -- one commit spanning two partition specs
    ├── _RewriteManifests           manifests.py     -- regroup manifests, touch no data
    └── _RemoveDeleteFiles          deletes.py       -- drop whole delete manifests
_FastAppendFiles                    (same module)
└── _AppendDeleteFiles              testing.py       -- register position delete files
ManifestWriterV2                    (pyiceberg.manifest -- public name, private contract)
└── _DeleteManifestWriter           testing.py:206   -- label a manifest `content: deletes`
```

The third base is easy to miss because its name carries no underscore.
`ManifestWriterV2.content()` returns `ManifestContent.DATA` unconditionally, which
is the fact `delete_manifests_writable` probes and the reason dangling-delete
removal can only drop whole manifests — so subclassing it to override `_meta` is
as much a private-contract dependency as anything above.

### 2.3 Overridden private methods

Each override exists because the inherited behaviour is wrong for the operation,
not because it is inconvenient.

Four names, at the sites `git grep -nE "super\(\)\._"` reports.

| Override | In | Why |
|---|---|---|
| `_summary()` | `committer.py:74` **only** | Let PyIceberg compute the totals as an overwrite, then relabel the finished summary as whatever was asked for. `evolution.py`'s copy is gone: its per-spec half moved into the maintenance fork, and the operation-label half turned out to be correcting a hardcode in `_ReplaceFiles`, now fixed there (ZMBNI-59) |
| `_existing_manifests()` | `deletes.py:97` **only** | Refuse the one path that corrupts metadata: upstream rewrites a partially-emptied manifest through `write_manifest`, which stamps `content: data`. Delegates to `super()` otherwise |
| `_manifests()` | `manifests.py` (via `_RewriteManifests`), `testing.py` | `evolution.py`'s override is **gone**: `_SnapshotProducer._manifests` used to declare the added manifest under `table_metadata.spec()` while grouping deleted entries by each file's own spec, and the maintenance fork now groups both the same way |
| `_meta` | `testing.py:219` | A **property**, not a method. Labels a manifest `content: deletes`, which `ManifestWriterV2` will not do |

**`evolution.py` now overrides nothing.** It had three: `_manifests`, `_summary`
and a `_surviving_manifests` that replaced upstream's pruning. The first moved
into the fork; the second was correcting its own parent; the third existed only
because of ZMBNI-58, a call this project had failed to make, and was measurably
unnecessary once that was fixed. `MultiSpecReplaceFiles` survives as a name so
the committer's producer choice still reads as a decision.

That is what this document is for. Three entries left this table in one change,
and two of them were never upstream's problem.

### 2.4 Inherited private attributes

Read by our subclasses, assigned by PyIceberg. **This is the least visible part
of the surface** — an import that disappears is an `ImportError` on the next run,
whereas an attribute that is renamed is an `AttributeError` in the middle of a
commit.

`_transaction` (19 uses) · `_snapshot_id` (7) · `_io` (6) ·
`_parent_snapshot_id` (5) · `_compression` (5) · `_deleted_data_files` (4) ·
`_added_data_files` (3) · `_target_branch` (2) · `_operation` (set by us, read by
upstream)

Counts are code references only, excluding prose. `_compression` was absent from
an earlier revision of this list — read at `evolution.py:163`, `:190`, `:252`,
`manifests.py:268` and `testing.py:238`, every one of them passing
`avro_compression=` to `write_manifest`. Missing it is exactly the failure this
subsection warns about: it appears in no import, so an audit that greps imports
does not see it, and a rename surfaces as an `AttributeError` mid-commit.

### 2.5 Inherited private methods called without being overridden

`self._process_manifests(...)` at `evolution.py:196` — `_SnapshotProducer`'s, on
both builds. Distinct from §2.3 because there is no `super()` call to grep for and
no `def` in our source: it is invisible to both of the searches that find
everything else here.

### 2.6 Other private reaches

| Call | In | Why |
|---|---|---|
| `Transaction._apply(...)` | `evolution.py:422` | Apply an `AddPartitionSpecUpdate` plus its `AssertTableUUID` in one commit |
| `hasattr(io, "_initialize_fs")` then `io._initialize_fs(...)` | `orphans.py:216-217` | The fsspec filesystem needed to **list** storage; `FileIO` exposes open/delete and no listing. Guarded — a non-PyArrow `FileIO` falls back rather than crashing |
| `from pyiceberg.table.delete_file_index import DeleteFileIndex` | `deletes.py:180` | Answers "does this delete file still apply" with upstream's own index rather than a second implementation of the rule. No underscore, and no public-API promise either |

**And the most fragile reach in the repository, which has no row above because it
is not attribute access at all:** three probes depend on upstream's *source text*.

| Reach | In | Breaks when |
|---|---|---|
| `"manifest_evaluator" in getsource(_OverwriteFiles._existing_manifests)` | `capabilities.py:118` | that method's body is refactored |
| `"does not yet support equality deletes" in getsource(pyiceberg.table)` | `capabilities.py:274-278` | an **error message** is reworded |
| `inspect.signature(_dataframe_to_data_files).parameters["df"].annotation` | `capabilities.py:320` | a type annotation changes — **which it did**, see §3 |

Each fails *silently*, to its own safe direction, so the cost is a capability
believed absent forever rather than a crash. That is the whole subject of
[#36](https://github.com/paulcaron16k/Zamboni/issues/36).

`Table._do_commit`, `ExpireSnapshots._commit`, `DataScan._plan_files_local` and
`_SnapshotProducer._manifests`/`._summary` appear in **prose only** — docstrings
in `properties.py`, `expire.py`, `profile.py`, `capabilities.py` and
`evolution.py` explaining upstream behaviour. They are not called, and are listed
so a future audit does not count them twice.

---

## 3. What this surface actually did between 0.11.1 and 0.12.0

Measured, not assumed — this is the interesting result, and it cuts against the
intuition that private APIs churn constantly.

**One signature changed; everything else in §2 survived 0.11.1 → 0.12.0
unchanged.** The exception matters more than the rule:

```
0.11.1   _dataframe_to_data_files(df: pa.Table, ...)
0.12.0   _dataframe_to_data_files(df: pa.Table | pa.RecordBatchReader, ...)
```

That is §2.1's fourth row, and `capabilities.py:316-326` exists *only* to read
that annotation — so the one signature that moved is the one a probe watches, and
§4.6 bills for its consequence (the two tests that skip by build). An earlier
revision of this document claimed nothing changed, which was both false and
self-contradictory with its own §4.6.

Everything else held: 20 signatures, the owning class of all four overridden
names, and all nine inherited attributes are identical across the two builds. One
non-signature drift worth noting — on 0.12.0 `_deleted_data_files` is declared on
`_OverwriteFiles` as well as on `_SnapshotProducer`.

Between the two builds are **108 commits touching `pyiceberg/`** (463 counting
the whole repository), by
`git log --oneline pyiceberg-0.11.1..0bf4d13d -- pyiceberg/ | wc -l`. An earlier
revision said 397 with no method given, which is reproducible by no counting I
tried.

What did change was subtler and worse:

| Change | Detected by | Consequence |
|---|---|---|
| `_build_delete_files_partition_predicate` → `..._filters` | The name-based probe going False | Zamboni refused to run on the build that *fixed* a corruption bug |
| **Behaviour** of manifest pruning changed with **no name change** | A behavioural probe | The name-based probe declared a data-corrupting build safe |
| `pyiceberg-core` moved out of the `[pyarrow]` extra | `test_the_rust_core_arrives_with_the_base_install` | Every transformed-partition write raises `NotInstalledError` |

### The three movers this audit was opened to check

`ZMBNI-1103`, the story this document closes, named three specific upstream
changes to check. All three were resolved by inspecting both installed builds
rather than by reading release notes:

| Named change | 0.11.1 | 0.12.0 | Our exposure |
|---|---|---|---|
| `_scan_plan_helper` renamed to `_plan_manifest_entries` | both absent | `_scan_plan_helper` absent; **`_plan_manifest_entries` present** as a `DataScan` method | **None**, but the evidence took two attempts. The first probe asked `hasattr(pyiceberg.table, name)`, which sees module-level attributes and not methods, so it reported both absent on both builds. It is `DataScan._plan_manifest_entries` (`pyiceberg/table/__init__.py:2412`, referenced at `inspect.py:407`). The conclusion holds — we call neither; reads go through `ArrowScan` over a hand-filtered task list — but a structural probe got the structure wrong, in the document about structural probes getting things wrong |
| `BaseScan` / `ManifestGroupPlanner` extracted | absent | **present** | **Mitigated before the fact.** This extraction turned `DataScan._plan_files_local` into a five-line delegation, which is precisely why `_guard_anywhere_in_scan_planning` searches the whole `pyiceberg.table` module instead of one function. The first version of that probe inspected the function, found no guard, and reported equality deletes as *readable* while the refusal was alive one call deeper |
| `ManifestEntry.snapshot_id`'s setter fixed for writing to the wrong index | — | — | **Verified behaviourally, and it holds.** `manifests.py` re-writes entries as `EXISTING` and passes `snapshot_id=entry.snapshot_id` to preserve the original, so a setter that wrote to the wrong index would corrupt exactly this. `test_rewrite_reduces_manifests_without_touching_data` compares every entry's `(snapshot_id, sequence_number, file_sequence_number, record_count)` before and after a rewrite, and passes on both lines |

The third is the one worth dwelling on, because it is the shape this whole
document is about: the *name* `snapshot_id` never moved, so nothing structural
would have caught a change in what assigning to it does. Only a round-trip
assertion answers it. That test already existed for the sequence-number
guarantee, and it covers this for free — which is an argument for asserting a
tuple of fields rather than the one field a story happens to be about.

Three lessons follow, and they are the whole basis of §4:

1. **Rename risk is real but loud.** An `ImportError` or `AttributeError` stops
   the run. It is the failure mode you want.
2. **Behaviour risk is quiet, and names cannot detect it.**
   `_build_delete_files_partition_predicate` exists on an early 0.12 candidate *which corrupts
   data*, and on both attempted fixes for it. Any probe that asks "does this
   symbol exist" declares the corrupting build safe.
3. **The packaging around the private API moves independently of it.** Nothing in
   §2 changed, and 0.12 still could not write a partitioned table.

---

## 4. How the risk is mitigated

Six mechanisms, in the order they engage.

### 4.1 One choke point, at the producer rather than at each verb

`assert_supported_pyiceberg()` raises `UnsupportedPyIceberg` if the installed
build fails the checks in `capabilities.py`. It is called from three places, and
it takes all three to make the coverage a property rather than a list:

| Call site | Covers |
|---|---|
| `_ReplaceFiles.__init__` | every operation committing through a private producer subclass, including one added tomorrow |
| `ReplaceCommitter.commit` | the paths where the producer is the **stock** `_OverwriteFiles` — `snapshot_operation="overwrite"`, and any build with native REPLACE summaries — plus library callers, since `ReplaceCommitter` is public API |
| `TableCompactor.execute` | refusing *before* an expensive rewrite rather than after it |

The second was missed on the first attempt at this fix, and the reason is worth
keeping: the producer class is chosen at **runtime**, so guarding the class made
coverage a property of something that is itself a decision. Two of the three
choices are not `_ReplaceFiles`, and one of them — a future PyIceberg whose
`replace_summary_supported` is true — is exactly the build class the guard exists
for.

It was not always so, and the history is the point.
[#37](https://github.com/paulcaron16k/Zamboni/issues/37): until it was fixed the
guard had exactly **one** caller, and five of the six mutating operations never
consulted it — two of them, `rewrite-manifests` and `remove-dangling-deletes`,
committing through `_ReplaceFiles` subclasses on builds the guard would have
refused. An earlier revision of this section asserted the opposite, and that
assertion is *how the gap survived*: the claim was read off the guard's docstring
rather than off its callers, and the document was then treated as evidence the
guard was sound.

`test_every_replace_producer_consults_the_guard` enumerates
`_ReplaceFiles.__subclasses__()` rather than listing today's operations, so a
subclass that overrides `__init__` without calling `super()` fails the suite
instead of quietly opting out. Verified by breaking it both ways: deleting the
guard fails the test, and a subclass calling `_OverwriteFiles.__init__` directly
fails it.

Three mutating operations deliberately do **not** consult it, with the reasons
recorded on the function itself: `expire` commits through PyIceberg's own
`ExpireSnapshots` and rewrites no manifest; `apply-properties` sets table
properties in a plain transaction; `remove-orphans` produces no snapshot at all
and is fenced instead by the deletion invariants in design.md §6.6.

### 4.2 Capability probes, never version comparisons

`capabilities.py` asks the *installed* build what it can do. This is why the same
release behaves correctly on both sides of a release boundary, and why the answer
to "does Zamboni support 0.12" is a probe result rather than a support matrix.

**Each probe's unknown-answer default is chosen for the safe direction, per
probe, and says so in a comment.** They are not the same direction: an
uninspectable build is assumed *to* prune (so it must earn its answer) and
assumed *not* to be able to write delete manifests (so we do not try).

### 4.3 Behavioural probing where names cannot answer

The pruning probe **performs an overwrite on a transformed partition and counts
what survived**. It runs once per process (`detect()` is
`lru_cache`d) and only on builds that prune at all — which is no current release.

**What it costs is not settled, and the numbers in circulation disagree.**
`capabilities.py:170` says "~150ms warm, ~600ms cold"; `docs/tasks_historical.md`
records "~1.7s once per process". Measured here against the 0.12 tree: **591, 675
and 1517 ms** for the first call in a fresh process, then 1045 / 289 / 242 /
400 ms for successive calls inside one. The figure therefore depends entirely on
whether import and filesystem caches are counted, and neither existing number
says which it measured. Treat ~0.3s as the floor and ~1.5s as a realistic cold
CLI invocation — a spread that is itself an argument for
[#39](https://github.com/paulcaron16k/Zamboni/issues/39), which would confine
this cost to untested builds. This is the single most important
mitigation on the list, because it is the only one that catches a behaviour
change that kept its name.

Its limitation, worth knowing: it returns `None` — "could not establish" — for
*any* failure, including a missing optional dependency. On 0.12 without
`pyiceberg-core`, `zamboni doctor` said "prunes incorrectly" when the truth was
"could not tell". Safe, and a misleading diagnosis.

### 4.4 Tripwires on the things a probe would not think to ask

`test_the_rust_core_arrives_with_the_base_install` guards a *packaging* fact
rather than an API. Its docstring promises it will "fail if upstream moves the
dependency out of `[pyarrow]`, rather than leaving a user to discover it by
writing a table" — and that is exactly how the 0.12 requirement was found, so the
promise has been collected on. Tripwires like this are cheap and belong wherever
an upstream *convention* is load-bearing.

**It is deliberately still the strict form here.** On 0.11.1 the assertion is
true, and it is what fires the moment a build moves the dependency.
`feature/pyiceberg-0.12` carries a line-aware replacement that accepts either
provider — deliberately unnamed here, because `test_every_cited_test_exists`
checks every test this document names and that one does not exist on this branch.
It belongs on `main` when 0.12 becomes the supported line, not before. Relaxing it early would disarm the only thing that noticed.

### 4.5 Never reconstruct what upstream will compare by identity

Removed files are passed as the `DataFile` objects read from the manifests, never
rebuilt from an inspect table, because `_OverwriteFiles._existing_manifests`
matches with `entry.data_file in self._deleted_data_files`. A reconstructed
equivalent object silently leaves stale entries live. `profile.py` reads manifests
directly for this reason and says so.

### 4.6 A branch that tracks the next line, continuously

`feature/pyiceberg-0.12` builds against a checkout of `apache/iceberg-python`, so
the delta is known before the release rather than after. It has already produced
four findings that would otherwise have been a user's bug report: the upsert
corruption ([#3758](https://github.com/apache/iceberg-python/issues/3758), fixed
by [#3780](https://github.com/apache/iceberg-python/pull/3780)), the rename, the
behaviour-without-rename, and the `pyiceberg-core` packaging move.

**The cost of keeping it is measured** and small: one mypy flag, two tests that
skip by build, and no `uv sync --frozen` while the dependency is a local path.
See [roadmap.md](roadmap.md) open question 1.

---

## 5. What is deliberately *not* done

- **No vendoring.** Copying `_SnapshotProducer` into this repository would
  freeze a snapshot of upstream's internals and guarantee divergence, while
  taking on maintenance of code nobody here wrote.
- **No abstraction layer over the private API.** A wrapper does not make the
  dependency smaller; it makes it harder to see. The inventory above is the
  documentation an abstraction would have hidden.
- **No pinning to an exact version.** `==0.11.1` would trade a loud failure for a
  silent staleness, and the cap that does exist (`<0.12`) is there for a data
  corruption bug, not as a general policy.
- **No probe for correctness.** A probe answers "can this build do X"; whether it
  does X *correctly* is a test. The pruning probe is the deliberate exception,
  and it is a test in a probe's clothing — which is why it is the expensive one.

---

## 6. Which of these should stop being ours

Reviewed 2026-09-09 against upstream `main` at `9299bdb8` (40 commits past the
0.12.0 tag). Each reach was asked two questions in turn: **is it still required**,
and **is it a gap upstream would want closed** — the second answered
adversarially, as a maintainer who has to tell user error from a defect and who
starts from the position that an outsider's patch is probably missing context.

That posture is not theoretical politeness. On 2026-09-09 a failure was traced
almost to an upstream bug report before the cause turned out to be ours:
`MultiSpecReplaceFiles._manifests()` never called
`_build_delete_files_partition_predicate()`, which the method it overrides calls
(ZMBNI-58). A pure-PyIceberg reproduction is what caught it — the same scenario
driven through `overwrite()` with no Zamboni subclass in the path **commits
fine**. Nothing below should be raised upstream without that step.

### 6.1 Still required, all of them

| Reach | Still required because | Measured |
|---|---|---|
| `_dataframe_to_data_files` | No public API writes data files without committing. `add_files`/`parquet_file_to_data_file` register files that already exist; compaction has to create them, with partition keys computed from the data so `bucket` works | source, `main` |
| `_OverwriteFiles` → `_ReplaceFiles`, `_summary` relabel | `update_snapshot_summaries` still rejects `REPLACE` (`snapshots.py:354`) | probe: `replace_summary_supported = False` |
| `_manifests` for added-file specs | Upstream still writes every added file under `table_metadata.spec()` | source, `main` |
| `_deleted_entries` no-ops | Metadata-only operations delete nothing; this is the extension point used as intended | — |
| `_existing_manifests` refusal | `ManifestWriterV2.content()` still returns `DATA` | probe: `delete_manifests_writable = False` |
| `_SnapshotProducer` | Probe only; dies with ZMBNI-38 | — |
| `_FastAppendFiles`, `_meta` | PyIceberg cannot write position deletes at all | probe, as above |

### 6.2 Upstream is already building table maintenance

The single most important finding of this review, and it changes the strategy:
**several of these workarounds have in-flight upstream work.** Checked
2026-09-09:

| Upstream | State | Overlaps |
|---|---|---|
| [PR #3131](https://github.com/apache/iceberg-python/pull/3131) metadata-only replace API | open since 2026-03-09, +1130/-82 | `_ReplaceFiles` and the `_summary` relabel — **the whole reason that subclass exists** |
| [PR #3624](https://github.com/apache/iceberg-python/pull/3624) write V3 manifests and manifest lists | open since 2026-07-08, +761/-15 | **Nothing of ours — measured.** See the correction below |
| [PR #3631](https://github.com/apache/iceberg-python/pull/3631) `rewrite_manifests` maintenance | open | `manifests.py`'s `_RewriteManifests` |
| [issue #3925](https://github.com/apache/iceberg-python/issues/3925) remove dangling delete files | open | `deletes.py` |
| [PR #3858](https://github.com/apache/iceberg-python/pull/3858) preserve explicit `delete_data_file()` | open | the delete-entry path ZMBNI-58 sits in |
| [issue #1818](https://github.com/apache/iceberg-python/issues/1818) V3 tracking | open | ZMBNI-10, ZMBNI-12 |

**#3624 was checked and does not do what its title suggests to us.** It adds
`ManifestWriterV3` and the V3 record layout; it does **not** make a delete
manifest writable. Measured against the PR: `ManifestWriterV1`, `V2` **and `V3`**
all return `ManifestContent.DATA` from `content()`, and `write_manifest` still
has no `content` parameter — its arguments are `format_version, spec, schema,
output_file, snapshot_id, avro_compression, first_row_id`. Our
`delete_manifests_writable` probe reports `False` against the PR, which is
correct.

That claim was written into this document from the PR's title before it was
measured, and it was wrong. It is left visible rather than quietly deleted,
because inferring a capability from a title is the exact failure this document
exists to prevent. **`_meta`, `_FastAppendFiles`, `_existing_manifests` and
ZMBNI-9 are unaffected by #3624.**

So the correct contribution for most of this surface is **not a new pull
request**. It is testing an in-flight one against a real workload and saying what
happened — which is a thing this project can do that the author usually cannot,
and which is worth more than a duplicate.

### 6.3 The determinations

**Contribute as a defect — one candidate.**

*Added files ignore their own partition spec* (ZMBNI-59). Inside a single method,
`_write_added_manifest` uses `spec=self._transaction.table_metadata.spec()` for
every added file while `_write_delete_manifest`, fifteen lines below, groups by
`deleted_entry.data_file.spec_id`. Upstream already does the right thing on one
side. The adversarial objection — *"adding files under a non-default spec is
unusual; is your caller wrong?"* — does not hold: partition evolution requires
rewriting aged data under the old spec, the Iceberg spec supports per-file specs,
and their own delete side honours it. No upstream PR was found for it. **It needs
a pure-PyIceberg reproduction before it is raised.**

**Contribute as an enhancement — one candidate.**

*A public way to write data files without committing* (ZMBNI-60). Every
compaction tool needs it and every one currently reaches for
`_dataframe_to_data_files`. The maintainer's objection is real — it is private
because its signature moves — and that is precisely the argument for stabilising
it.

**Support work already in flight — do not duplicate.**

*The `replace` snapshot operation* (ZMBNI-61) is PR #3131, open six months.
*Delete-manifest writing* is PR #3624. Filing either would be a duplicate; the
useful act is review and evidence.

**Not upstream material.** `_deleted_entries` no-ops and `_SnapshotProducer` —
the first is the extension point working, the second is ours to delete.

---

## 7. If you are about to change any of this

1. Read the comment above the probe or override before editing it. Several
   explain a specific bug that a "cleanup" reintroduces —
   `_guard_anywhere_in_scan_planning` searches a whole module on purpose, and
   `_guard_anywhere_in_scan_planning` searches a whole module on purpose.
2. Exercise both lines. `uv pip install -e ../iceberg-python` then
   `.venv/bin/python -m pytest` — via the venv interpreter, because `uv run`
   re-syncs from `uv.lock` and undoes the install you just made.
3. If you add a private reach, add its row to §2 and a probe or tripwire for it.
   An unlisted reach is the one that breaks a release.
