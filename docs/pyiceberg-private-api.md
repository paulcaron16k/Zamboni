# The PyIceberg private API this depends on

**What Zamboni reaches into, why each reach is unavoidable, and what stops an
upstream rename from becoming a corrupted table.**

| | |
|---|---|
| Audience | Anyone changing `committer.py`, `evolution.py`, `deletes.py`, `manifests.py` or `capabilities.py` |
| Companion | [design.md §6.2](design.md) — the upstream constraints · [../CONTRIBUTING.md](../CONTRIBUTING.md) rule 3 — testing across both PyIceberg lines |
| Verified | Every row below was probed against **0.11.1** and against **0.12.0** (`../iceberg-python` at `0bf4d13d`) on 2026-08-21 |

---

## 1. Why there is any of this

Zamboni's job is to rewrite data files and commit the result as an Iceberg
`replace` snapshot. PyIceberg's public surface cannot express that:

- `UpdateSnapshot.overwrite()` hardcodes `Operation.OVERWRITE`, and
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

Five private imports, three private base classes subclassed, four private methods
overridden, and eight private instance attributes read. That is the whole
surface; it is small, and it is concentrated.

### 2.1 Imported symbols

| Symbol | Imported by | What public API cannot do |
|---|---|---|
| `_OverwriteFiles` | `committer.py` | Base for `_ReplaceFiles`. Nothing public emits a `replace` snapshot |
| `_SnapshotProducer` | `evolution.py`, `capabilities.py` | Needs the `operation` constructor argument and the `_manifests` hook |
| `_FastAppendFiles` | `testing.py` | Register **position delete files**, which PyIceberg cannot write at all. Test-only; never on a user's path |
| `_dataframe_to_data_files` | `backends/duckdb_arrow.py`, `capabilities.py` | Computes the partition key **from the data**, so `bucket` works. `add_files` infers it from statistics |
| `PyArrowFileIO._initialize_fs` | `orphans.py` | Returns the fsspec filesystem needed to **list** storage. `FileIO` exposes open/delete and no listing |

### 2.2 Subclassed base classes

`_ReplaceFiles` (in `committer.py`) is the hub: it subclasses `_OverwriteFiles`
and is itself the base for three more producers. One upstream rename of
`_OverwriteFiles` therefore reaches six modules.

```
_OverwriteFiles                     (pyiceberg.table.update.snapshot)
└── _ReplaceFiles                   committer.py     -- relabels the snapshot `replace`
    ├── MultiSpecReplaceFiles       evolution.py     -- one commit spanning two partition specs
    ├── _RewriteManifests           manifests.py     -- regroup manifests, touch no data
    └── _RemoveDeleteFiles          deletes.py       -- drop whole delete manifests
_FastAppendFiles
└── _AppendDeleteFiles              testing.py       -- test fixtures only
```

### 2.3 Overridden private methods

Each override exists because the inherited behaviour is wrong for the operation,
not because it is inconvenient.

| Override | In | Why |
|---|---|---|
| `_summary()` | `committer.py`, `evolution.py` | Let PyIceberg compute the totals as an overwrite, then relabel the finished summary `replace` |
| `_existing_manifests()` | `deletes.py`, `evolution.py` | Upstream's pruning double-counts rows; `_surviving_manifests` replaces it |
| `_deleted_entries()` | `evolution.py`, `deletes.py`, `manifests.py` | Upstream filters entries to `DataFileContent.DATA`, so a delete file passed to `delete_data_file` is silently ignored |
| `_manifests()` | `evolution.py`, `manifests.py`, `testing.py` | `_summary` hardcodes `table_metadata.spec()`, the table default, while grouping needs per-manifest specs |
| `_meta` | `testing.py` | Label a manifest `content: deletes`, which `ManifestWriterV2` will not do |

### 2.4 Inherited private attributes

Read by our subclasses, assigned by PyIceberg. **This is the least visible part
of the surface** — an import that disappears is an `ImportError` on the next run,
whereas an attribute that is renamed is an `AttributeError` in the middle of a
commit.

`_transaction` (19 uses) · `_snapshot_id` (7) · `_io` (6) ·
`_parent_snapshot_id` (5) · `_deleted_data_files` (4) · `_added_data_files` (3) ·
`_target_branch` (2) · `_operation` (set by us, read by upstream)

### 2.5 Other private reaches

| Call | In | Why |
|---|---|---|
| `Transaction._apply(...)` | `evolution.py:422` | Apply an `AddPartitionSpecUpdate` plus its `AssertTableUUID` in one commit |
| `hasattr(io, "_initialize_fs")` | `orphans.py:216` | Guarded — a non-PyArrow `FileIO` falls back rather than crashing |

`Table._do_commit` and `ExpireSnapshots._commit` appear in **prose only**
(`properties.py`, `expire.py` docstrings) to explain upstream behaviour. They are
not called, and are listed here so a future audit does not count them twice.

---

## 3. What this surface actually did between 0.11.1 and 0.12.0

Measured, not assumed — this is the interesting result, and it cuts against the
intuition that private APIs churn constantly.

**Every symbol above survived 0.11.1 → 0.12.0 unchanged, signatures included.**
Across 397 upstream commits, the inventory in §2 needed no edit. What did change
was subtler and worse:

| Change | Detected by | Consequence |
|---|---|---|
| `_build_delete_files_partition_predicate` → `..._filters` | The name-based probe going False | Zamboni refused to run on the build that *fixed* a corruption bug |
| **Behaviour** of manifest pruning changed with **no name change** | A behavioural probe | The name-based probe declared a data-corrupting build safe |
| `pyiceberg-core` moved out of the `[pyarrow]` extra | `test_the_rust_core_is_installed_however_it_gets_here` | Every transformed-partition write raises `NotInstalledError` |

Two lessons follow, and they are the whole basis of §4:

1. **Rename risk is real but loud.** An `ImportError` or `AttributeError` stops
   the run. It is the failure mode you want.
2. **Behaviour risk is quiet, and names cannot detect it.**
   `_build_delete_files_partition_predicate` exists on 0.12.0rc1, *which corrupts
   data*, and on both attempted fixes for it. Any probe that asks "does this
   symbol exist" declares the corrupting build safe.
3. **The packaging around the private API moves independently of it.** Nothing in
   §2 changed, and 0.12 still could not write a partitioned table.

---

## 4. How the risk is mitigated

Six mechanisms, in the order they engage.

### 4.1 A single choke point, not a scatter

`assert_supported_pyiceberg()` runs before any commit path and raises
`UnsupportedPyIceberg` if the installed build fails the checks in
`capabilities.py`. Every mutating operation passes through it, so "is this build
safe" is asked once, in one place, and answered the same way for every verb.

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
what survived**. It costs ~150ms warm, once per process, and only on builds that
prune at all — which is no current release. This is the single most important
mitigation on the list, because it is the only one that catches a behaviour
change that kept its name.

Its limitation, worth knowing: it returns `None` — "could not establish" — for
*any* failure, including a missing optional dependency. On 0.12 without
`pyiceberg-core`, `zamboni doctor` said "prunes incorrectly" when the truth was
"could not tell". Safe, and a misleading diagnosis.

### 4.4 Tripwires on the things a probe would not think to ask

`test_the_rust_core_is_installed_however_it_gets_here` guards a *packaging* fact
rather than an API. Its predecessor's docstring promised it would "fail if
upstream moves the dependency out of `[pyarrow]`, rather than leaving a user to
discover it by writing a table" — and that is exactly how the 0.12 requirement
was found. Tripwires like this are cheap and belong wherever an upstream
*convention* is load-bearing.

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

## 6. If you are about to change any of this

1. Read the comment above the probe or override before editing it. Several
   explain a specific bug that a "cleanup" reintroduces —
   `_guard_anywhere_in_scan_planning` searches a whole module on purpose, and
   `_surviving_manifests` replaces upstream's pruning on purpose.
2. Exercise both lines. `uv pip install -e ../iceberg-python` then
   `.venv/bin/python -m pytest` — via the venv interpreter, because `uv run`
   re-syncs from `uv.lock` and undoes the install you just made.
3. If you add a private reach, add its row to §2 and a probe or tripwire for it.
   An unlisted reach is the one that breaks a release.
