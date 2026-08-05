# PyIceberg 0.12: `upsert` duplicates rows on a partitioned table

**Status: blocks ZMBNI-11.** Found by ZMBNI-1103 while auditing what unreleased
PyIceberg changes. This is an upstream defect, not ours, and the reproduction
below uses no Zamboni code.

**Severity: silent data corruption.** No error is raised. The table simply ends
up with rows that should have been replaced, plus rows duplicated outright, and
a later `upsert` fails on the duplicates the earlier one created.

---

## Reproduction

Twenty-five lines, PyIceberg and PyArrow only. Append two rows to a partitioned
table, upsert one of them, read it back.

```python
import tempfile, pathlib, datetime as dt
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform
from pyiceberg.types import NestedField, StringType, IntegerType, TimestampType

wh = pathlib.Path(tempfile.mkdtemp())
cat = SqlCatalog("c", uri=f"sqlite:///{wh}/c.db", warehouse=f"file://{wh}")
cat.create_namespace("db")
schema = Schema(
    NestedField(1, "k", StringType(), required=False),
    NestedField(2, "v", IntegerType(), required=False),
    NestedField(3, "ts", TimestampType(), required=False),
)
spec = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="ts_day")
)
t = cat.create_table("db.t", schema=schema, partition_spec=spec,
                     properties={"format-version": "2"})

arrow = pa.schema([pa.field("k", pa.string()), pa.field("v", pa.int32()),
                   pa.field("ts", pa.timestamp("us"))])
base = dt.datetime(2026, 1, 6, 12)
def rows(pairs):
    return pa.table({"k": [p[0] for p in pairs],
                     "v": pa.array([p[1] for p in pairs], type=pa.int32()),
                     "ts": [base] * len(pairs)}, schema=arrow)

t.append(rows([("a", 1), ("b", 1)]))
t.refresh()
t.upsert(rows([("a", 2)]), join_cols=["k"])
t.refresh()

arr = t.scan().to_arrow()
print(sorted(zip(arr["k"].to_pylist(), arr["v"].to_pylist())))
```

| PyIceberg | Output | Data files |
|---|---|---|
| **0.11.1** (released) | `[('a', 2), ('b', 1)]` — correct | 2 |
| **main @ `154288fb`** | `[('a', 1), ('a', 2), ('b', 1), ('b', 1)]` | 3 |

Two defects in one result: the superseded `('a', 1)` survives beside its
replacement, and `('b', 1)` — a row the upsert never targeted — is duplicated.

**The partition spec is required to reproduce.** The same script on an
unpartitioned table is correct on both versions.

---

## Mechanism

`upsert` delegates to the overwrite path, and from 0.12
`_OverwriteFiles._existing_manifests` prunes manifests with a partition
predicate:

```python
if not manifest_evaluators[manifest_file.partition_spec_id](manifest_file):
    existing_files.append(manifest_file)      # kept verbatim
    continue
```

A manifest the evaluator does not match is appended **whole**, including the
entries the operation is deleting. The predicate here is built from the upsert's
join column `k`, which is not a partition field, so it cannot select the
manifest holding the rows being replaced. That manifest survives intact; the
rewritten file and the new row are added alongside it; the reader sees both.

The knock-on effect is what surfaces first in practice. The next `upsert`
against the same table calls `upsert_util.get_rows_to_update`, which now finds
the duplicates the previous one created and raises:

```
ValueError: Target table has duplicate rows, aborting upsert
```

That guard is new in 0.12 and it is *correct*. It is detecting real corruption —
just corruption that 0.12 itself introduced one step earlier.

---

## Why this matters beyond the demo

Zamboni hit the same underlying mechanism in its own commit path. Partition
evolution turned `[3, 3, 4, 4]` into `[3, 3, 3, 3, 4, 4, 4, 4]` for exactly this
reason: `MultiSpecReplaceFiles` overrides `_manifests()` and inherited the
pruning `_existing_manifests()`. Fixed on our side by
`_surviving_manifests()`, which matches entries directly instead of relying on a
predicate — see `src/zamboni/evolution.py`.

We could work around it there because we know precisely which entries we are
replacing. `upsert` has the same information and does not use it.

**Our own maintenance operations are safe on 0.12 after that fix** — the full
suite passes apart from the demo, whose ingest calls `upsert`. What is not safe
is any *write* path that goes through overwrite on a partitioned table, which is
most merge-style ingestion.

---

## Verification record (ZMBNI-1105)

Against `../iceberg-python` @ `154288fb`, with the dev stack live:

| | |
|---|---|
| Full suite | **418 passed**, 7 failed, 5 errors |
| Non-demo failures | **0** — every failure is the demo's ingest |
| Dev stack (real Lakekeeper + MinIO) | 19 of 20 pass; the one failure is the demo |

So **every maintenance operation works on 0.12**, including against a live
catalog and object store: compaction, ordering, partition evolution, expiry,
orphan removal, dangling-delete removal, manifest rewriting, metadata retention.
What does not work is *ingest*, and only because it upserts.

The two blockers we were waiting on were re-confirmed rather than assumed:

| | On `main` | |
|---|---|---|
| `delete_manifests_writable` | False | `ManifestWriterV2.content()` still returns `DATA`; no `ManifestWriterV3` |
| `equality_deletes_readable` | False | the guard is still present |

ZMBNI-604 and 704–706 stay blocked. 0.12 lifts neither.

---

## Consequences for ZMBNI-11

0.12 cannot be adopted for a deployment that upserts into partitioned tables
until this is fixed upstream, regardless of what Zamboni does — the corruption
happens in the ingest job, not in maintenance.

The capability probes do not and should not catch this. They answer "can this
build do X", and this build *can* upsert; it simply does it wrongly. A probe for
correctness would have to write data and read it back, which is a test, not a
probe. Recorded here so the distinction is deliberate rather than an oversight.
