# The `write.completed` contract

**Contract version 1.0.** What an Iceberg writer emits when it has finished with a
table, so maintenance can follow the write rather than wait for its schedule.
Zamboni is the consumer and defines the contract; any writer may produce it.

The JSON Schema is generated from the code and published with the package:

```python
from zamboni import get_write_completed_spec
spec = get_write_completed_spec()      # a dict; spec["x-contract-version"] == "1.0"
```

Why the contract has this shape — the decisions and the alternatives rejected —
is in [maintenance-runtime.md §5](maintenance-runtime.md).

### Getting the schema without installing Zamboni

A producer that may not depend on this package fetches the same file. **Its path
is promised:** `src/zamboni/zamboni-write-completed_spec.json`, and moving or
renaming it is a breaking change, recorded as one in the changelog. Two places
serve it, identical at a given version:

- **The repository, at a tag:**
  `https://raw.githubusercontent.com/paulcaron16k/Zamboni/v0.6.0/src/zamboni/zamboni-write-completed_spec.json`
- **The GitHub release, as an asset:**
  `https://github.com/paulcaron16k/Zamboni/releases/download/v0.6.0/zamboni-write-completed_spec.json`

Substitute the release you build against. 0.6.0 is the first that carries the
contract; before it is tagged, use the repository URL at a commit on `main`.
Vendor the file at a pinned version and refresh it with a script, so a contract
change reaches your repository as a reviewed diff, never silently.

---

## When to emit

**Once per table, when the writer has finished with it for this run** — after
its last commit to that table succeeded. *Completed*, not *committed*: a catalog's
per-commit events fire during ingestion, which is exactly when maintenance must
not run.

- A run that writes a table in many batches, through several internal sinks or
  from several source streams emits **one** event for that table.
- A run that committed nothing to a table emits nothing for it.
- A run that failed or was interrupted emits nothing. "Completed" means
  completed; the consumer's schedule covers a run that was not.

**The event is advisory.** Emitting must never fail or slow the write: a broker
that is unreachable or misconfigured costs one logged warning, within a bounded
time, and the run succeeds. A deployment with no broker configured must not
touch one.

---

## The event

A CloudEvents 1.0 event in **structured-mode JSON** — the envelope and wire shape
Lakekeeper uses for its own events, so one subscriber stack reads both. That is
checked against Lakekeeper's source and live events, with a captured one kept as
a test fixture: [event-driven-maintenance.md §5](event-driven-maintenance.md).
Sharing the envelope is not sharing the type — a consumer of this contract
refuses Lakekeeper's `updateTable` rather than mistaking it for a write.

```json
{
  "specversion": "1.0",
  "type": "writeCompleted",
  "source": "uri:target-iceberg:loader-7f9c",
  "id": "acme/raw/events@4126931178312874903",
  "dataschema": "https://github.com/paulcaron16k/Zamboni/blob/main/docs/write-completed.md/1.0",
  "datacontenttype": "application/json",
  "data": {
    "warehouse": "acme",
    "namespace": ["raw"],
    "table": "events",
    "snapshot_id": 4126931178312874903,
    "completed_at": "2026-10-08T02:14:09Z",
    "started_at": "2026-10-08T02:11:52Z",
    "writer": "target-iceberg/0.3.0",
    "load_method": "append-only",
    "full_refresh": false,
    "rows": 120000,
    "data_files_added": 12,
    "bytes_added": 48211934,
    "snapshot_count": 12,
    "streams": ["events"],
    "table_uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94"
  }
}
```

### Envelope

| Attribute | Required | Value |
|---|---|---|
| `specversion` | yes | `"1.0"` |
| `type` | yes | `"writeCompleted"` |
| `source` | yes | `uri:<producer>:<host>` |
| `id` | yes | **deterministic**: `<warehouse>/<namespace levels joined by />/<table>@<snapshot_id>`. A redelivered or re-emitted event then carries the same id, which is what lets it be recognised as a duplicate |
| `dataschema` | recommended | this document's URL plus `/<major>.<minor>` — the version written against |
| `datacontenttype` | optional | `"application/json"` |
| `data` | yes | the payload below |

### Payload

The first five fields are **required** and identify the table and the write. The
rest are optional and describe the write; **none is ever used to decide
anything** — the consumer decides against the table itself.

| Field | Required | Meaning |
|---|---|---|
| `warehouse` | yes | the warehouse's name as the catalog knows it — **the matching key** |
| `namespace` | yes | a **list** of levels, outermost first; never a dotted string |
| `table` | yes | the table's name |
| `snapshot_id` | yes | the last snapshot this writer committed to the table in this run |
| `completed_at` | yes | end of the write window, ISO-8601 with a UTC offset |
| `started_at` | no | start of the write window |
| `writer` | no | producer and version, e.g. `target-iceberg/0.3.0` |
| `load_method` | no | `append-only`, `upsert` or `overwrite` |
| `full_refresh` | no | true when the table's contents were replaced wholesale |
| `rows` | no | rows committed in this run |
| `data_files_added` | no | data files the run added |
| `bytes_added` | no | bytes of data files the run added |
| `partitions_changed` | no | partitions changed — **omit rather than estimate**; summing per-snapshot counts over-counts |
| `snapshot_count` | no | snapshots committed in this run |
| `streams` | no | the source streams that wrote the table |
| `table_uuid` | no | the table's `table-uuid` (required by the Iceberg spec from format v2); informational |
| `warehouse_id` | no | the catalog's own warehouse identifier, where it has one; informational — not part of the Iceberg REST specification |
| `catalog_uri` | no | the catalog URI as the writer reached it; informational — the same catalog is reached by different URLs from different places |

Bounded: no record samples, no configuration, **never a credential**.

---

## How a consumer reads it

- **Matching is by warehouse name, exactly**, then `namespace` + `table` against
  that warehouse's configuration. An event for a warehouse or table the consumer
  does not manage is dropped and counted, never an error.
- **Duplicates** are recognised by `(warehouse, table, snapshot_id)`, which is
  why `id` is deterministic.
- **Unknown payload fields are ignored.** That is what makes a minor version safe
  to deploy producer-first.
- A malformed event is rejected once and not redelivered.

---

## Delivery

**Over NATS, on one configurable subject** with no hierarchy, as Lakekeeper does;
the CloudEvents `type` distinguishes events. Environments sharing a broker use
separate subjects (a per-deployment prefix), not payload fields.

- **On JetStream**, publish with the `Nats-Msg-Id` header equal to the event's
  `id`, so the server deduplicates within its window. Delivery is then
  at-least-once.
- **On core NATS**, delivery is at-most-once: a consumer that is not connected
  when the event is published never sees it, so every restart of a core-NATS
  consumer loses what was published meanwhile. The consumer's schedule is the
  backstop for anything lost.

Neither is a correctness requirement: the event is a hint, and a consumer that
receives none still maintains every table on its schedule.

---

## Versioning

`major.minor`. A **minor** increment only adds optional payload fields; a
consumer of 1.0 accepts a 1.1 event and ignores what it does not know. Anything
else — a removed or renamed field, a changed meaning, a new required field — is
a new **major**, and a consumer refuses a major it does not read.

**For a producer:** publish the JSON Schema of what you emit, and test it against
`get_write_completed_spec()` — every field you emit is in the contract with the
same type, and every required field is emitted. That test is what catches drift
in your repository instead of in production.
