# `table-config.json` — specification (version 2)

The declarative layout target for each Iceberg table. Data Engineers and Analysts author
it; the compactor reads it and works out what rewriting that implies. Nothing in it names
an action.

Two ways to author it:

1. **Directly**, as a file in the pipeline repo.
2. **In the Meltano/Singer catalog**, under an `x-iceberg` key per stream, and generated
   with `zamboni from-catalog`. See [Authoring in Meltano](#authoring-in-meltano) — and
   the warning there about why it is *generated* rather than read at runtime.

---

## The model: warehouse → namespace → table

One file describes **one warehouse**, and it says which:

| Iceberg | Postgres / Snowflake | Where it lives |
|---|---|---|
| warehouse (catalog) | **database** | `warehouse` at the root of this file |
| namespace | **schema** | a key under `namespaces` |
| table | table | a key under that namespace's `tables` |

That is the shape every data engineer already has, and version 2 exists to make it
explicit. Note that Snowflake's *warehouse* means compute, not a container — the thing
Iceberg calls a warehouse is Snowflake's **database**.

```json
{
  "version": 2,
  "warehouse": "acme",
  "defaults": {
    "partition_evolution": {
      "enabled": true,
      "rules": [{ "from": "day", "to": "month", "older_than_days": 90 }]
    }
  },
  "namespaces": {
    "analytics": {
      "tables": {
        "events": {
          "description": "Clickstream fact table",
          "partition": [
            { "column": "occurred_at", "transform": "day" },
            { "column": "tenant_id", "transform": "bucket", "num_buckets": 16 }
          ],
          "ordering": {
            "mode": "zorder",
            "zorder": { "columns": ["customer_id", "product_id"], "precision_bits": 16 }
          },
          "target_file_size_bytes": 268435456,
          "min_input_files": 4
        },
        "audit_log": {
          "partition": [{ "column": "occurred_at", "transform": "day" }],
          "partition_evolution": { "enabled": false }
        }
      }
    }
  }
}
```

### `warehouse` is required, and it asserts rather than selects

It does not choose which warehouse to maintain — `--warehouse`/`--db`, the profile, or the
per-customer directory does that. It is checked against whichever was chosen, and a
mismatch stops the run:

```console
zamboni: error: configs/globex/table-config.json declares warehouse 'acme', but this run
is maintaining 'globex'. One of the two is wrong; the file is the one that travels
between directories.
```

That is the mistake the multi-tenant layout invites: copy `acme`'s config into `globex`'s
directory, forget to edit one line, and maintain the wrong tenant's tables all night. It
is required rather than optional because an optional assertion is missing from exactly the
files nobody thought carefully about.

### Table names cannot contain a dot

A dot separates namespace levels, so one in a table name would be ambiguous — and that
ambiguity is the reason this format has a version 2.

Version 1 keyed tables by a dotted string, `"analytics.events"`, and the split had to be
*guessed*. It was guessed with `rpartition`, the last dot winning. For a one-level
namespace that is always right. For `raw.telemetry.events` it produced:

| | resolved as |
|---|---|
| local engine (PyIceberg) | namespace `('raw','telemetry')`, table `events` |
| Trino, Spark | a single namespace literally named `raw.telemetry` |

The same key, two different tables, no error anywhere. Stating the namespace and forbidding
dots in the table name removes the guess rather than papering over it.

### Nested namespaces work, and are still worth avoiding

A dot inside a `namespace` key means nesting, unambiguously — the dots are in a field whose
meaning is "namespace", so splitting them is safe. `"raw.telemetry"` is the namespace
`raw` → `telemetry`.

They are supported and discouraged, because **each engine spells them differently and
rejects the other's spelling.** Verified against a live Trino 483 and Spark 4.0.4 with a
genuinely nested namespace:

| Form | Trino | Spark |
|---|---|---|
| `"ice"."raw.telemetry"."events"` | reads | `Namespace parts cannot contain '.'` |
| `` `ice`.`raw`.`telemetry`.`events` `` | `Too many dots in table name` | reads |

Zamboni emits the right form for each engine, so this is handled rather than left to you.
But a layout that needs two incompatible spellings does not map onto
`database.schema.table`, and `zamboni table-config summary` will point it out. Prefer one
level where you can.

---

## Inheritance

`defaults` applies to every table. A table's own block overrides it **per section**: a
table that declares `ordering` replaces the default ordering entirely rather than having
individual keys blended, so what you read in one block is what applies.

There is deliberately no namespace-level `defaults` yet. `namespaces.<name>` is an object
rather than a bare table map precisely so one can be added without another format change.

**Unknown keys are rejected.** A typo like `"partiton"` would otherwise silently leave a
table unpartitioned — exactly the class of mistake this file exists to prevent.

---

## `partition`

A list of Iceberg partition fields, in order.

| Key | Required | Notes |
|---|---|---|
| `column` | yes | Source column name |
| `transform` | no (default `identity`) | `identity`, `year`, `month`, `day`, `hour`, `bucket`, `truncate`, `void` |
| `num_buckets` | with `bucket` | Rejected on any other transform |
| `width` | with `truncate` | Rejected on any other transform |
| `name` | no | Override the derived partition field name |

Bucket partitioning is fully supported on rewrite. (PyIceberg's `add_files` path cannot do
it — it infers partition values from column statistics and refuses non-order-preserving
transforms — so this package writes through `_dataframe_to_data_files`, which derives the
partition key from the data.)

---

## `partition_evolution`

Condenses aged fine-grained partitions into coarser ones. The motivating case: a table
partitioned by `day` where last year's months are hundreds of tiny files no query prunes
into.

```json
"partition_evolution": {
  "enabled": true,
  "rules": [{ "from": "day", "to": "month", "older_than_days": 90 }]
}
```

**The default is days-to-months, enabled, at 90 days.** Turn it off fleet-wide by setting
`defaults.partition_evolution.enabled` to `false`, or per table in that table's block.
`zamboni from-catalog --no-evolution` generates a config with it off.

- `from` / `to` must be time granularities (`hour` < `day` < `month` < `year`) and `to`
  must be **coarser**. Compaction merges files; it never splits them, so a
  month→day rule is rejected at load.
- `older_than_days` is measured from the **end** of the partition window. A rule at 90 days
  leaves a partition alone until every row it could contain is at least 90 days old, so
  late-arriving rows do not get rewritten out from under.
- Only one rule per `from` granularity — a field can age one way.
- A rule naming a granularity the table does not partition by is an error, not a no-op.

### How it works, and what it costs

New data keeps landing in the fine-grained spec. Iceberg lets a table carry several
partition specs with each file recording its own, and PyIceberg exposes
`AddPartitionSpecUpdate` separately from `SetDefaultSpecUpdate` — so the month spec is
added **without** becoming the default.

One caveat worth knowing: PyIceberg's stock snapshot producer hardcodes the added manifest
to the table's *default* spec while grouping deleted entries by each file's own spec. Left
alone it writes month-partitioned files into a day-spec manifest — metadata that reads fine
until a predicate tries to prune on it. `zamboni` overrides that (`MultiSpecReplaceFiles`)
and tests assert manifest/file spec agreement on every evolved table.

**Compound specs are supported** when exactly one field matches the rule's `from`
granularity. That field coarsens; the others are copied through untouched, keeping their
partition field ids because they still mean what they meant. Only one new spec is needed —
the combinations of the other fields are partition *values*, which every file carries
individually under one shared spec.

Grouping then keys on the whole output partition tuple, so `[ts:day, region:identity]` with
two regions in one month produces two groups and two output files, not one merged across
regions.

A spec where **two or more fields share the rule's granularity** is still skipped, and the
reason names them: `older_than_days` is measured from a partition window's end, so two `day`
fields give two answers about when the data ages, and picking one would be a guess about
which column dates the row.

---

## `retention`

How much history a table keeps, and whether unreferenced files are deleted. Compaction
frees no storage on its own — old snapshots still reference the files it supersedes — so
these are what actually reclaim disk.

```json
"retention": {
  "expire_snapshots":        { "enabled": true, "max_snapshot_age_days": 5,
                               "min_snapshots_to_keep": 1, "max_ref_age_days": null },
  "remove_orphan_files":     { "enabled": true, "older_than_days": 3 },
  "remove_dangling_deletes": { "enabled": true }
}
```

Every `expire_snapshots` value except `enabled` may be omitted, in which case the Iceberg
table property (`history.expire.max-snapshot-age-ms`, `history.expire.min-snapshots-to-keep`)
applies, then the spec default. A table already tuned through its properties is not
silently overridden just because this file exists.

### `expire_snapshots`

Applies the Iceberg spec's retention algorithm: every branch and tag head is kept, each
branch's ancestors are kept until a snapshot is *both* older than `max_snapshot_age_days`
*and* beyond `min_snapshots_to_keep`, and `main` never expires. Files that only the expired
snapshots referenced are then deleted — computed as a set difference, so a file that was
never referenced cannot be touched.

> PyIceberg's own `expire_snapshots()` implements almost none of this: it protects ref
> heads and otherwise expires anything older than a timestamp, ignoring
> `min-snapshots-to-keep`, branch ancestry and `max-ref-age-ms`. It also deletes no files.

`max_ref_age_days` **drops the ref**, which is what lets the snapshots it was pinning
expire. Per the spec, a ref's own `max-ref-age-ms` wins over the table property, and `main`
is exempt — "The main branch never expires."

It is off unless asked for: the default is `null`, meaning "not configured", so no tag or
branch is ever removed without a table property or this setting saying so. That matters
because dropping a named ref destroys metadata a person chose to create; unlike expiring an
anonymous snapshot, nobody can tell afterwards what it was called.

There is deliberately **no `--max-ref-age-days` CLI flag**, unlike the other two retention
knobs. `expire` accepts one-off overrides for snapshot age and count because both act on
anonymous snapshots. Deleting a tag someone named should be a reviewable change to this file,
not a value typed at a prompt.

### `remove_orphan_files`

Deletes files under the table's location that nothing references: writes that were made and
then abandoned (a failed commit, or `create_table_if_not_exists` on a table that already
exists — it writes the metadata file before trying the insert), and metadata versions
dropped from the log by `previous_versions_max`.

**`older_than_days` must exceed the longest single write the warehouse performs.** A file
that has been written but not yet committed is indistinguishable from an orphan, so the
guard is what stops one being deleted mid-write. Iceberg's default, and this one, is 3 days.
Note that "longest write" includes *compaction*, not just ingest.

The guard does **not** protect long-running readers — file mtime is the wrong clock for
that. Snapshot retention is the mechanism there.

Enabling `remove_orphan_files` without `expire_snapshots` is rejected: live snapshots hold
references to superseded files, so almost nothing would be reclaimable and the result would
be quietly disappointing rather than obviously wrong.

### `remove_dangling_deletes`

Drops delete files that no longer apply to any live data file — which is what compaction
leaves behind on a merge-on-read table, since the rewritten rows already have the deletes
applied and the new files carry a higher sequence number.

On by default, and unlike the other two it cannot change what a query returns: a dangling
delete is *by definition* one no scan would have applied. Whether a delete still applies is
decided by PyIceberg's own `DeleteFileIndex` — the index the scan planner builds — rather
than by a restatement of the rule here.

Metadata only. The delete files stay on disk, still referenced by older snapshots, exactly
as compaction's superseded data files do; expiry and orphan removal free the bytes.

> **Limited to whole manifests.** `ManifestWriterV2.content()` returns
> `ManifestContent.DATA` unconditionally and writes `content: data` into the avro metadata,
> so PyIceberg cannot write a delete manifest at all. A delete manifest can therefore be
> dropped whole or left alone, never rewritten — rewriting one would produce a manifest
> claiming to hold data files, and a reader would treat position deletes as rows. A dangling
> delete sharing a manifest with one that still applies is reported as retained, with the
> reason. This lifts automatically if PyIceberg gains a delete-manifest writer; the
> capability is probed, not assumed (`zamboni doctor`).

---

## `ordering`

How rows are arranged inside each output file. `mode` is a closed choice because sort and
z-order both define row order and only one can apply.

### `mode: "sort"`

```json
"ordering": {
  "mode": "sort",
  "sort": [
    { "column": "customer_id", "direction": "asc", "nulls": "last" },
    { "column": "occurred_at", "direction": "desc", "nulls": "first" }
  ]
}
```

Clusters the leading column tightly and progressively less thereafter. Output files are
left with `sort_order_id = null` unless the ordering *is* the table's declared sort order —
claiming otherwise would assert an ordering the data does not satisfy. (Use
`zamboni compact --sort-by-table-order` for the case where it does.)

### `mode: "zorder"`

```json
"ordering": {
  "mode": "zorder",
  "zorder": { "columns": ["customer_id", "product_id"], "precision_bits": 16 }
}
```

Clusters **every** key at once. A lexicographic sort on `(x, y)` groups `x` and scatters
`y`, so a query filtering only on `y` reads every file; a z-order prunes both.

Nothing in Iceberg, PyIceberg, or duckdb-iceberg provides z-order — there is no transform,
no metadata field, no function. It is built here from SQL: each key is dense-ranked (which
makes it work for strings, timestamps and decimals alike, and preserves locality where a
hash would destroy it), scaled to `precision_bits`, then bit-interleaved into a single
ordering key.

- At least 2 columns; use `mode: "sort"` for one.
- `precision_bits` × column count must fit in **62 bits** (a signed `BIGINT`). Four columns
  at 16 bits is fine; four at 32 is rejected at load rather than silently overflowing.
- More bits means finer locality per column but fewer columns fit.

Iceberg has no clustering concept distinct from sort order, so "clustering" here means
choosing `sort` or `zorder`.

---

## Sizing

| Key | Default | Notes |
|---|---|---|
| `target_file_size_bytes` | table property, else 128 MiB | Falls back to `write.parquet.target-file-size-bytes`, then `write.target-file-size-bytes` |
| `min_input_files` | 2 | A partition is only compacted with at least this many candidates |

---

## Authoring in Meltano

Put one `x-iceberg` object per stream in `meltano.yml`. Its contents are exactly a
`tables.<identifier>` block, so there is one schema to learn:

```yaml
extractors:
  - name: tap-postgres
    metadata:
      events:
        replication-method: INCREMENTAL
        x-iceberg:
          table: analytics.events        # optional; else derived from the stream
          partition:
            - {column: occurred_at, transform: day}
          ordering:
            mode: zorder
            zorder: {columns: [customer_id, product_id]}
```

Then generate:

```bash
zamboni from-catalog .meltano/catalog.json --namespace analytics -o table-config.json
zamboni validate-config table-config.json
zamboni compact analytics.events --table-config table-config.json --yes
```

The block is read from stream metadata (breadcrumb `[]`) or, failing that, from the
stream's JSON Schema root.

### Why generated, not read at runtime

**The catalog is a good authoring surface and a bad transport.**

- Meltano core manipulates the catalog as raw dicts, so `x-iceberg` keys authored in
  `meltano.yml` survive its metadata and schema rules intact.
- **The Singer SDK drops them.** `singer_sdk.singerlib.Metadata` and `Schema` are
  dataclasses whose `from_dict`/`to_dict` only round-trip their own declared fields.
  Verified against the SDK checkout: a catalog carrying `x-iceberg-partition` in the schema
  and `x-iceberg-sort-order` in stream metadata comes back with **both silently dropped**.
  Any SDK-based tap that reads `--catalog` and re-emits it loses them.

So `table-config.json` is the artifact that survives, and generating it is a pipeline step
with a reviewable diff. A property lost upstream shows up as a config change rather than as
a table quietly reverting to an unpartitioned layout.

Streams with no `x-iceberg` block are reported as skipped, not silently ignored — that is
what surfaces a misspelled key.
