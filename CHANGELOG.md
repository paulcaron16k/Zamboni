# Changelog

Notable changes to Zamboni. Format follows [Keep a Changelog][kac]; versioning
follows [semantic versioning][semver] with the 0.x caveat and the tool-specific
definition of "breaking" set out in [docs/releasing.md](docs/releasing.md).

Two categories beyond the usual set, because this tool deletes files:

- **BREAKING** — a change to a verb, a flag, an exit code, a config key, or **a
  default that decides what gets deleted**. The last of these can change what a
  nightly run removes with no signature moved, so it is called out here or
  nowhere.
- **SAFETY** — an operation newly refused because it was found to be unsafe.
  Ships in a patch release even though it can break a working pipeline; the
  entry names the operation and why.

[kac]: https://keepachangelog.com/en/1.1.0/
[semver]: https://semver.org/spec/v2.0.0.html

---

## [Unreleased]

### Added

- **`zamboni maintenance`** — one command that runs the six operations in the
  runbook order over every configured table, exiting with the worst code any of
  them produced. This is the DevOps entry point: with `./zamboni.yml` and
  `./.env` present, a cron line is a `cd` and a command. `--status` reports file
  counts and bytes before and after.
- **`--profile` and `--env`** — non-secret configuration in `zamboni.yml`,
  credentials in `.env`, both discovered in the working directory. Templates are
  committed as `zamboni.yml.sample` and `env.sample`. Unknown profile keys are
  refused rather than ignored.
- **`zamboni warehouses`** — the catalog's warehouses, one per line, as input to
  a crontab generator. Zamboni does not schedule anything, and
  [docs/devops.md](docs/devops.md) explains why that boundary is deliberate.
- **[docs/devops.md](docs/devops.md)** — the cron line, the multi-tenant layout
  (`$ZAMBONI_ROOT/configs/{warehouse}/table-config.json`), and why per-warehouse
  invocation beats one loop over the fleet.
- **A maintainer interface, and `--engine`.** The six operations are Iceberg's,
  not Zamboni's; Trino and Spark implement most of them already. `LocalMaintainer`
  is the PyIceberg engine, extracted with no behaviour change. `TrinoMaintainer`
  and `SparkMaintainer` are not implemented, but their declared capabilities are
  real — `zamboni engines` reports what each would refuse, and why, before either
  is written.
- **`zamboni engines`** — per-engine, per-operation support, previewability and
  limitations.
- **`--engine trino`** — five of the six operations, over `ALTER TABLE … EXECUTE`.
  Needs the optional `zamboni[trino]` extra. Configure with `--trino-host`,
  `--trino-port`, `--trino-user`, `--trino-catalog` and `--trino-version`, or the
  matching `ZAMBONI_TRINO_*` variables. `remove-dangling-deletes` is refused:
  Trino has no equivalent.

  Two things translate rather than pass through, both found by running against a
  real Trino rather than by reading its documentation. Metadata retention uses
  Trino's `max_previous_versions` and `delete_after_commit_enabled`, because the
  Iceberg property names are refused outright — even through `extra_properties`.
  And `retain_last` (our `min_snapshots_to_keep`) only exists from **Trino 479**,
  so it is gated on `--trino-version` and its loss is reported rather than
  silent.

### SAFETY

- **Orphan removal now refuses when another table shares this table's location.**
  Previously it deleted that table's files. `0.1.0` scoped the sweep to the
  table's own roots, which prevents a warehouse-wide sweep but does not prevent a
  second table living *inside* those roots — its files are then unreferenced here
  and live there, and were deleted once past the age guard.

  **This needs no misconfiguration.** A default location is derived from the table
  name *at creation time*; `rename_table` rewrites the catalog entry and moves no
  files; creating the freed name derives the same location again. In the
  reproduction, renaming `db.orders` to `db.orders_v2` and re-creating
  `db.orders` left two live tables in one directory, and maintaining the new one
  deleted all nine files of the old one, including its current metadata — the
  table became unreadable.

  **If you have run `remove-orphans` on a warehouse where any two tables share or
  nest locations, check those tables before upgrading is any comfort:** the data
  is already gone, and `expire` is unaffected (it never lists storage). Catalogs
  that derive locations from a table UUID rather than its name — Lakekeeper does —
  cannot produce the rename collision, though an explicit `location` or a
  `write.data.path` pointing into another table still can.

  The check costs one metadata read per table in the catalog and runs before the
  listing. A catalog that cannot be enumerated now aborts the run rather than
  proceeding blind. ZMBNI-507, design.md §6.6 invariant 4, FR-7.18.

## [0.1.0] - 2026-08-03

The initial implementation. It has no released predecessor, so nothing here is
described as a change *from* anything.

**Why 0.1.0 and not 1.0.0.** The scope is delivered and every operation has been
verified against a live Lakekeeper and MinIO, so a 1.0 would have been defensible.
It is deliberately not that, for one reason and two specifics. The reason is
asymmetry: 0.x costs nothing and can be followed by 1.0.0 at any time, whereas a
promise cannot be withdrawn — and the first genuine correction to a destructive
default would then have to be either 2.0.0 or a quiet slip into a minor release,
which would make [docs/releasing.md](docs/releasing.md) untrue. The specifics are
that CI has never executed (this repository has no remote — ZMBNI-905), so the
honest claim is "every check passes on one machine, one Python, one live
Lakekeeper"; and that the `table-config.json` schema and the defaults deciding
what gets deleted have had exactly one author and one five-day dataset. 1.0.0
follows a green CI run and a maintenance cycle against a warehouse we did not
build.

### Added

**Compaction.** Rewrites small data files into target-sized ones per
`(spec_id, partition)`, committing the whole run in a single `replace` snapshot —
matching Iceberg's own `partial-progress.enabled=false` default. `--partial-progress`
commits per group instead. Peak memory is approximately one output file
regardless of table size, via DuckDB spill.

**Merge-on-read correctness.** Preserves live rows rather than physical rows, so
compaction never resurrects a row a position delete had removed.

**Ordering.** The table's declared sort order, and multi-key Z-order via Morton
encoding expressed as DuckDB SQL. `sort_order_id` is stamped only when the output
genuinely satisfies the order.

**Partition evolution.** Condenses aged fine-grained partitions — days into
months — by adding a coarse spec without making it the default, so new data keeps
landing daily. Compound specs evolve when exactly one field matches the rule's
granularity; two fields of the same granularity are refused, because
`older_than_days` measures from a window end and two fields give two answers.

**Storage reclamation.** Snapshot expiry implementing the spec's five-step
retention algorithm, including `max-ref-age-ms`, then deleting the file-set
difference — PyIceberg's `expire_snapshots()` implements almost none of the
algorithm and deletes no files. Orphan-file removal with a 3-day mtime guard,
list-before-reachable ordering, and invariants that abort rather than delete when
its own view of storage looks untrustworthy.

**Metadata hygiene.** Dangling-delete removal, manifest regrouping by partition
so predicates prune, and `metadata.json` retention via the Iceberg
`write.metadata.*` properties.

**Configuration.** `table-config.json` covering partitioning, evolution,
ordering, sizing and retention, with unknown keys rejected and contradictions
refused at load. `from-catalog` derives a starting config from a Meltano/Singer
catalog.

**CLI.** Eleven verbs. Five never touch a table; the six that mutate follow one
rule with no exceptions — **without `--yes`, nothing is committed** — and each
says on stdout that it is previewing. `--version` reports zamboni, PyIceberg and
Python, because the first alone does not identify behaviour.

**Dev stack.** Lakekeeper, Lakekeeper UI, Postgres and MinIO on shifted ports, so
it coexists with an existing deployment. Every operation has been verified
against it and against a live Lakekeeper 0.13.1 + MinIO — see
[docs/live-verification.md](docs/live-verification.md).

**Operator runbook.** [docs/runbook.md](docs/runbook.md): the six-verb order and
why each position matters, cadence derived from the retention windows, how to
size the orphan guard by measurement, and recovery per failure mode.

### Refused by design

Not limitations discovered late — each is a deliberate refusal with a stated
reason, and each exits non-zero rather than doing something surprising:

- **Format version 1** is blocked.
- **Format version 3** is blocked for row rewriting, because row lineage
  (`_row_id`, `_last_updated_sequence_number`) cannot survive a scan-and-rewrite.
  Metadata-only operations stay allowed.
- **Equality deletes** are blocked where the installed PyIceberg cannot read them.
- **Unsafe PyIceberg builds** are refused by structural capability probes rather
  than version comparison.
