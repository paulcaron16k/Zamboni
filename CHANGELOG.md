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

Everything below is the initial implementation. It has no released predecessor,
so nothing here is described as a change *from* anything.

`0.1.0` in `pyproject.toml` is the in-development version and is **not tagged**:
this repository has no remote, so CI has never executed. See
[docs/releasing.md §4](docs/releasing.md). The first tag follows ZMBNI-905.

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
