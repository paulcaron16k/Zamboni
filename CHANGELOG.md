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

### Published

- **The repository is public: [github.com/paulcaron16k/Zamboni](https://github.com/paulcaron16k/Zamboni).**
  Created private, pushed, CI verified green, then flipped — nothing was visible
  until it was known to build.

- **CI has now run, and the first run was green.** All six jobs, including
  `dev-stack` (a real Lakekeeper, Postgres and MinIO plus the demo end to end)
  and `spark` (a built Spark Connect server with the Iceberg runtime). The CI
  badge is in the README for the first time, because now it reports a result
  rather than making a claim.

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
- **`--engine spark`** — all six operations over the Iceberg Spark procedures,
  including Z-order, which Trino cannot do. Needs the optional `zamboni[spark]`
  extra (a JVM and ~300MB). Verified against a live Spark 3.5.9 driving
  Lakekeeper and MinIO.

  Three things behave differently from Trino and are declared rather than
  smoothed over: `older_than` is a **typed literal timestamp computed on the
  client**, because a `CALL` argument cannot be an expression — so a fast clock
  expires more than intended; `remove_orphan_files` refuses any interval under
  24 hours, and refuses exactly 1 day too, since the timestamp is evaluated
  moments after it is computed; and it **lists with Hadoop FileSystem rather
  than Iceberg FileIO**, so it needs its own `spark.hadoop.fs.s3a.*`
  credentials even though every other operation runs on the catalog's vended
  ones.

  The timestamp carries an **explicit `+00:00` offset**. A bare wall-clock is
  read in `spark.sql.session.timeZone`, not UTC — measured against a live
  session in `America/New_York`, every expiry cut four hours deeper than asked,
  and a 1-day orphan guard fell under Spark's 24-hour floor. Setting the
  operator's session timezone would have been the wrong fix; the offset makes
  the literal unambiguous without touching their session.

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

- **`--spark-remote`, `--spark-master` and `--spark-catalog`** — Spark had no
  CLI flags at all: the maintainer read `remote`, `master` and `catalog` from
  options that nothing on the command line ever populated, so `--engine spark`
  was reachable and unconfigurable, and `--trino-catalog` was silently
  configuring Spark. Engine options are now built per engine.

- **`zamboni[spark-connect]`** — the Spark engine over Spark Connect. This is
  `pyspark-client`, ~1.5MB of pure Python against pyspark's 434MB, and it starts
  no JVM, so the machine's Java version stops mattering. Mutually exclusive with
  `zamboni[spark]`: both provide the `pyspark` package. Needs a Spark 4 server.

  With Connect the Iceberg extensions and the S3 credentials `remove-orphans`
  lists with belong to whoever operates the server — `spark.hadoop.*` is read
  when that server builds its Hadoop configuration, so Zamboni cannot supply
  them at call time and `zamboni doctor` cannot check them.

- **A Spark Connect server in the dev stack**, in its own `spark` profile
  alongside Trino's, plus a CI job that runs six live tests against it. The
  Spark maintainer's automated coverage was previously the SQL strings it
  generated and nothing else, which is how a timezone defect reached review.
  Its session timezone is deliberately not UTC, because a UTC server cannot
  distinguish a correct timestamp literal from one missing its offset.

- **[docs/user_guide.md](docs/user_guide.md)** — four ways to run Zamboni, for
  two audiences. A capability table that leads with Z-order, because that is the
  row a small deployment should choose an engine on and the one where the
  obvious choice (Trino) is the wrong one. Secrets posture, a multi-warehouse
  SaaS loop, and transient-engine scripts for Trino and Spark.

- **A public API.** `zamboni.__all__` was compaction-only, so an application
  that wanted to expire snapshots had to import `zamboni.expire` — a private
  path. It now exports the maintainer interface, the config types and the local
  operation classes, and states the compatibility promise on the list itself.

- **`zamboni table-config generate|validate|summary`** — `generate` writes a
  config describing the catalog *as it is today*, so the first run against it
  changes nothing but file sizes. `summary` answers what `validate` does not:
  what the file would *do*, marking every value that came from a default,
  naming what an unset knob resolves to instead of printing `None`, and
  flagging the settings that silently do nothing on the wrong engine.
  `validate-config` remains as an alias.

- **[docs/runbook-dev.md](docs/runbook-dev.md)** — the developer half of the
  runbook: running each step by hand, the six-verb order and why each position
  matters, cadence arithmetic, sizing the orphan guard, the dev stack.
  [docs/runbook.md](docs/runbook.md) is now what an operator opens when a cycle
  has failed — exit codes first, getting a stack trace out of cron, table
  status, a health check, and common failures.

- **`LayoutFeature` and `MaintainerCapabilities.layout`** — Z-order, sort,
  partition evolution and output-size control are layout *settings* rather than
  verbs, so `OperationSupport` had nowhere to record them and they existed only
  as prose inside `compact`'s limitations. Each engine now declares them, and
  `zamboni engines` prints them. `zamboni table-config summary` derives its "not
  available on: trino" warnings from those declarations instead of a hardcoded
  string that would have gone stale the day Trino gained Z-order.

### Changed

- **`MemoryMode.CHUNKED` now bounds peak memory**, which it has always claimed
  to and never did. Compaction reads **one data file at a time** instead of
  handing PyIceberg the whole task list. That mattered because
  `ArrowScan.to_record_batches` materialises each data file into a list before
  yielding any of it, and drives that with `executor.map`, which submits every
  task at once and returns results in order — so files that finished early sat
  in memory waiting for the consumer, and peak grew with the group.

  Measured end to end with file size held at ~28MB while the group grew 4×:

  | Group | before | after |
  |---|---|---|
  | 224 MB | +822 MB | **+541 MB** |
  | 447 MB | +1088 MB | **+527 MB** |
  | 894 MB | +1111 MB | **+577 MB** |

  Flat is the point: peak is now set by the largest data *file*, so a partition
  larger than RAM compacts. It applies to the CHUNKED path only, because it
  costs time.

  That cost was then measured against object storage rather than local files,
  since the parallelism being given up is what hides network latency — MinIO
  through Lakekeeper with vended credentials, 228MB in 96 files, with a proxy
  injecting per-request RTT: 1.12× at 0ms, 1.26× at 10ms, 1.39× at 30ms. All of
  it was serialised round trips, which is what the read-ahead window below gives
  back.

- **`read_ahead_bytes` (64MiB) and `max_read_ahead_files` (8)** — a bounded
  window of files in flight, so CHUNKED is no longer slower than reading
  everything at once:

  | RTT | one file at a time | windowed | unbounded |
  |---|---|---|---|
  | 10 ms | 20.8 s | **15.3 s** | 15.9 s |
  | 30 ms | 36.2 s | **25.8 s** | 26.3 s |

  The window is sized in **bytes rather than files**, so it adapts: many small
  files get real concurrency — the case with the most round trips to hide —
  while a few large ones fall back towards one at a time, the case where memory
  binds. Peak stays bounded by the window rather than the group (692/840/784MB
  as a 28MB-file group quadrupled, against 822/1088/1111MB unbounded), at about
  70% of the unbounded peak rather than the 60% strictly-serial reads managed.
  Set `read_ahead_bytes=0` to restore those.

  A group cap would also have bounded memory and was **rejected**: clustering
  quality is a function of how many rows the sort can see at once, so N
  sub-groups would produce N overlapping ranges and silently degrade every
  Z-ordered table. Bounding the read avoids that — DuckDB still receives the
  whole group and spills its sort to disk.

- **`memory_budget_bytes` default lowered from 1GiB to 256MiB.** This is the
  size above which `AUTO` chooses CHUNKED. The old value predates CHUNKED
  bounding anything: crossing it bought nothing, so it was set high to avoid a
  slower path for no benefit. `IN_MEMORY` on a 1GiB group measures ~2.3GiB of
  peak growth, which is more than a small host has. Raise it if you have memory
  to spare and would rather have the speed.

- **`--read-ahead-bytes` and `--max-read-ahead-files`** — the two settings
  added above, now reachable from the command line. They shipped as dataclass
  fields wired into the backend with no flag, so an operator could not use them.

- **A complete controls reference** in [docs/user_guide.md](docs/user_guide.md):
  all four places a setting can live and which owns what, plus two worked
  configurations — general data, and day-partitioned event data with day→month
  evolution. Both are loaded by the test suite, so they cannot rot against the
  schema.

- **[SECURITY.md](SECURITY.md) and [CONTRIBUTING.md](CONTRIBUTING.md).** The
  security policy treats data loss as its first category and says plainly that a
  report does not need to be attacker-triggerable to count — a logic error that
  makes a live file look unreferenced fires without anyone trying. The
  contributing guide writes down the conventions that were previously visible
  only by reading commits.

- **An SPDX tag on every source file.** `# SPDX-License-Identifier: Apache-2.0`,
  one line, with a pre-commit hook that fails on a file without it. Chosen over
  the full Apache header (520 lines, 4.5% of the codebase, 76% of the smallest
  module) and over nothing at all: the tag gives the machine-readable provenance
  that survives a file being copied out, at a fifteenth of the cost.

- **A `Secrets` section covering all four deployment shapes** — cron, the Python
  API, a subprocess, and Airflow — in
  [docs/user_guide.md](docs/user_guide.md). They leak in different places, and
  the guide previously covered only cron.

- **The README is written for someone who has not decided yet.** It opens with an
  install line, a **Status** block (0.x, one author, CI written and never
  executed, the PyIceberg cap) and a what-it-is-and-is-not list, instead of
  opening with the evidence that nothing else does this. No CI badge: a badge
  that has never run is a claim.

- **`trino:` and `spark:` blocks in `zamboni.yml`.** A host, a port, a user name
  and a catalog name are not secrets, and that file is defined as everything
  which is not one — but the profile refused those keys, so they could only come
  from a flag or `.env`. Keys are allow-listed per engine and there is
  deliberately none for a password.

- **Spark settings in `env.sample`**, which documented five `ZAMBONI_TRINO_*`
  variables and no Spark equivalent.

- **`maintain()` — the CLI's `maintenance` run, callable from Python.**

  ```python
  from zamboni import CatalogSession, maintain

  report = maintain(session, table_config="table-config.json", commit=True)
  raise SystemExit(report.exit_code)
  ```

  The loop lived only in `cli.py`, and the user guide told integrators to write
  their own — so the operation order, the `fulfilled_by` skip, which exceptions
  are refusals rather than failures, and when to stop after a safety abort all
  existed twice. Now one implementation, with the CLI as a printing adapter over
  it. `report.exit_code` is the number `zamboni maintenance` would have exited
  with, and a test pins that. `commit=False` is the default, matching the rule
  that nothing commits without `--yes`.

### Fixed

- **Manifest-pruning safety is decided by behaviour, not by a private symbol
  name.** The probe asked whether
  `_SnapshotProducer._build_delete_files_partition_predicate` existed. That
  method is present on PyIceberg `0.12.0rc1`, **which corrupts data**, and on
  the builds that fix it — its behaviour changed while its name did not, so no
  name-based check could tell them apart. Zamboni now runs the smallest
  operation that would go wrong (two rows in a day-partitioned table, replace
  one, count) and looks at the result. `0.11.1` does not prune and short-circuits
  at 3ms; a build that does prune pays ~1.7s once per process. `zamboni doctor`
  reports whether the answer was observed or assumed.

  This also unblocks PyIceberg 0.12: the unmodified suite passes against it —
  496 tests plus 31 against live Lakekeeper, MinIO, Trino and Spark — where
  before, 83 failed for this one reason.

- **`.env` is now looked for under `$ZAMBONI_ROOT`**, after `--env` and
  `./.env` — the same order the profile already used. `docs/devops.md` puts the
  fleet-wide `.env` there, so the documented multi-tenant layout worked only
  when the cron line's `cd` made the working directory and `$ZAMBONI_ROOT` the
  same place; from anywhere else the symptom was a run with no credentials
  rather than an error. A foreign `.env` in the working directory no longer
  masks the fleet's, and finding none remains legal.

- **Spark addressed a nested namespace as one dotted identifier**, which it
  rejects. Verified against live servers: Spark needs one quoted part per level
  (`` `ice`.`a`.`b`.`events` ``) and refuses a dot inside a part; **Trino needs
  exactly the opposite** (`"ice"."a.b"."events"`) and refuses the per-level form
  with "Too many dots in table name". Trino's existing code was correct and is
  unchanged. Two engines, mutually incompatible spellings of the same table.

- **`docs/devops.md` documented an `--all-warehouses` flag that does not exist**,
  including what its `--help` said. The claim is removed rather than the flag
  added — every argument in that section is an argument against a loop inside
  Zamboni.

- **The README's first code sample carried a credential-shaped literal.** A
  document telling operators to keep secrets out of files and command lines
  cannot open with one pasted into a code block — anyone copying the sample
  inherited the habit. Samples read from the environment now, and
  `test_no_document_carries_a_credential_shaped_literal` fails on a regression.

- **The CI section claimed four jobs when there are five**, and read as though
  it were green. It now says plainly that it has never executed.

- **`S3Settings` printed its secret access key in `repr()`.** A frozen dataclass
  prints every field, so the key would appear in any traceback rendered with
  locals, any `logger.debug("%s", settings)`, and any error aggregator. Nothing
  in this package logs it, which is why it had gone unnoticed. Redacted; the key
  *id* is kept, since that is what identifies a wrong credential.

- **Secrets passed as flags are now warned about.** `--token`, `--credential`
  and `--s3-secret-access-key` put a value on the command line, where any local
  user can read it from `ps` or `/proc/<pid>/cmdline` — confirmed by reading one
  back — and where shell history keeps it. A warning rather than a refusal,
  because an interactive one-off is a legitimate use.

- **A group- or world-readable `.env` is now warned about.** The guide has always
  said `chmod 600`; nothing checked.

- **`--memory-budget-bytes` ignored the default it was supposed to have.** The
  flag hardcoded `1 << 30` while `CompactionConfig` said 256MiB, so the
  threshold lowered in this release reached Python callers and **not the CLI** —
  every command-line run kept the old 1GiB behaviour. CLI defaults now come from
  the dataclass instead of being repeated as literals.

- **Z-order was unreachable from the CLI on any engine but the local one.**
  `table-config.json` ordering was translated into a compaction config only on
  the local path, so `--engine spark --table-config …` silently compacted
  without the ordering the file asked for. The translation now happens for every
  engine.

### BREAKING

- **`table-config.json` is version 2: warehouse -> namespace -> table.** The file
  now has the shape every data engineer already has — an Iceberg warehouse is a
  Postgres/Snowflake **database**, a namespace is a **schema** — instead of a
  dotted key whose split had to be guessed:

  ```json
  {"version": 2, "warehouse": "acme",
   "namespaces": {"analytics": {"tables": {"events": {}}}}}
  ```

  `warehouse` is required and **asserts** rather than selects: `--warehouse`/`--db`
  or the per-customer directory chooses, and a file naming a different one stops
  the run. Table names may not contain a dot. A dot in a *namespace* means
  nesting, unambiguously. Version 1 files are refused with a message naming the
  shape rather than a generic unknown-key error; there is no migration, because
  nothing has shipped against it.

- **`--catalog` is not a flag**, and will not be: it already means the engine's
  catalog in `--trino-catalog`/`--spark-catalog`, and a Singer catalog file in
  `from-catalog`. Use `--warehouse`, or its alias **`--db`**.

- **`table-config generate` and `from-catalog` require `--warehouse`/`--db`.**
  Without it they could emit a file that fails its own validation.

- **`--token`, `--credential` and `--s3-secret-access-key` are removed.** A
  value on a command line is readable by every local user from `ps` or
  `/proc/<pid>/cmdline` — confirmed by reading one back — and shell history
  keeps it. Set `ZAMBONI_TOKEN`, `ZAMBONI_CREDENTIAL` or
  `ZAMBONI_S3_SECRET_ACCESS_KEY` instead. The flags still parse, only to exit 2
  naming the variable, so a script that used them says what to change rather
  than `unrecognized arguments`. `--s3-access-key-id` is kept: a key id is an
  identifier, not a secret.

- **A `.env` readable by group or other now stops the run.** It was a warning in
  the same release; a warning on a nightly cron job is a line in a log nobody
  opens. `chmod 600` — or `0400`, which also passes, since the check is for
  group and other access rather than an exact mode.

- **Only `ZAMBONI_*` entries are read from a `.env`.** Such a file is very often
  shared with docker compose or a framework, and loading all of it meant Zamboni
  silently altering the environment of everything downstream. A discovered file
  with no `ZAMBONI_*` entries is now treated as not ours and ignored entirely;
  the same file named with `--env` is an error, because there you meant it.

### SAFETY

- **Spark identifiers containing a backtick could target a different table.**
  The plain string a Spark procedure takes was derived by stripping every
  backtick off the quoted form, but `quote()` doubles an embedded backtick to
  escape it — so ``we`ird.ta-ble`` reached `remove_orphan_files` and
  `expire_snapshots` as `weird.ta-ble`, a different table, with no error. Found
  by an independent review before any release carried it. The plain form is now
  built from the original name and never by unquoting.

- **Spark compaction ignored the dangling-delete settings.** The
  `remove-dangling-deletes` option was hard-coded `true`, so an operator who
  disabled that removal in `table-config.json`, or set `dangling_delete_policy:
  block` specifically so compaction would refuse rather than touch delete files,
  got them deleted anyway and silently. It now follows the config, and the
  coupling is declared in `zamboni engines` so the side effect is discoverable
  from `compact` rather than only from the operation it rides on.

- **PyIceberg is now capped at `<0.12`.** 0.12 corrupts data on a partitioned
  `upsert`: it leaves the row it replaced *and* duplicates one it never touched,
  with no error. Reproduced in 25 lines and filed upstream as
  [apache/iceberg-python#3758](https://github.com/apache/iceberg-python/issues/3758)
  — see [docs/upstream-0.12-upsert-regression.md](docs/upstream-0.12-upsert-regression.md).
  The previous `>=0.11.1` had an open upper bound, so the day 0.12 published,
  any `uv lock --upgrade` would have pulled it in without anyone touching this
  code. The cap lifts when 0.12 is released *and* the regression is fixed.

- **The equality-delete capability probe could report a false positive.** It
  inspected `DataScan._plan_files_local` for PyIceberg's refusal string. That
  method holds the guard inline in 0.11.1, but newer PyIceberg extracted the
  planner and left it a five-line delegation — so the probe found nothing and
  reported equality deletes as *readable* while the refusal was alive one call
  deeper. On the probe whose job is stopping compaction from resurrecting
  deleted rows. It now searches the whole `pyiceberg.table` module, and a test
  fails if it is ever narrowed back.

- **Partition evolution no longer relies on upstream manifest pruning.** Newer
  PyIceberg appends a manifest its partition-predicate evaluator does not match
  *verbatim*, entries being deleted included — which duplicated rows on a
  multi-spec commit. `_surviving_manifests()` matches entries directly instead.
  Correct on 0.11.1 and on 0.12; no behaviour change on the released line.

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
