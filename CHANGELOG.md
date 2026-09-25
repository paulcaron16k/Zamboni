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

## [0.5.1] - 2026-09-25

### Changed

- **The dev stack and its client moved to Spark 4.1.3**, from 4.0.4. The `spark`
  extra is untouched at `>=4.0,<5` — it is a client for a cluster someone else
  operates, and their Iceberg runtime is their choice — so this affects
  contributors and CI, not installs.

  What made 4.1 adoptable was Iceberg, not Spark: 1.11.0, already pinned here,
  now publishes `iceberg-spark-runtime-4.1_2.13`. **4.2 remains unadoptable** for
  the same reason — Iceberg publishes no 4.2 runtime — so ZMBNI-45's watch will
  keep reporting `pyspark-client` 4.2.0 above our cap, correctly.

  Four versions move as a set: Spark 4.1.3 ships Hadoop 3.4.2 where 4.0.4 shipped
  3.4.1, and `hadoop-project` 3.4.2 declares AWS SDK 2.29.52 where 3.4.1 declared
  2.24.6. A mismatched pair fails at class-load with a `NoSuchMethod` inside the
  S3A client, so both were read off the image and the pom rather than assumed to
  have moved together. (ZMBNI-45)

- **`remove-orphans` now says the inventory-report path was declined rather than
  pending.** Its `limitations` string on the local and Spark engines pointed at a
  historical id as though that were the open ticket. An inventory-report file
  list was considered and declined — not on safety, which
  [design.md §6.6a](docs/design.md) works through in full, but on cost: measured
  on one table, the listing is ~50 ms against ~2,035 ms to compute the reachable
  set, so an inventory optimises 2% of the operation. (ZMBNI-94)

### Added

- **An Azure connection string is a fourth credential shape.**
  `ZAMBONI_AZURE_CONNECTION_STRING`, `storage.azure.connection_string` in
  `zamboni.yml`, and `AzureSettings(connection_string=...)`, mapped to
  `adls.connection-string` which is what `adlfs` and PyIceberg read.

  It is the form `target-iceberg` and `tap-any-file` already expose, so the
  ecosystem shares one Azure credential vocabulary (ELT-1016). Unlike the other
  three it is self-contained — account and credential in one value — so it needs
  no account name beside it, and the validation that used to demand one was
  relaxed accordingly.

  It counts as a secret everywhere secrets are counted: redacted in `__repr__`,
  and in `SECRET_PROFILE_KEYS`, so a profile carrying one is held to `.env`'s
  mode rule. It carries the account key inline, which makes it the most complete
  credential of the four rather than the least. (ZMBNI-104)

## [0.5.0] - 2026-09-22

The release security review (docs/releasing.md §3a) found one item: `Profile`
gained secret fields this cycle and its generated dataclass repr printed them in
full. Fixed below, with a test. The other seven items passed, and there is no
dependency delta at all — `pyproject.toml` and `uv.lock` are unchanged since
0.4.0 apart from the version itself.

### BREAKING

- **DuckDB now spills to the system temp directory, not to `.tmp` under the
  working directory.** Previously `temp_directory` was left unset by default,
  which meant DuckDB's own default — `.tmp`, *relative to the current working
  directory*. After upgrading, a run writes its spill files somewhere different;
  an operator who granted write access to the working directory specifically, or
  who sized a volume for it, needs to know. The new default honours `TMPDIR`.
  Setting `temp_directory` explicitly is unchanged and still wins.

  It is a `BREAKING` line under the rule that a changed default which decides
  where a nightly run writes is public surface, not an implementation detail.

### Fixed

- **`Profile` printed its credentials.** It held nothing secret until
  `credential`, `token` and `storage` were added this cycle, at which point the
  generated dataclass repr rendered all three in full — and a repr reaches a
  traceback with locals, a `logger.debug("%s", profile)`, `pytest --showlocals`
  and any error aggregator, so nothing had to log it deliberately for it to
  leak. The same defect `S3Settings` had, found by the release checklist rather
  than by a test; there is now also a test, deriving what to redact from
  `SECRET_PROFILE_KEYS` so a future secret is covered without anyone
  remembering. Non-secret fields still print, because "which warehouse, which
  provider" is what a repr is read for.

- **A run with no usable storage credentials ended in a traceback, not a
  refusal.** `StorageCredentialsRequired` was raised before anything ran — which
  was the point — but `maintain()` did not catch it, so a fleet run against a
  remote-signing catalog with no credentials configured failed with a traceback
  from the first table instead of one clear reason per table. It now joins
  `WorkspaceUnavailable` and the other config refusals at **exit 2**, which is
  the shape ZMBNI-76 established. The same applies to the two new refusals:
  credentials for the wrong store, and a cloud backend that is not installed.

- **A build observed losing rows could still be declared safe.**
  `manifest_pruning_is_safe` read `derives_delete_predicate or not
  prunes_manifests_by_predicate`, and the structural half short-circuited the
  behavioural one — so "this build shows no sign of pruning" granted safety with
  nothing measured. What decides that is the string `manifest_evaluator` grepped
  out of `_OverwriteFiles._existing_manifests`, and on 0.12 it matches a **local
  variable name inside the function body**. An upstream rename — a refactor
  changing no behaviour — was enough to skip the measurement and declare a
  pruning build safe. That is the ZMBNI-1109 class one level cheaper to trigger.

  The measurement is authoritative now: the overwrite runs whatever the source
  looks like, and the structural answer is consulted only when nothing could be
  observed, where it can withdraw safety but never grant it unexamined. Its
  original justification was cost — "does not prune → no cost, this is 0.11.1,
  which is every current user" — and both halves have expired: the floor is
  `>=0.12`, every supported build prunes, and since ZMBNI-88 the probe is paid
  once per install. (ZMBNI-38)

- **`delete_manifests_writable` believed a parameter name.** It returned True
  merely because `write_manifest` had grown a `content` parameter. A parameter
  that exists but does not mean what we assume would engage ZMBNI-9's rewrite
  path and stamp a delete manifest `content: data`, after which a reader treats
  position deletes as **rows** — silent corruption, and the only structural
  probe whose wrong answer was silent rather than loud. It now writes a manifest
  holding a position-delete entry and reads the label back off
  `to_manifest_file()`, which is where the label actually lives. Measured at
  **3 ms**, against the ~200 ms the story estimated: that assumed a table, and
  no table is needed. (ZMBNI-38)

- **A container with a read-only root filesystem failed partway through a
  rewrite instead of refusing up front.** DuckDB creates its spill directory
  lazily, only when a rewrite actually exceeds the memory budget — and `CHUNKED`
  is chosen precisely when a group will not fit — so the failure struck the
  *largest* tables, deep inside the backend, after the read was done.
  Reproduced before fixing:

  ```
  temp_directory setting : '.tmp'
  spill                  : IOException: Failed to create directory ".tmp": Permission denied
  ```

  The location is now resolved and checked at the top of `execute()` for any
  memory mode that can spill, with a refusal naming `temp_directory`,
  `--temp-directory`, `TMPDIR` and the Kubernetes `emptyDir` fix. `zamboni
  doctor` reports the resolved path and whether it is writable, so the check can
  be run before deploying. `maintain()` maps it to **exit 2** beside the other
  configuration refusals, so one misconfigured deployment does not end a fleet
  run with a traceback. (ZMBNI-90)


### Added

- **TLS transport security for the catalog connection**: `--ssl-ca-bundle`
  (`ZAMBONI_SSL_CA_BUNDLE`) trusts a private CA or self-signed certificate, and
  `--ssl-insecure` / `--no-ssl-insecure` (`ZAMBONI_SSL_INSECURE`) skips
  verification as a development escape hatch. An on-prem HTTPS Lakekeeper behind
  a private CA can now be maintained, which matches what target-iceberg and IWS
  already expose (ELT-1014 / ELT-1016).

  The mapping is narrower than it looks: `RestCatalog` has no verify boolean, so
  both settings land on one property — `ssl.cabundle`, which `requests` assigns
  straight to `session.verify`, taking a path (trust) or the bool `False`
  (skip). `insecure` wins if both are given. **Catalog leg only**: the storage
  FileIO takes no TLS property, so the object store's trust comes from the
  container trust store or `AWS_CA_BUNDLE`.

  `lakekeeper_properties()` is split out of `for_lakekeeper` so the mapping is
  testable without a live catalog. Both settings also take an `ssl:` block in
  `zamboni.yml`, with the ordinary flag > environment > profile precedence; a CA
  bundle is a path to a public certificate, so that block does not make the
  profile a credential file. (ZMBNI-100)

- **`zamboni.yml` may hold secrets**, with the ordinary precedence: a flag, then
  a `ZAMBONI_*` variable, then the profile. `credential`, `token` and a
  `storage:` block (`s3` / `gcs` / `azure`) are now accepted there.

  The rule that made them illegal assumed the profile is committed — the common
  case, and not the only one. A Kubernetes Secret mounts as a *file*, so a
  profile projected from one is exactly as protected as an environment variable,
  and splitting a single deployment's configuration across two mechanisms buys
  nothing. Forcing the split also pushes the other way: an operator who cannot
  put a credential where the rest of the configuration lives moves the rest of
  the configuration to where the credential is, and `.env` becomes the profile.

  **A profile holding a secret is treated as a credential file**: it gets the
  same mode rule `.env` has always had, and readable by group or other is a hard
  error rather than a warning — a warning on a nightly cron job is a line in a
  log nobody opens. A profile with no secrets is unaffected and stays
  world-readable. `storage.s3.access_key_id` does not count as a secret, for the
  same reason `--s3-access-key-id` is still a flag; `storage.gcs.token` does,
  even when it holds `google_default`, because deciding per value would make a
  file a credential file on one day and not the next.

  Nothing existing changes: the environment still wins, so a deployment that
  injects secrets properly is unaffected by anything the profile says. The
  engine blocks still have no password key — not because the file may not hold
  one, but because neither maintainer accepts one.

- **Storage credentials for GCS and Azure, not only S3.** `CredentialUse` is how
  Zamboni reclaims storage from a warehouse whose catalog remote-signs — and
  until now the only credentials it could be given were S3's, so a GCS or Azure
  Blob warehouse had no way to reclaim at all. That is not a corner: IWS
  provisions every warehouse with `sts-enabled: false`, so the own-credentials
  path is not a fallback there but the only path, and ExperienceFlow's IWS
  deployments run in GCP and Azure.

  `GCSSettings` and `AzureSettings` join `S3Settings`, which is unchanged
  because it is public API. Set `ZAMBONI_GCS_TOKEN` or
  `ZAMBONI_AZURE_ACCOUNT_NAME` (with one of key, SAS token, or client secret);
  S3 keeps its existing flags. Ceph RGW and other S3-compatible stores need
  nothing new — they differ only in `endpoint`.

  **GCS is routed through `gcsfs`, deliberately diverging from PyIceberg**, whose
  `SCHEMA_TO_FILE_IO` maps `gs` to `PyArrowFileIO` only. Both read
  `gcs.oauth2.token`, but pyarrow takes it as a bearer token with an expiry —
  useless to a deployment holding a service-account key file, and a token that
  expires part-way through a run. `gcsfs` reads the same property as a key-file
  path, as `google_default` (Application Default Credentials, which GKE Workload
  Identity provides and which needs no secret in the environment), or as a raw
  token. Azure follows PyIceberg's own preference and is not a divergence.

  **Credentials for the wrong store are refused up front.** The table's location
  scheme decides which provider fits; handing S3 credentials to a `gs://` table
  would otherwise fail at the first read, or worse return an empty listing,
  which orphan removal reads as "everything is unreferenced". (ZMBNI-97)

- **Every capability probe now has an independent cross-check.**
  `operation_is_injectable`, `replace_summary_supported` and
  `streaming_write_supported` were asserted only to be real booleans and to
  appear in `doctor` output — neither asks whether the value is *right*, so a
  probe that silently flipped passed both. Each is now re-derived by a different
  route: constructing a producer with `operation=` for real, reading what
  `update_snapshot_summaries` accepts, and handing `_dataframe_to_data_files` an
  actual `RecordBatchReader`. That last one matters most, because the streaming
  compaction tests *skip* when the probe is False — a wrongly-False probe made
  them pass by not running. Verified by flipping each probe and confirming the
  matching cross-check fails. (ZMBNI-38, ZMBNI-14)

- **Probe results are remembered on disk, keyed to a hash of the installed
  build.** `zamboni doctor` and every run call `capabilities.detect()`, which
  costs ~0.6s because two of its probes create a table and commit to it. That
  was already cached per process, which is enough for a run over a hundred
  tables and useless to an operator whose cron line invokes the CLI per table —
  where it was the dominant cost of a no-op run.

  Keyed on **content, not on a version**: the fork and stock PyIceberg both
  declare `0.12.0` and disagree about `added_files_honour_spec`, so the key is a
  hash over each distribution's version, its `direct_url.json` and its `RECORD`,
  whose per-file hashes move whenever a shipped file does. Measured on a real
  venv: installing the fork over stock changed the key and re-probed, and
  installing stock back again restored the original answers from its own entry
  without probing.

  **An editable install of either PyIceberg or Zamboni disables it entirely.**
  An editable `RECORD` lists 15 entries rather than the installed files, and the
  tree underneath moves with `git checkout` — the exact scenario that closed
  ZMBNI-39 as not planned. Nothing here can fail a run: an unwritable
  filesystem, an unreadable entry or a build that cannot be keyed all mean
  probe, which costs the half second and nothing else. `zamboni doctor` reports
  which happened, and `ZAMBONI_NO_PROBE_CACHE=1` turns it off. (ZMBNI-88)

- **A `consumer` CI job, running what a normal install gets.** Every other job
  resolves PyIceberg from the maintenance fork via `[tool.uv.sources]`, which is
  right for developing against unreleased fixes and is not what anyone downstream
  has — that directive does not reach wheel metadata, so `pip install
  iceberg-zamboni` resolves PyIceberg from PyPI.

  ZMBNI-83 makes that build work by carrying a probe-gated fallback, which on
  every other leg is **dead code**. This job builds the wheel, `pip install`s it
  into a clean venv, asserts the build really is stock, and runs the suite
  against it.

  It found three tests that only held on the fork, which is the point of having
  it: two capability guards that hard-asserted the fork, and an evolution
  parametrisation that forced a probe True on a library that cannot honour it —
  a configuration that cannot exist, and which produced a silently wrong result
  rather than an error. The guards are now build-aware in both directions, so a
  leg that resolved the wrong PyIceberg fails instead of agreeing with whatever
  it found. (ZMBNI-40)


## [0.4.0] - 2026-09-17

### BREAKING


- **`ReplaceCommitter.commit` no longer takes `expected_snapshot_id`.** It backed a
  table-level conflict check that has been deleted (below). The parameter is gone
  rather than ignored: a library caller passing it gets a `TypeError` and reads
  this entry, where a silently accepted no-op would have taken away a guard they
  believed they had. `ReplaceCommitter` is public API; nothing else in its surface
  changed. (ZMBNI-79)

### SAFETY


- **`table-config.json` now type-checks every value, and a `null` is refused
  where it used to be swallowed.** The case this is filed under SAFETY for:
  `{"remove_orphan_files": {"enabled": null}}` **loaded, and silently disabled
  reclamation.** `dict.get("enabled", True)` returns `None` when the key is
  present with a null — the default only applies when the key is *absent* — and
  `None` is falsy, so a run reported "disabled in the config" for a config that
  never said so. That is precisely the "silently reclaiming far less than
  expected" outcome `Retention.validate` warns about, reached by a typo, in a
  tool whose job includes deleting files.

  Two more shapes of the same root cause — values were never checked as they were
  read. A `null` where a *block* belongs escaped as a bare `TypeError: 'NoneType'
  object is not iterable` from `_reject_unknown`, which called `set(raw)` on
  whatever it was handed; measured across all fourteen nested blocks, **thirteen
  crashed.** And a wrong-typed scalar reached arithmetic inside `validate()`:
  `min_input_files: "day"` raised `TypeError: '<' not supported`. Neither is a
  `TableConfigError`, which is the type the CLI maps to exit 2 and the user guide
  documents as *the* config failure — so both were broken promises, surfacing as
  a traceback with no path.

  Every value now goes through one checked accessor that names the key, the path
  and what it found: `namespaces.a.tables.b.min_input_files: expected a number,
  found a string`. `bool` is rejected where a number is wanted, since it is an
  `int` subclass and `min_input_files: true` would otherwise read as `1`.

  **What still accepts `null`, unchanged:** the settings documented as "leave
  whatever is there" — the `expire_snapshots` windows, the `metadata` properties,
  `target_file_size_bytes`, `description` — and the four list-valued keys the
  loader coalesces to empty (`namespaces`, `partition`,
  `partition_evolution.rules`, `ordering.sort`). Narrowing those would break
  configs that work, and the shipped JSON Schema declares exactly those keys
  nullable; a sweep over every field position asserts the schema and the loader
  agree about `null` everywhere, which is what found all of this.

  **This narrows accepted config values, which the BREAKING definition above
  names.** Filed under SAFETY because it ships in a patch release for the reason
  SAFETY exists: a configuration that silently disabled an operation is worse
  than one that is refused. If you have a `null` in a `table-config.json`, the
  run now tells you where — and if it was on an `enabled` flag, that operation
  was not running.
- **Every operation that commits through the private snapshot producers now
  refuses an unsupported PyIceberg build.** `assert_supported_pyiceberg()` had
  one caller, so five of the six mutating operations never consulted it — and
  two of those, `rewrite-manifests` and `remove-dangling-deletes`, commit through
  `_ReplaceFiles` subclasses, which is the machinery the guard exists to protect.
  On a build that prunes manifests by predicate without deriving the predicate
  correctly, the manifest holding a replaced file is kept verbatim and its rows
  are counted twice; compaction refused such a build and those two proceeded.

  The guard now runs in `_ReplaceFiles.__init__` **and** in
  `ReplaceCommitter.commit`, which is where the producer class is chosen at
  runtime — `snapshot_operation="overwrite"` and any build with native REPLACE
  summaries both select the stock `_OverwriteFiles`, so guarding the subclass
  alone would have left those paths open. `expire`, `apply-properties` and
  `remove-orphans` deliberately do not consult it, for reasons recorded on the
  function.

  `rewrite-manifests` and `remove-dangling-deletes` now **declare** the same gate
  compaction does, so `zamboni engines` stops advertising support that the run
  refuses, and the refusal is exit 3 with a reason rather than an uncaught
  exception — which previously would have aborted an entire fleet run.

  **Latent rather than exploited**: on 0.11.1, the only version the `<0.12` cap
  admits, `prunes_manifests_by_predicate` is false and the guard never fires. The
  exposure would have arrived with the cap being lifted.

### Security


- **Every GitHub Action is pinned to a commit SHA**, with the release in a
  trailing comment. They were pinned to movable refs, so each job ran whatever
  `v4` or `release/v1` pointed at that morning — including the `release.yml`
  job holding an OIDC credential that can publish to PyPI. `.github/dependabot.yml`
  proposes monthly bumps so the pins do not rot, and two tests keep both halves
  honest. Raised by the 0.3.0 security review; ZMBNI-1817.

### Added


- **`as_dict()` on every operation result**, alongside `describe()`. Until now the
  only machine-readable thing a run produced was an exit code: everything about
  *what changed* was English prose, so an integrator exporting per-operation
  counters had to regex sentences that [docs/releasing.md](docs/releasing.md)
  explicitly does not cover — meaning a wording improvement was a breaking change
  by accident, in the one direction the contract could not describe.

  `Outcome` and `MaintenanceReport` carry it too, so a whole run serialises in one
  call. Keys are stable identifiers, JSON-serialisable, and never contain a
  PyIceberg object — `DanglingReport.removable` and `RewritePlan.replaced` hold
  `DataFile`/`ManifestFile` and are reported as counts, so upstream's internal
  representation does not become something this package owns.

  **The keys are now a covered surface**: removing or renaming one is breaking,
  adding one is not, and `describe()`'s wording remains explicitly uncovered.
  Trino reports no counters and so has none — absent rather than zero, because
  inventing `files_rewritten: 0` would be a false measurement dressed as a uniform
  schema. Raised by the first production integrator.
- **`remove-orphans` and `expire` now reclaim storage on Zamboni's own object-store
  credentials, governed by `ZAMBONI_CREDENTIAL_USE`.** A warehouse whose catalog
  remote-signs instead of vending credentials could not reclaim anything: Lakekeeper's
  signer refuses `ListObjectsV2`, `HeadObject` and multi-object `DELETE`, so `expire`
  committed and freed nothing while `remove-orphans` failed outright. Reads and writes
  worked throughout, which is why the warehouse looked healthy until you tried to free a
  byte.

  The warehouse system owns its object store; the catalog is a service in front of it, and
  remote signing exists to constrain external readers rather than the maintenance job. So
  given bucket credentials, Zamboni now uses them and the signing policy no longer applies
  to it — which is what Spark has always done via `spark.hadoop.fs.s3a.*` on the Spark
  server.

  | `ZAMBONI_CREDENTIAL_USE` | Behaviour |
  |---|---|
  | `always` *(default)* | Zamboni's credentials for every operation, when configured |
  | `reclaim-only` | `expire` and `remove-orphans` only; reads stay on the catalog's |
  | `never` | The catalog governs Zamboni as it governs any client |

  Under the first two, **a signing catalog with no credentials configured is refused before
  anything runs**, naming the table and what to set — rather than a reclaim pass that lists
  what it can and deletes what it managed to sign.

  Not a change for a credential-vending (`sts-enabled: true`) warehouse: there is nothing to
  override, and nothing is refused. Not a way around a deliberate boundary either — it needs
  credentials someone has to grant. The safety invariants are untouched: owning the storage
  changes who authenticates, not what may be deleted.

  Implemented by replacing the table's `FileIO`, because PyIceberg has no supported
  precedence for client-supplied storage credentials — passing `s3.access-key-id` and
  `s3.endpoint` to a signing catalog is measurably a no-op, and silently so. Tracked as
  ZMBNI-56 with the measurements, and the override is deleted when upstream gains one.
- **A JSON Schema for `table-config.json`, shipped in the wheel and served by
  `zamboni.get_table_config_spec()`.** Anything that *writes* these files — a
  Meltano catalog conversion, a UI, a service modelling its own warehouses — had
  nothing machine-readable to check its output against, and had to pull a copy
  from GitHub or guess a path inside the installed package.

  **Generated from the dataclasses in `zamboni.tableconfig`, not hand-written.**
  A hand-written schema would make three descriptions of one format —
  `docs/table-config.md`, `tableconfig.py`, and the schema — and two of them
  would drift. `scripts/build-table-config-schema.py` regenerates it and a test
  fails when the committed file falls behind, the same arrangement as `bin/`.

  It is a **shape** check: keys, types, enumerations, and
  `additionalProperties: false` everywhere, matching the loader's own refusal of
  unknown keys. Cross-field rules cannot be expressed in JSON Schema —
  `ordering.mode: "sort"` requiring a non-empty `sort` list, evolution having to
  move to a coarser granularity — so `TableConfig.load()` stays the authority and
  a document can satisfy the schema and still be refused. The reverse never
  happens, and a test pins it by comparing the schema's keys against the key set
  the loader's own error message declares.

  The schema declares its dialect with `$schema` (draft 2020-12) and pins the
  format's `version` to `SPEC_VERSION`, so a schema from an older wheel rejects a
  newer file loudly rather than half-accepting it. `jsonschema` is a test-only
  dependency: the API hands back a dict and lets the caller pick a validator
  rather than making every install carry one.

- **`azure`, `gcs` and `cloud` extras.** Azure was the gap: `abfs`/`abfss` *prefer*
  `FsspecFileIO`, and PyIceberg's fallback to `PyArrowFileIO` cannot save it —
  `_infer_file_io_from_scheme` falls through only on a `ModuleNotFoundError` at
  construction, and `FsspecFileIO({})` constructs fine without `adlfs`, so the
  failure is deferred to first use.

  **GCS needs no extra** and never did: `gs` maps to `PyArrowFileIO` only, so
  fsspec is never chosen for it and pyarrow's `GcsFileSystem` does the work. The
  `gcs` extra exists for the narrow case where a catalog overrides `py-io-impl`.
  `cloud` pulls all three, for an image built before the tenant's cloud is known;
  it is deliberately not the default, because one deployment uses one cloud.
- **Partition evolution now works on a stock PyIceberg install**, not only on one
  redirected to the maintenance fork. `[tool.uv.sources]` is a uv workspace
  directive and does not reach wheel metadata, so `pip install iceberg-zamboni`
  resolves PyIceberg from PyPI — and until now that build had the feature
  *withdrawn*, because the library writes every added data file under the table's
  default partition spec.

  `MultiSpecReplaceFiles` carries the behaviour again as a **fallback**, and the
  behavioural probe `added_files_honour_spec` decides: where the library already
  does it, every override delegates to `super()` and the class is a name. So the
  complexity is absorbed here rather than pushed onto consumers, and no consumer
  has to redirect a source to get the full feature set.

  Two copies of one behaviour is the arrangement that hid ZMBNI-58 for months, so
  the guard against a repeat is structural rather than a comment: the fallback
  runs *only* when the probe says the library will not, so it disables itself the
  moment the library gains the behaviour and can never mask a fixed one. Because
  that makes it dead code on this repository's own CI,
  `test_evolution_condenses_days_into_a_month` is parametrised to force the probe
  false and drive the whole evolution path through the fallback — verified by
  instrumentation to enter both branches, once each. (ZMBNI-16)

- **A monthly version watch, in place of a nightly test run.** Every `<` bound in
  `pyproject.toml` is a decision with an expiry date -- `pyiceberg<0.12` is a
  data-corruption workaround, the dev group's `pyspark-client<4.1` is matched to
  the dev stack's server -- and nothing announced when one went stale.
  Dependabot cannot: its `uv` ecosystem updates `uv.lock` and not
  `pyproject.toml`, so with `<4.1` written down the most it can offer is a 4.0.x
  patch. `scripts/version_watch.py` asks PyPI about every cap the file declares,
  ignores pre-releases and fully yanked versions, and keeps one issue current.
  Seconds, no containers, no matrix. It found `pyspark-client` 4.2.0 on its first
  run.

  A nightly re-run of the suite was considered and rejected: every input to the
  tests is pinned -- `uv.lock`, exact image tags, pinned Maven jars, SHA-pinned
  actions -- so against an unchanged commit it re-proves the tick that commit
  already has.

### Changed


- **Compaction now leaves the partition a loader is still writing alone**, by
  default. `skip_partitions_newer_than_windows` defaults to `1`, which on a
  day-partitioned table holds **today and yesterday** — two partitions, because a
  loader extracting yesterday's data at 02:00 writes it into *yesterday's*
  partition, not today's.

  **DevOps should know this before deploying**: on time-partitioned tables, the
  most recent one or two partitions stop being compacted until their window
  closes. Nothing is deleted and no data changes; only how soon recent data is
  compacted. Set `0` to hold only the open window, or `null` to restore the
  previous behaviour. Tables partitioned on `identity`, `bucket` or `truncate`
  are unaffected — the floor is scoped to `hour`/`day`/`month`/`year`.

  The reason is the *writer's* cost. A copy-on-write upsert rewrites the whole
  data file holding a matched row, so compacting an active partition makes every
  later update more expensive in proportion to how well it was compacted.
  Measured, one upserted row: 3,542 bytes rewritten against 10 small files,
  26,126 once compacted into one — 7.4x, and the ratio is roughly the number of
  files merged, so nearer 64x at production file sizes. (ZMBNI-78)
- **PyIceberg is now `>=0.12,<0.13`, resolved from a maintenance fork.** Partition
  evolution needs a library that writes an added data file under *its own*
  partition spec rather than the table default. Stock PyIceberg does not: a
  partition `Record` has the arity of the spec that produced it, so an evolved
  file written into a manifest declared under another spec raises
  `IndexError: list index out of range` from inside the Avro writer, four frames
  down and naming nothing.

  That behaviour moved **out of Zamboni and into the library** — 241 lines of
  private-API override deleted from `evolution.py` — and lives on
  `feature/maintenance` in our PyIceberg fork, pinned by commit.

  **What this means for an install.** The wheel publishes only the range;
  `[tool.uv.sources]` is a uv workspace directive and does not reach wheel
  metadata, so `pip install iceberg-zamboni` gets stock PyIceberg. Rather than
  fail obscurely there, `capabilities.detect()` gained a behavioural probe —
  `added_files_honour_spec` — and a build without the behaviour has **partition
  evolution withdrawn as a layout feature**, with the reason stated. The six
  operations are unaffected and still run.

  A consumer that wants evolution redirects the source itself:

  ```toml
  dependencies = ["iceberg-zamboni", "pyiceberg"]

  [tool.uv.sources]
  pyiceberg = { git = "https://github.com/paulcaron16k/iceberg-python.git", rev = "<sha>" }
  ```

  `pyiceberg` has to be a *direct* dependency there — uv applies `tool.uv.sources`
  to the declaring project's own dependencies, and a redirect aimed only at a
  transitive one is silently ignored.
- **`pyiceberg-core` is a declared dependency.** 0.12 moved it out of the
  `[pyarrow]` extra while `transforms.pyarrow_transform` still needs it for six
  transforms — bucket, day, month, year, hour, truncate — so relying on that extra
  installs a build that cannot write a partitioned table at all.

- **PyIceberg's streaming writer is now opt-in (`streaming_writes`), not
  automatic.** It was selected whenever the build supported it and the rewrite was
  unpartitioned. Measured across 20/40/80 input files, compaction working set
  only: the streaming writer is **12–25% faster and uses 55–60% more memory**
  (551MB against 347MB at the largest size). Both are bounded, so neither breaks
  the bounded-memory guarantee — but `CHUNKED` is chosen precisely when a group
  will not fit the memory budget, so spending more memory there is the wrong
  default. Set `streaming_writes = true`, or `--streaming-writes`, where there is
  headroom and wall-clock matters.

  `zamboni engines` now reports the writer as *available* rather than in use: the
  probe says the build has one, which since this change no longer means a run
  will take that path. (ZMBNI-16)
- **Compaction no longer refuses because *something else* committed.** The
  pre-commit check compared the **table's** snapshot id against the one planning
  saw, so any commit anywhere — an append to a partition compaction never
  planned — ended the rewrite. Measured on a live catalog against an appending
  writer: **0 of 42 compactions succeeded**, every one having already rewritten
  its files.

  The check is redundant as well as coarse. PyIceberg 0.12 validates the right
  thing per file: `_validate_data_files_exist` refuses when a file this operation
  is deleting no longer exists, which is exactly the lost update the old check
  guarded against. Proven adversarially rather than argued — with the old check
  gone, a writer that rewrites a file in the removal set is still refused, the
  rows are not duplicated, and its update survives.

  A second, matching narrowing landed in the maintenance fork: serializable
  isolation ran `_validate_added_data_files` with a `None` filter for any
  operation without a predicate, and a `None` filter matches everything — so a
  file rewrite conflicted with *every* concurrent append. Java's `RewriteFiles`
  draws the line the same way this now does.

  Together: **0 → 31 of 32 successful compactions** against a writer committing
  continuously into another partition, and 27 of 32 into the *same* partition.
  The few remaining are the raw compare-and-swap after PyIceberg exhausts its
  retries — a real lost race, reported as the clean exit-3 refusal added in
  ZMBNI-76 rather than as a failure.

  `rewrite-manifests` and `remove-dangling-deletes` **keep** their equivalent
  checks, with the reason recorded at each: the redundancy argument depends on
  the producer populating `_deleted_data_files`, which rewriting manifests does
  not do at all, and which `remove-dangling-deletes` does only with *delete*
  files whose handling by that validator has not been measured. (ZMBNI-79)
- **A table being written to no longer ends the whole fleet run.** Losing a commit
  race to a live writer is a normal Iceberg outcome, and Zamboni already detected
  it correctly — `ReplaceCommitter.commit` re-reads the table and refuses rather
  than losing an update. But `maintain()` did not catch the refusal, so the
  exception propagated: the run stopped at the first busy table, every later table
  went unmaintained, and the caller got a traceback instead of the exit-code
  contract.

  Three exceptions now map to **exit 3**, the documented *"the table is blocked —
  a refusal, not a failure"* code, because the conflict surfaces at three depths
  and a fleet run cannot be asked to care which: our `ConcurrentModification`,
  PyIceberg's `ValidationException` from `_validate_concurrency` (which its own
  retry loop does not retry), and `CommitFailedException` once
  `commit.retry.num-retries` is exhausted.

  Found by testing rather than review, against a live Lakekeeper with an upsert
  writer committing every 50 ms: 67 compaction runs gave 60 successes, 3
  `ConcurrentModification` and 4 `ValidationException`. **Nothing was corrupted in
  any of them** — detection worked every time, and the defect was only ever in
  what happened next. (ZMBNI-76)
- **The docs said PyIceberg 0.11.x was fully supported, five days after it stopped
  being installable.** ZMBNI-59 moved the floor to `>=0.12,<0.13`; the README still
  carried a section headed "Why PyIceberg is capped at `<0.12`" telling readers the
  pin was `>=0.11.1,<0.12` and that "0.11.x is fully supported" — a promise
  `uv sync` could not keep. `docs/roadmap.md` still listed the support-window
  question as open after it had been answered by events.

  Both rewritten, along with the probe table (six probes, listed against a "main"
  that has since released — now eight, reported from `zamboni doctor`), the
  `doctor` sample output, and the V3 blocker, whose claim about 0.11.1's
  serialiser was re-checked on 0.12: still true, and now cited to the
  `NotImplementedError` it actually raises.
  `test_no_doc_states_a_pyiceberg_range_the_project_does_not_declare` makes the
  next such drift a build failure. (ZMBNI-18)
- **The dev stack's object storage is Silo (`pgsty/silo`), a maintained fork of
  MinIO.** Between 2026-09-09 and 2026-09-12 the `minio/minio` and `minio/mc`
  Docker Hub repositories stopped resolving — an anonymous `docker pull` answers
  `repository does not exist`, and the Hub v2 API 404s — which took out the
  `dev-stack` and `spark` CI jobs on every branch and any developer's
  `docker compose up`. MinIO has ended community distribution.

  The quay.io mirror was tried first and **rejected**: its last community
  release is `RELEASE.2025-09-07`, a year old, and everything pushed there since
  is a customer hotfix build on a 2024/2025 base. It resolves today but can
  never carry a security fix. Silo is the same lineage under maintenance — S3
  API, `MINIO_*` variables, `RELEASE.<timestamp>Z` tags and on-disk format
  unchanged, only its own delivery surfaces renamed.

  **Nothing in the stack's interface moves**: the service is still `minio`, the
  endpoint still `http://minio:9000`, and the credentials still `MINIO_ROOT_*`.
  `MINIO_VERSION` in `dev-stack/.env.sample` becomes `SILO_VERSION` — the only
  rename a developer with an existing `.env` has to make. (ZMBNI-67)
- **`MultiSpecReplaceFiles` now builds the delete predicate its base class
  requires.** Overriding `_OverwriteFiles._manifests` takes on that method's
  ordering, and ours called `_deleted_entries()` without the
  `_build_delete_files_partition_predicate()` that precedes it upstream.

  **No effect on the shipped 0.11.1 line**, where `_deleted_entries` walks every
  manifest and filters by identity, consulting no predicate — which is why this
  survived unnoticed. From PyIceberg 0.12 it gates each manifest on an evaluator
  built from `partition_filters`, defaulting to a projection of `self._predicate`,
  itself defaulting to `AlwaysFalse()`: with no predicate built, every manifest is
  skipped and nothing is found to delete. Silently, before
  apache/iceberg-python#3818 — the replaced files stayed live and rows duplicated
  — and as a `ValidationException` since.

  Measured: 19 test failures on PyIceberg 0.12.0 and on upstream `main`
  (`9299bdb8`) become 1, the remainder being an unrelated packaging assertion.
  This is what stood between us and adopting 0.12.
- **`apply-properties` no longer fails a run when the config declares no metadata
  properties.** `MetadataSettings` defaults both `previous_versions_max` and
  `delete_after_commit` to `None`, and its own documentation defines that as
  "leave the table property alone" — so declaring neither is a supported choice.
  The local engine read it that way and reported a clean no-op; **Trino and Spark
  raised `EngineConfigProblem`** for the identical config, and the difference was
  declared nowhere a caller could see it: Trino's `capabilities()` reports
  `apply-properties` as supported, so `check_engine_supports` had nothing to
  refuse and the operation was requested anyway.

  The consequence was worse than a wrong exit code. `apply-properties` is second
  in `RUNBOOK_ORDER`, immediately after compaction, and `MaintenanceReport.exit_code`
  is the *worst* outcome — so a committing fleet run compacted the table, then
  reported the table failed. Exit 2 means "usage/config", sending an operator to
  fix a configuration that was never wrong. Reported by the first production
  integrator, whose maintenance policy has no metadata settings at all, so this
  was every run of every table.

  All three engines now return the same result for that input — literally the same
  `ApplyResult`, so `as_dict()` is byte-identical across engines and a dashboard
  keyed on `changed` keeps its row. Neither engine opens a connection when there
  is nothing to run.

  **Why this is not BREAKING, though an exit code changed.** The definition above
  names exit codes deliberately, so the exemption is stated rather than assumed:
  the affected exit code is only ever produced by a run that did no work and had
  nothing to do, and it reported failure for a correct configuration. Nothing that
  worked stops working, and no pipeline can be relying on a spurious failure.
  Newly *refusing* an operation is the direction that breaks a pipeline, which is
  what the asymmetric fifth row of the covered surface in
  [docs/releasing.md](docs/releasing.md) is about.

- **The demo is `zamboni.demo`, not a top-level `himsdemo`.** Installing
  `iceberg-zamboni` put an unnamespaced `himsdemo` package on the import path --
  a name that says nothing about where it came from, and one any other
  distribution could collide with silently. The same objection that renamed the
  `demo` console script to `zamboni-demo`, and for the same reason; this was the
  case that was missed. Raised by someone installing the package.

  **Migration:** `import himsdemo.x` becomes `import zamboni.demo.x`. The
  `zamboni-demo` command is unchanged, and so is everything under `zamboni`. The
  demo's CSV fixtures move with it, to `zamboni/demo/data/` inside the wheel.

  **Deliberately not filed as `BREAKING`.** An import path moving is not one of
  the five things this file defines that term for -- a verb, a flag, an exit
  code, a config key, or a default that decides what gets deleted -- and
  [docs/releasing.md](docs/releasing.md) says of `src/zamboni/demo` that "the
  demo is a teaching aid that ships in the same wheel. It has no stability
  contract at all." Filing it as breaking would also commit the next release to a
  minor bump for a change the contract says carries no promise. It is called out
  here in full anyway, because "no stability contract" is a reason not to bump a
  version and not a reason to let someone's import break in silence.

  A separate `iceberg-zamboni-demo` distribution behind a `demo` extra was
  considered and rejected: a second distribution buys its own release cadence,
  version synchronisation and CI leg, which is disproportionate to a 212KB
  payload and a naming problem. The one argument for it -- that those 212KB stop
  shipping to people who never run the demo -- is recorded in #24 for if the
  fixtures grow.

- **Epics and stories are GitHub issues, not rows in a markdown file.** Ids came
  from us before -- `ZMBNI-1xx` per epic, assigned by hand -- and now they come
  from GitHub, tracked on project #23 with the `gh agile` extension.
  `docs/tasks.md` is `docs/tasks_historical.md`: frozen, hash-pinned by a test,
  and carrying a map from every migrated id to the issue it became, because 147
  commit messages cite those ids and cannot be rewritten. Four of its five tests
  are gone -- three are now covered by GitHub or by `gh agile validate`, and the
  story-count check died with the hand-written totals it existed to police. The
  fifth, FR traceability, was broadened to every document instead.
- **The PyPI development status is Beta**, not Alpha. Alpha understated where
  this is: the scope is delivered, every operation is verified against a live
  Lakekeeper and MinIO plus real Trino and Spark servers, and CI runs green on
  three Pythons. It is deliberately not Production/Stable -- that waits on the
  same two things `1.0.0` does, a maintenance cycle against a warehouse we did
  not build and a second user on the config schema. Takes effect on the next
  release; PyPI metadata cannot be edited in place.
- **CI tests every Python `pyproject.toml` claims**, not only the endpoints.
  The matrix was 3.11 and 3.13 -- the floor and the development pin -- while the
  classifiers promised 3.12 as well. Endpoints catch a 3.12-only *construct*,
  which fails on the 3.11 leg, but not a 3.12-only runtime difference, which
  passes both and breaks for whoever is on 3.12. The legs run in parallel, so
  the third costs no wall-clock. 507 tests pass there, so the claim was true --
  it simply had no evidence behind it.

- **What "Spark works" means is now stated per connection path.** The security
  review flagged `spark-lib`'s `pyspark>=3.5` floor as looser than the Connect
  client's `>=4.0`. Investigating inverted the conclusion: Spark 3.5 carries an
  *extended* LTS to **November 2027**, Iceberg still publishes
  `iceberg-spark-runtime-3.5` at 1.11.0, and `pyspark-client` did not exist
  before 4.0 -- so `spark-lib` with `--spark-master` is the **only** way to
  drive a Spark 3.5 cluster. Raising the floor would have removed that for no
  gain in verification, since CI covers Connect against 4.0.4 and the classic
  path is untested at every version.

  So the floor stays and the claim got fixed instead. README and
  [docs/user_guide.md](docs/user_guide.md) now carry a verified/best-effort
  table per path, and every Spark run logs the version it reached and how it
  connected -- once per run -- so a failure on an untested combination names
  the combination. ZMBNI-1818.

### Removed


- **`docs/upstream-0.12-upsert-regression.md`**, which both this file and the
  README linked. Its reproduction is now
  `test_upsert_on_a_transformed_partition_replaces_rather_than_duplicates`, which
  fails on a build exhibiting the bug and passes on one that does not — the
  question the document was answering in prose. The upstream issue
  ([#3758](https://github.com/apache/iceberg-python/issues/3758)) and its fix
  ([#3780](https://github.com/apache/iceberg-python/pull/3780), merged) remain the
  source of detail, and every reference now points at one of those or at the test.

  Two things the document carried that prose was the wrong home for are now
  assertions instead: the partition spec being *required* to reproduce is
  `test_the_upsert_defect_needs_a_partition_spec`, and which transforms are
  affected is measured per transform in the test's own docstring rather than
  characterised — an earlier draft said "any non-identity transform", and
  `truncate` is non-identity and correct.

### Fixed


- **The README claimed object stores nobody had run against.** It told an operator
  that IT provides the store "(MinIO on real disk, AWS S3, GCS)" and that the `s3`
  extra covers "S3, MinIO or GCS". Neither GCS nor AWS S3 proper has been run
  against — `docs/live-verification.md` records Lakekeeper + MinIO, now Silo — and
  the `s3` extra is not what makes a bucket work, since `s3` and `gs` both prefer
  `PyArrowFileIO`.

  Replaced with a **Verified against** table naming each store's real status,
  including the ones verified elsewhere (Garage, in ExperienceFlow's end-to-end
  ELT testing) and the ones simply not run against. `test_every_extra_the_readme_names_exists`
  now ties the install table to `pyproject.toml` in both directions. (ZMBNI-85)
- **`target_file_size_bytes` was ignored on the streaming write path.** PyIceberg's
  writer bin-packs a `RecordBatchReader` by the table property
  `write.target-file-size-bytes`, falling back to its own 512MB default — so the
  configured value was honoured when an unpartitioned chunked rewrite bin-packed
  locally and silently ignored when it delegated. One config key, two meanings,
  decided by whether a table happened to be partitioned. A 16MB target produced
  **1 output file one way and 14 the other** from identical input. The resolved
  target now travels with the metadata handed to the writer. (ZMBNI-16)

## [0.3.0] - 2026-08-13

What the first publication exposed: two defects only a real install could show,
and an extras list that had accumulated one entry doing nothing and two named the
wrong way round. A minor rather than a patch because of the third of those --
`spark` now means something different, and a version number is the only warning
anyone gets.

### Security

- **Pre-release security review run against `v0.2.0..HEAD`** — the first under
  [docs/releasing.md §3a](docs/releasing.md), now step 0 of the release
  checklist. All eight checks clean: no secret reaches a log, `repr`, exception
  or the process table; the credential-literal guard was mutation-verified; no
  destructive default moved; the reclaim invariants still abort (70 tests); every
  engine identifier is quoted; **zero** packages added, removed or version-changed
  in `uv.lock`; the wheel and sdist carry no `.env`, catalog, warehouse or data
  file; and the release workflow still publishes from OIDC with no stored secret.
  Two observations recorded rather than fixed, neither a blocker: GitHub Actions
  are pinned to movable tags rather than commit SHAs (ZMBNI-1817), and `spark-lib`
  still floors at `pyspark>=3.5` where the Connect client requires `>=4.0`.

### BREAKING

- **`spark` is now the Spark Connect *client*; the embedded library is
  `spark-lib`.** `spark` used to install `pyspark` -- 472MB and a JDK -- while
  the client hid behind `spark-connect`, a name nobody guessed. That was
  backwards: driving a Spark you already have is the common case, and it is what
  `trino` means for Trino, so the two engines now read the same way.

  **If you install `iceberg-zamboni[spark]`** and used a *local* session
  (`--spark-master local[*]`), switch to `iceberg-zamboni[spark-lib]`. If you
  used `--spark-remote`, you now get a 13MB install instead of a 472MB one and
  need no JDK.
  **If you install `iceberg-zamboni[spark-connect]`**, that extra is gone;
  use `spark`. ZMBNI-1816.

- **The `bucket` extra is removed.** It installed nothing: `pyiceberg[pyarrow]`
  is a hard dependency and already requires `pyiceberg-core`, so `[bucket]` only
  restated that with a looser floor. `pip install "iceberg-zamboni[bucket]"` now
  warns that the extra is unknown and installs exactly what it installed before.
  **Nothing to do** -- bucket-partitioned tables work with a plain install, and
  did before. Removing it rather than keeping an inert alias, because an extra
  that exists and does nothing reads as a warning about a problem that does not
  exist. ZMBNI-1815.

### Fixed

- **The demo told new users to run a command that does not exist.** Every "run X
  next" hint said `./bin/zamboni-demo` -- right in a checkout, a dead path for
  anyone who ran `pipx install iceberg-zamboni`, where the command is bare
  `zamboni-demo`. `invocation()` now keys off the same signal as
  `default_inputs()` and `default_root()`, and an AST guard fails the build if
  the literal comes back. ZMBNI-1811.

- **The PyPI page had no links.** `[project.urls]` was missing entirely, so the
  published page carried no Homepage, Repository, Changelog or Issues link. That
  is worse here than usual: the docs are deliberately not shipped in the wheel
  and are meant to be reached by link. PyPI metadata cannot be edited in place,
  which is what makes this a release rather than a commit. ZMBNI-1812.

### Changed

- **The PyIceberg 0.12 position is stated without drama.** 0.11.x is fully
  supported; each 0.12 release candidate is tested as it appears; issues found
  are reported upstream and fixed, and the supported range widens when a release
  passes. The README section is now a one-row table naming the open issue
  ([#3758](https://github.com/apache/iceberg-python/issues/3758)) and its fix
  ([#3780](https://github.com/apache/iceberg-python/pull/3780)) -- those are the
  source of detail, so there is no second copy here to keep current.
  ZMBNI-1814.

- **The README said Spark was "declared but not yet implemented".** It has been
  implemented and verified against Spark 4.0.4 since 0.2.0. Spark now leads the
  engine section with its own install line, ahead of Trino, because it is the
  more complete of the two -- it Z-orders and Trino does not.

## [0.2.0] - 2026-08-12

**Three engines, a public repository, and a name on PyPI.** 0.1.0 was one
maintainer over PyIceberg; this release adds Trino and Spark behind the same six
verbs and the same `table-config.json`, makes `maintenance` a single command an
application or a cron line can call, and publishes the result as
`iceberg-zamboni`.

The BREAKING section is longer than a 0.x release would like. Most of it is the
cost of those two facts: a distribution rename, a config file that had to grow a
namespace level once one warehouse was not the only shape, and secrets that had
to stop being command-line flags before anyone's process table was the place
they lived. Each entry names what to change.

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

- **`bin/zamboni` and `bin/demo` were broken by the distribution rename.** They
  embed the project as a path dependency, and uv refused it: *"Package metadata
  name `iceberg-zamboni` does not match given name `zamboni`"*. Testing the wheel
  had not caught it, because the wheel is not what `bin/` builds.
  `scripts/build-executable.py` duplicated `[project.scripts]` and hardcoded the
  distribution name in `[tool.uv.sources]`; both now come from
  `pyproject.toml`. `bin/demo` is consequently **`bin/zamboni-demo`**, matching
  the console script a `pip install` puts on your PATH.

- **`zamboni-demo` shipped as a command that could not run.** It resolved the
  demo's input data relative to the source tree, so an installed copy died on an
  unhandled `FileNotFoundError` pointing inside `site-packages`, and no data was
  in the wheel to find. The 17 input files (212 KB) now ship, and reads are
  separated from writes so nothing is ever written into `site-packages` — the
  demo writes to `./zamboni-demo/` when run from an install, and keeps using
  `data/healthims/` in a checkout. `pipx install "iceberg-zamboni[sql]" &&
  zamboni-demo next-day` now works with no clone.

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

- **The distribution is now `iceberg-zamboni`.** The import is still `zamboni`
  and the repository is still `Zamboni`; only the PyPI name changes, because
  `zamboni` there is a dormant registration by an unrelated project. Install
  with `pip install "iceberg-zamboni[s3,sql]"`.

- **The `demo` console script is now `zamboni-demo`.** Installing the package
  put a bare `demo` on the user's PATH, which no library should claim globally —
  and the collision would have been silent, with whichever package installed
  last winning. `./bin/demo` inside the repository is unchanged.

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
  [apache/iceberg-python#3758](https://github.com/apache/iceberg-python/issues/3758).
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
