# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Environment — always `uv`, never bare `pip`/`python`. `uv sync` resolves from
`uv.lock` alone; CI runs `uv sync --frozen`, so a dependency edit that forgot to
re-lock cannot reach `main`.

```bash
uv sync                                          # build .venv from uv.lock (dev group included)
uv run pre-commit install
uv run pytest                                    # full suite, no Docker (test_dev_stack.py skips)
uv run pytest --ignore=tests/test_dev_stack.py   # what CI's `test` job runs
uv run pytest tests/test_orphans.py::test_name   # one test
uv run pytest -m spark                           # live Spark tests (need the dev stack up)
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
uv run mypy                                      # config declares its own file set; no arguments
uv run pre-commit run --all-files
```

Dev stack, needed for anything touching storage, credentials or an engine:

```bash
cd dev-stack && cp .env.sample .env
docker compose up -d --wait                      # Lakekeeper + Postgres + MinIO
docker compose --profile trino up -d trino       # add Trino; `--profile spark` for Spark Connect
uv run bootstrap.py                              # create the warehouse
ZAMBONI_REQUIRE_DEV_STACK=1 uv run pytest tests/test_dev_stack.py -v
```

`ZAMBONI_REQUIRE_DEV_STACK=1` turns "cannot reach the stack" from a skip into a
failure. Set it whenever you mean to prove the stack was exercised — the
fixtures skip on a closed port, so a green run without it can mean nothing ran.

Regenerate the PEP 723 executables after any `uv lock` / `uv sync --upgrade`;
CI fails if `bin/` is stale:

```bash
uv run scripts/build-executable.py               # writes bin/zamboni and bin/zamboni-demo
./bin/zamboni doctor                             # also works from outside the repo
uv run zamboni-demo next-day                     # five-day HIMS demo on a local SQLite catalog
```

Probe work against unreleased PyIceberg (see "Capability probes" below):

```bash
uv pip install -e ../iceberg-python && uv run zamboni doctor && uv run pytest
uv sync                                          # back to the pinned line
```

## Architecture

Two packages under `src/`: `zamboni` (the tool) and `himsdemo` (the
`zamboni-demo` narrative, a separate console script). The distribution is
`iceberg-zamboni`; the import is `zamboni`.

**`CatalogSession` (`session.py`)** owns both halves of a run — the PyIceberg
catalog that reads metadata and commits, and the DuckDB connection used to sort
and spill. DuckDB never attaches the Iceberg catalog; data reaches it as an
Arrow stream from PyIceberg's scan, which is why the local SQL catalog and a
Lakekeeper REST catalog behave identically.

**`table-config.json` (`tableconfig.py`)** is the authored surface: declarative
layout and retention per table, `defaults` shallow-merged with a table's own
block. It never describes actions — the tool derives the work. `catalog_import.py`
generates one from a Meltano/Singer catalog.

**The maintainer seam (`maintainers/`)** is the central abstraction. Six
operations (`Operation`: compact, expire, remove-orphans,
remove-dangling-deletes, rewrite-manifests, apply-properties) × three engines
(`local`, `trino`, `spark`), joined by a registry (`register`/`get`/`available`,
built-ins imported by `_load_builtins()`). Engines are compared by *declaration*,
not prose: `MaintainerCapabilities` must declare every operation (construction
fails otherwise), `Support` is three-valued, and a non-FULL `OperationSupport`
must carry a `limitations` string. `LayoutFeature` covers the layout capabilities
that are settings rather than verbs (zorder, sort, partition-evolution,
target-file-size) so `zamboni table-config summary` can derive its warnings
instead of hardcoding them. Ask the declarations (`engines_lacking`,
`capabilities().can(...)`) rather than writing a second copy of the fact.

Trino and Spark maintainers keep **statement generation pure and separate from
execution**, so every emitted `ALTER TABLE … EXECUTE` / `CALL` is asserted
without a server or a JVM. `local.py` translates and delegates to the operation
modules below; its distinguishing property is not a capability but a guarantee —
everything previews, and the reclaim operations abort rather than delete.

**`maintenance.maintain()`** is the one run loop, shared by the CLI and by
library callers, so an integrator never re-derives `RUNBOOK_ORDER`, the
`fulfilled_by` skip, or which exceptions are refusals. It returns a
`MaintenanceReport` whose `exit_code` is the *worst* outcome and is the same
number the CLI exits with. `RUNBOOK_ORDER` is duplicated as strings in
`settings.DEFAULT_OPERATIONS` — keep them in step; the gaps between operations
are load-bearing (docs/runbook-dev.md §1).

**Compaction pipeline:** `profile.py` (read manifests directly, not
`Table.inspect`, because the committer needs the real `DataFile` objects) →
`planner.py` (group live files by `(spec_id, partition)`) → `backends/`
(`duckdb_arrow.py`: PyIceberg scan reads, DuckDB sorts/z-orders, PyIceberg's
`_dataframe_to_data_files` writes) → `committer.py` (an Iceberg `replace`
snapshot via `_ReplaceFiles`). `compactor.py` is the entry point; `zorder.py`
builds Morton order as DuckDB SQL because no layer below has any notion of it.

**Sibling operations**, each its own module and each explaining what PyIceberg
will not do: `expire.py` (the spec's retention algorithm plus file deletion),
`orphans.py`, `deletes.py`, `manifests.py`, `evolution.py`, `properties.py`.
`reachable.py` is the single referenced-file set both reclaim verbs subtract
from — six categories, because Iceberg points at files from six places. A
category omitted there is live data destroyed; treat it as the most
safety-critical module in the repo.

**`settings.py`** resolves the operator config: flag > `ZAMBONI_*` env var >
`./zamboni.yml` > `$ZAMBONI_ROOT/zamboni.yml` > built-in default. `zamboni.yml`
is committable; credentials live in `.env`. `ENGINE_SETTINGS` deliberately
rejects password/token/secret keys by name.

**`cli.py`** is argparse, grouped by how much a verb changes: read-only
(`doctor`, `engines`, `describe`, `plan`, `validate-config`, `from-catalog`,
`table-config …`), then the mutating verbs plus `maintenance`.

## Invariants that constrain edits

- **Without `--yes`, nothing is committed.** Every mutating verb previews and
  says so unconditionally, including on a no-op run. Where an engine cannot
  preview an operation, the rule is kept by *refusing* (`check_consent` raises
  `PreviewUnavailable`) — never by executing, and never by printing a dry-run
  notice over an engine that is about to delete.
- **Exit codes are a contract**: 0 success, 2 usage/config/consent, 3 the table
  is *blocked* (a refusal, not a failure), 4 a safety check aborted and nothing
  was deleted. Exit 4 also stops the rest of *that table*, because everything
  after it reads state we just said we distrust. Three shapes of
  not-a-failure — disabled, unsupported, fulfilled by another operation — all
  exit 0 and ran nothing.
- **Capability probes, not version checks.** `capabilities.py` asks the
  *installed* PyIceberg what it can do — signatures, source inspection, and one
  behavioural probe that actually performs an overwrite on a transformed
  partition. Never replace a probe with a version comparison, and never make one
  name-based: `_build_delete_files_partition_predicate` exists on the build that
  corrupts data, so its name proves nothing. Each probe's unknown-answer default
  is chosen per probe for the safe direction and says so in a comment.
- **Format versions:** V1 refused, V2 full, V3 metadata-only. Equality deletes
  block compaction.
- Private PyIceberg APIs in use are guarded by
  `committer.assert_supported_pyiceberg()`; `pyiceberg` is capped `<0.12` because
  0.12 corrupts partitioned upserts (docs/upstream-0.12-upsert-regression.md).
- Removed files are passed as the `DataFile` objects read from the manifests,
  never reconstructed — `_OverwriteFiles._existing_manifests` matches by
  identity.

## Conventions (CONTRIBUTING.md is the full text; these are the ones that bite)

1. **Verify a claim before making it.** Any comment, docstring, commit message
   or doc asserting behaviour means someone ran it and looked. Where a number
   appears, the comment says how it was obtained; where a behaviour is
   attributed upstream, it names the function. "assumed", "not verified",
   "unknown" are acceptable words — an unchecked assertion is not, and is
   grounds for rejecting a change.
2. **Tests assert the property, not the implementation.** Assert the call shape,
   not a megabyte figure. Derive the expectation from the declarations rather
   than comparing to a literal string. Make the negative case fail loudly.
3. **Docs are checked mechanically.** `tests/test_docs.py` enforces that cited
   tests exist, relative links resolve, every documented config sample loads
   against the current schema, every `CompactionConfig` field is mentioned in the
   user guide, no doc carries a credential-shaped literal, and every GitHub
   Action is pinned to a SHA that dependabot watches. It also validates the
   `docs/tasks.md` backlog (statuses, completion dates, epic/story consistency,
   `FR-` references). If you add a checkable claim, add the check.
4. **Workarounds explain themselves in place** — `_surviving_manifests` in
   `evolution.py`, `_guard_anywhere_in_scan_planning` in `capabilities.py`, the
   `fs.s3.impl` mapping in the compose file. Several things here look like
   removable dead code and are not. If you remove something as dead, the commit
   message says how you established that.
5. Every file under `src/` and `scripts/` carries `# SPDX-License-Identifier:
   Apache-2.0` (pre-commit hook, deliberately not the full ASF header).
6. **A changed default is a breaking change** for this tool: a lowered
   `older_than_days` deletes files on the next nightly run with no signature
   moved. Those need a `BREAKING` or `SAFETY` changelog entry — see
   docs/releasing.md.
7. Commit messages are long and explain *why*: what was wrong, how it was found,
   what was rejected, what it cost. Reference the `ZMBNI-` story; adding one to
   `docs/tasks.md` is part of a change that has none.
8. The ruff rev in `.pre-commit-config.yaml` must equal the ruff version in
   `uv.lock` — CI checks it. Bump both together.

## Docs worth reading before changing behaviour

`docs/user_guide.md` (the user-facing reference: four ways to run it, the
engine-capability table, secrets, memory numbers) · `docs/design.md` (why
maintenance is necessary and how the pieces fit) · `docs/engine-comparison.md`
(where the seam is and why) · `docs/runbook.md` and `docs/runbook-dev.md`
(operator triage; running each step by hand) · `docs/releasing.md` (what a
release may change) · `docs/plan.md` and `docs/tasks.md` (requirements and
backlog, both test-enforced).
