# Contributing

The conventions below are currently visible only by reading the commit history.
They are written down here because they are what makes this codebase's claims
trustworthy, and a drive-by change that ignores them costs more to review than
it saves.

None of this is about style — `ruff` handles that and disagreeing with it is not
interesting. It is about what counts as *done*.

## Getting set up

```bash
uv sync                 # builds .venv from uv.lock; the Python is pinned too
uv run pre-commit install

uv run pytest                                   # the suite; no Docker needed
uv run pytest --ignore=tests/test_dev_stack.py  # explicitly skip the stack tests
```

`uv sync` rather than `pip install -e .` is the whole point of this project's
environment: it resolves from `uv.lock` alone and never against whatever happens
to be installed globally. CI runs `uv sync --frozen`, which fails if the lockfile
is stale against `pyproject.toml` — so a dependency edit that forgot to re-lock
cannot reach `main`.

For anything touching storage, credentials or an engine, bring up the dev stack:

```bash
cd dev-stack && cp .env.sample .env
docker compose up -d --wait && uv run bootstrap.py
uv run pytest tests/test_dev_stack.py
```

[docs/runbook-dev.md](docs/runbook-dev.md) covers running each maintenance step
by hand; [docs/user_guide.md](docs/user_guide.md) is the user-facing reference
and the fastest way to understand what the tool is for.

## The five rules

### 1. Verify a claim before making it

The single most important convention here. If a comment, a docstring, a commit
message or a document asserts something about behaviour — this engine refuses
that, this path bounds memory, that upstream function does this — then someone
ran it and looked.

This is not pedantry. Three of the most useful findings in this repository came
from checking a claim that everyone, including the person who wrote it, believed:

- `MemoryMode.CHUNKED`'s docstring said peak memory was "roughly one output
  file". Measured, it grew linearly with the rewrite group and was
  indistinguishable from the in-memory path.
- The Spark maintainer's `older_than` was said to be short by a round trip.
  Measured against a live session, the real cause was the session timezone and
  the error was four hours, not seconds.
- `--memory-budget-bytes` was documented as defaulting to 256MiB. The flag
  hardcoded 1GiB, so the default reached Python callers and not the CLI.

Where a number appears in a comment, the comment says how it was obtained. Where
a behaviour is attributed to an upstream library, the reference names the
function. If you cannot check something, say so in the text rather than
asserting it: "assumed", "not verified", "unknown" are all acceptable words.

### 2. A test asserts the property, not the implementation

Prefer an assertion that survives a refactor and fails on a regression. Some
patterns this codebase uses deliberately:

- **Assert the call shape, not the memory figure.** `read_ahead_bytes` is
  covered by asserting one task per read call and a concurrency ceiling, not by
  asserting a number of megabytes — a memory assertion is a flaky assertion.
- **Derive the expectation.** The config summary's "not available on: trino"
  warning is tested by asking the capability declarations which engines lack the
  feature, then granting Trino that feature at runtime and asserting the warning
  disappears. A test comparing against the literal string would keep passing
  after Trino gained Z-order.
- **Make the negative case fail loudly.** The dev-stack fixtures skip when the
  stack is down, so CI asserts *no skips* — a green tick with nothing tested is
  the failure mode those tests exist to prevent elsewhere.

`ruff` lints `tests/` alongside `src/`, with `SLF001` ignored there so test code
can reach into private helpers on purpose. `mypy` does not run over tests, which
is deliberate — they monkeypatch types and the noise would train people to
ignore the checker.

### 3. Test against both PyIceberg lines when you touch the probes

`src/zamboni/capabilities.py` decides what this tool will attempt by *probing
the installed PyIceberg* — asking whether a function exists, what a signature
contains — rather than comparing version numbers. That is what lets the same
release behave correctly across a release boundary.

If your change touches a probe, or depends on one, exercise it against both the
pinned release and a checkout of PyIceberg `main`:

```bash
uv pip install -e ../iceberg-python     # a checkout of apache/iceberg-python
uv run zamboni doctor                   # what does this build support?
pytest
uv sync                                 # back to the pinned line
```

The probes have safe-direction defaults for the case where source is not
inspectable, and each says which direction is safe *for that probe* — they are
not the same. Read the comment before changing one.

### 4. Review before committing, and act on what it finds

The working loop is **develop → test → review → revise → re-test**, and repeat
the last three if the review produces findings. The review step is not
self-approval: read the diff as though someone else wrote it and you are looking
for the thing that will be embarrassing later.

It works. An independent review of the Spark maintainer produced five findings,
all real, one of which sent file-deleting operations at a *different table* when
an identifier contained a backtick.

When you fix something a review found, the commit message says what was wrong
and why it happened — not "address review comments".

### 5. Never let a document assert something the code does not do

Documentation here is checked mechanically where it can be:

- `test_every_cited_test_exists` — a doc naming a test that does not exist reads
  as verified coverage while proving nothing.
- `test_doc_links_resolve` — relative links must point at files that exist.
- `test_the_documented_configurations_are_valid` — every whole-document config
  sample in the user guide is loaded against the current schema. The first
  event-data example used the Python attribute names where the file wants
  `from`/`to`; this is why that was caught before a reader copied it.
- `test_the_guide_documents_every_run_control` — a new `CompactionConfig` field
  with no mention in the guide fails the suite.
- `test_every_referenced_fr_exists_in_the_plan` — any document citing an
  `FR-` id that plan.md does not declare fails the suite. It reads every
  document, so a new plan doc is covered the day it is written.
- `test_the_historical_backlog_is_frozen` — `docs/tasks_historical.md` is
  hash-pinned. It is the archive of the ZMBNI backlog, not a tracker.

If you add a claim that could be checked mechanically, add the check.

## Commit messages

Long, and explaining *why*. The house style is: what was wrong, how it was
found, what was rejected and why, and what it cost. A reader six months later
should be able to reconstruct the reasoning without the conversation that
produced it.

Reference the GitHub issue: `#123`, or `ZMBNI-123` — the key form is accepted
anywhere an issue number is, and `gh agile` resolves it. If your change does
not fit an open issue, filing one is part of the change:

```bash
gh agile story "What it is" --epic <n> --intent "..." --acceptance "..."
```

Epics and stories live on [board #23](https://github.com/users/paulcaron16k/projects/23).
**Ids are GitHub's now.** Older
commits cite hand-assigned `ZMBNI-` ids from when the backlog was a markdown
file; [docs/tasks_historical.md](docs/tasks_historical.md) is that file, frozen,
and its header maps every id that moved to the issue it became.

## Non-obvious workarounds carry their reason

Several things in this codebase look like removable dead code and are not. The
convention is that a workaround explains itself in place, with an issue link
where one exists. Three examples worth reading before changing anything nearby:

- `_surviving_manifests` in `evolution.py`, which replaces upstream's pruning
  `_existing_manifests` and exists because that pruning double-counts rows;
- `_guard_anywhere_in_scan_planning` in `capabilities.py`, which searches a
  whole module rather than one function, because the first version inspected one
  function and reported a guard as absent after upstream extracted the planner;
- the `fs.s3.impl` mapping in `dev-stack/docker-compose.yaml`, without which
  exactly one of the six operations fails while the other five pass.

If you find a workaround that does not explain itself, that is a bug worth
fixing.

The same applies in reverse: if you remove something as dead, say in the commit
message how you established it was dead.

## Licence and provenance

Apache-2.0. By contributing you agree your contribution is licensed under it;
there is no CLA.

Every file under `src/` and `scripts/` carries a one-line SPDX tag:

```python
# SPDX-License-Identifier: Apache-2.0
```

and a pre-commit hook fails if a new one does not. **This is deliberately not
the full Apache header.** The choice was between the nine-line ASF boilerplate,
nothing at all, and this:

| | cost | machine-readable |
|---|---|---|
| Full ASF header | 520 lines, 4.5% of the codebase, 76% of the smallest module | yes |
| Nothing | 0 | no — provenance lives only in `LICENSE` and wheel metadata |
| **SPDX tag** | **40 lines, 0.3%** | **yes** |

The benefit anyone actually wants from headers is provenance that survives a
file being copied out of the repository, and the SPDX tag delivers exactly that
at a fifteenth of the cost. The licence's own appendix *recommends* the full
notice rather than requiring it, and this is a single-licence repository that
vendors nothing.

## What gets a change rejected

- A claim in a comment or document that nobody checked.
- A test that would pass if the behaviour it names were removed.
- A new default that changes what gets deleted, without a `BREAKING` or `SAFETY`
  changelog entry — see [docs/releasing.md](docs/releasing.md), which defines
  those terms for this tool specifically. A lowered `older_than_days` deletes
  files on the next nightly run with no signature moved.
- Silently narrowing scope. If part of a change turns out to be blocked, say
  which part and why rather than shipping the rest as though it were complete.

## Security issues

Do not open a public issue. See [SECURITY.md](SECURITY.md) — data-loss reports
are the priority category, and they do not need to be attacker-triggerable to
count.
