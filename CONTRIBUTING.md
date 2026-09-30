# Contributing

The conventions below are currently visible only by reading the commit history.
They are written down here because they are what makes this codebase's claims
trustworthy, and a drive-by change that ignores them costs more to review than
it saves.

None of this is about style — `ruff` handles that and disagreeing with it is not
interesting. It is about what counts as *done*.

## Getting set up

```bash
make doctor             # uv, the venv and its pip, the matrix Pythons, docker, the subnet
make                    # every target, and which dev stack is currently up
make venv               # builds .venv from uv.lock; the Python is pinned, and pip is inside it
uv run pre-commit install

make test               # the suite; no Docker needed
make ci                 # lint, suite, executables, version caps -- all of CI bar the stacks
```

`uv sync` rather than `pip install -e .` is the whole point of this project's
environment: it resolves from `uv.lock` alone and never against whatever happens
to be installed globally. CI runs `uv sync --frozen`, which fails if the lockfile
is stale against `pyproject.toml` — so a dependency edit that forgot to re-lock
cannot reach `main`.

For anything touching storage, credentials or an engine, bring up the dev stack. One
target per engine, and the test target refuses the wrong stack rather than skipping
against it:

```bash
make local-stack-start && make test-local     # Lakekeeper + Postgres + MinIO
make trino-stack-start && make test-trino     # ... plus Trino
make spark-stack-start && make test-spark     # ... plus Spark Connect
make stack-status                             # what is up right now
```

[ONBOARDING.md](ONBOARDING.md) is the worked version of all of this, with a
checkpoint after each step and the reading order that makes a first change
review in one pass. [docs/runbook-dev.md](docs/runbook-dev.md) covers running
each maintenance step by hand; [docs/user_guide.md](docs/user_guide.md) is the
user-facing reference and the fastest way to understand what the tool is for.

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
uv pip install -e ../iceberg-python      # a checkout of apache/iceberg-python
.venv/bin/zamboni doctor                # what does this build support?
.venv/bin/python -m pytest
uv sync                                 # back to the pinned line
```

**Through `.venv/bin/python`, not `uv run`.** `uv run` re-syncs the environment
from `uv.lock` before running, and it does not merely ignore the install you just
made -- it **reverts** it, so every later command is wrong too. Measured when the
lock pinned 0.11.1, so the numbers below are that era's; the behaviour is not:

```
uv pip install -e ../iceberg-python
.venv/bin/python -c "...version..."   -> 0.12.0    # the install worked
uv run python -c "...version..."      -> 0.11.1    # re-synced to the lock
.venv/bin/python -c "...version..."   -> 0.11.1    # and the checkout is gone
```

So a single `uv run` anywhere in the loop silently returns you to the pinned
release, and the suite then passes against the wrong build while you believe you
are testing the checkout.

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

## Issue management

Work is tracked on [board #23](https://github.com/users/paulcaron16k/projects/23),
driven from the command line by the `gh agile` extension. The board is the
record of what was done and when — a board that lags is worse than no board,
because it reads as current.

### Installing and upgrading the extension

```bash
gh extension install paulcaron16k/gh-agile     # once
gh extension upgrade agile                     # thereafter
gh extension list                              # confirm: gh agile  paulcaron16k/gh-agile  <sha>
```

It needs a `gh` already authenticated against the repository and the project
(`gh auth status`), and a token with `project` scope — `gh auth refresh -s project`
if a command fails on permissions rather than on arguments.

The board's own configuration lives in the profile the extension keeps; you do
not need to create anything. `gh agile key` lists the repository keys, which is
what makes `ZMBNI-123` work anywhere an issue number does.

### Picking up an issue

Choose from the backlog, then set three things — `move` does two of them:

```bash
gh agile board show backlog                                        # pick one
gh agile set --issue <n> --field Sprint --value "ZMBNI Sprint 4"   # a) sprint
gh agile move <n> --status "In Progress" --add-assignee            # b) + c) assignee, status
```

`--add-assignee` with no value means you, and moving to In Progress claims an
unassigned issue anyway; pass it explicitly when the work belongs to someone
else.

**Step (a) needs a current sprint to exist.** `gh agile sprint list` shows them;
`gh agile sprint ensure` creates the current one plus a few ahead. This is worth
checking before starting rather than after finishing: `gh agile sprint backfill`
derives the `sprint:` label from the Sprint *field*, so an issue that was worked
while no sprint existed has nothing to backfill from and needs the field set by
hand.

Optionally confirm the issue moved, which is worth doing after a batch:

```bash
gh agile board show backlog    # it should be gone from here
gh agile board show sprint     # and present here
```

### Finishing an issue

After the pull request is approved and the branch merged — **not when the code
is written**:

```bash
gh agile move <n> --status Done --close --evidence "Delivered in PR #<pr>. ..."
```

`--evidence` is required for Done and is written into the issue's Evidence
section. Say what shipped and how it was verified: the PR, what the tests cover,
what was measured. It is *silently discarded* for any other status, so record a
Blocked or In Review rationale with `gh issue comment` instead.

Then re-read the board, because several of these commands report success
whether or not anything changed:

```bash
gh agile board show backlog    # flags "closed, but Status is not Done", with the fix command
gh agile validate              # missing sprint: labels, epics stamped into a sprint, and more
```

**Let the hook remind you.** A merge is exactly when this is easiest to forget,
so install the post-merge hook once per clone:

```bash
gh agile hooks                             # installs post-merge into this clone
gh agile unfinished                        # what the hook runs: ORIG_HEAD..HEAD by default
gh agile unfinished --range <a>..<b>       # or any range, after the fact
```

It names each issue the merge closed whose board state has not caught up, says
what is missing, and prints the command to finish it. Run it against an older
merge and it will find whatever was missed then, too.

### Three things that catch everyone

**Epics and initiatives are never in a sprint.** They span them, so they carry
no Sprint value and no `sprint:` label. Strays clear with
`gh agile sprint unstamp` (`--dry-run` first).

**Board state is written by hand here, and the project automations that used
to write it have been turned off.** Two were doing damage and are now disabled:

| Workflow | Why it is off |
|---|---|
| `Pull request linked to issue` | set a linked issue to *In Progress* when a PR naming `Closes #n` was opened — after `Item closed` had set Done, so an issue closed before its PR was raised finished closed but **not** Done. Ten issues drifted that way in September |
| `Auto-close issue` | closed an issue when its Status became Done, racing `gh agile move --not-planned` and winning: closed as COMPLETED, and a close reason cannot be changed afterwards, so a "won't do" was recorded permanently as done |

`Item closed` is still on and is the one left to decide. It writes **Status and
nothing else** — no Evidence, no closing `sprint:` label, no
completed-or-not-planned — so an issue it touches reads Done while being
unfinished, and the gap is invisible except to `gh agile validate`. With it off,
a closed issue stays visibly unfinished on the backlog board until
`gh agile move --status Done --evidence` writes all three. `gh agile validate`
reports the current state of all of these, so check it rather than this table if
they have moved again.

None of these are reachable from the command line; they are switches in the
project's settings.

## Non-obvious workarounds carry their reason

Several things in this codebase look like removable dead code and are not. The
convention is that a workaround explains itself in place, with an issue link
where one exists. Two examples worth reading before changing anything nearby:

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
