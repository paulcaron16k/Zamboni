# Onboarding — Zamboni

Zamboni maintains Apache Iceberg tables: compaction, Z-order clustering, partition
evolution, dangling-delete removal, manifest rewriting, snapshot expiry and orphan-file
removal — in one Python process, without needing Trino or Spark, and able to drive either
when you have one.

**It deletes files.** That single fact shapes everything below. Read [the rules](#the-five-rules-in-one-screen)
before your first pull request.

This guide is meant to be *worked through*, not read. Run the commands; the checkpoints say
what you should see.

---

## 1. Get the machine ready

```bash
git clone https://github.com/paulcaron16k/Zamboni && cd Zamboni
make doctor
```

`make doctor` checks the machine rather than the code: `uv`, the virtualenv and whether pip
is inside it, the Pythons the test matrix needs, docker and its daemon, the pinned subnet the
dev stack claims, the pre-commit hook, and `gh agile` for the issue board. It ends by running
`zamboni doctor`, which is a different question with the same word — what the *installed
PyIceberg* can do.

**Checkpoint:** `0 blocker(s)`. Warnings are fine to start with; each one names its fix.

The only hard requirement is [uv](https://docs.astral.sh/uv/getting-started/installation/).
Docker is needed only for the dev-stack targets in step 4.

```bash
make venv                    # .venv from uv.lock: the package, the dev group, and pip
uv run pre-commit install
```

Two things about the environment, both deliberate:

- **Never `pip install -e .`, and never a bare `python`.** Every make target runs through
  `uv`, which builds `.venv` from `uv.lock` alone. That is the point: a run that resolved
  against whatever happened to be installed globally would be testing a different program.
  To add a dependency, `uv add` — it updates `uv.lock`, and CI's `uv sync --frozen` fails on
  a stale lockfile, so a dependency edit that forgot to re-lock cannot reach `main`.
- **pip is installed inside the venv on purpose.** `uv sync` does not put it there, and the
  resulting failure is silent and off-target: with `.venv` activated but no pip in it,
  `pip install x` runs whichever pip is next on `PATH` and installs into *that* interpreter.
  On one machine here that was a Python 3.10 pip against a 3.13 project.

---

## 2. Run the tests

```bash
make            # every target, grouped, each naming the CI job it corresponds to
make test       # the suite, no Docker, about four minutes
```

**Checkpoint:** everything passes and **nothing skips** — 522 tests at the time of writing.
The count grows, so a different number is fine; a skip or a failure is not.

The organising idea is that **every CI job has a make target**, so "did I break CI" is
answerable before pushing:

| CI job | Locally | Needs |
|---|---|---|
| `lint` | `make lint` | nothing |
| `test` | `make test` | nothing |
| `test`, all three Pythons | `make test-matrix` | nothing |
| `executables` | `make test-executables` | nothing |
| `dev-stack` | `make test-local`, `make test-demo` | the local stack |
| `dev-stack`, Trino leg | `make test-trino` | Trino in the stack |
| `spark` | `make test-spark` | Spark in the stack |

`make ci` runs everything that needs no containers. While editing, the tighter loops are
`make format` (writes the fix), `make test-docs` (documentation invariants, under a second),
and `make typecheck`.

---

## 3. Read four files, in this order

Do this before writing code. Roughly an hour, and it is the difference between a change that
reviews in one pass and one that does not.

1. **[README.md](README.md)** — the *why*: the evidence behind each design decision, and the
   four installation shapes from a laptop to a Spark cluster.
2. **[docs/user_guide.md](docs/user_guide.md)** — what the tool is for, from the operator's
   side. The fastest way to understand the problem being solved.
3. **[docs/design.md](docs/design.md)** §2 — how Iceberg stores a table, and why maintenance
   is unavoidable rather than housekeeping. Every operation in the codebase follows from
   this section.
4. **[CONTRIBUTING.md](CONTRIBUTING.md)** — what counts as *done* here. Summarised below,
   but read the originals: each rule carries the specific bug that earned it.

Then, for the code itself, [CLAUDE.md](CLAUDE.md) is the fastest map of the architecture —
the maintainer seam, the compaction pipeline, and the invariants that constrain edits.

---

## 4. Bring up real infrastructure

Unit tests run against a SQLite catalog in a temp directory. Anything touching storage,
credentials or an engine needs the dev stack: Lakekeeper, Postgres and MinIO, with each
engine as a compose profile on top.

```bash
make local-stack-start && make test-local     # the base stack
make test-demo                                # five days of ingestion, then maintain it

make trino-stack-start && make test-trino     # ... plus Trino
make spark-stack-start && make test-spark     # ... plus Spark Connect

make stack-status                             # local, local+trino, local+spark, +both, none
make stack-stop                               # stop everything, keep the warehouse
```

Adding an engine does not disturb what is already running.

**Why a test target refuses instead of skipping.** The fixtures in `tests/test_dev_stack.py`
skip when a port is closed, by design — so a green run against a stack that never started
would mean nothing was tested. Each target therefore checks the engine is really present,
sets `ZAMBONI_REQUIRE_DEV_STACK=1`, selects by marker, and fails if anything skipped.
`make test-local` additionally requires that *no* engine is running, because one left over
from an earlier session changes what those tests exercise.

---

## 5. The five rules, in one screen

From [CONTRIBUTING.md](CONTRIBUTING.md). These are what make this codebase's claims
trustworthy; a change that ignores them costs more to review than it saves.

1. **Verify a claim before making it.** If a comment, docstring, commit message or document
   asserts something about behaviour, someone ran it and looked. Where a number appears, the
   comment says how it was obtained. "assumed", "not verified" and "unknown" are acceptable
   words; an unchecked assertion is not.
2. **A test asserts the property, not the implementation.** Assert the call shape, not a
   megabyte figure. Derive the expectation from the declarations rather than comparing to a
   literal. Make the negative case fail loudly.
3. **Test against both PyIceberg lines when you touch the probes.** `capabilities.py` decides
   what the tool attempts by *probing the installed PyIceberg*, never by comparing version
   numbers. Each probe's unknown-answer default is chosen for the safe direction, per probe.
4. **Review before committing, and act on what it finds.** develop → test → review → revise →
   re-test. One such review of the Spark maintainer produced five real findings, one of which
   sent file-deleting operations at a *different table* when an identifier held a backtick.
5. **Never let a document assert something the code does not do.** The doc invariants are
   mechanically checked — cited tests must exist, links must resolve, every config sample in
   the guide is loaded against the live schema. If you add a checkable claim, add the check.

And the one that is specific to this tool: **a changed default is a breaking change.**
Lowering `older_than_days` deletes files on the next nightly run with no signature moved, so
those defaults are public surface and need a `BREAKING` or `SAFETY` changelog entry.

---

## 6. Where the work is tracked

Epics and stories are GitHub issues on [project #23](https://github.com/users/paulcaron16k/projects/23),
managed with the [`gh agile`](https://github.com/paulcaron16k/gh-agile) extension:

```bash
gh extension install paulcaron16k/gh-agile
gh agile validate                            # do the board's conventions hold?
gh agile story "What it is" --epic <n> --intent "..." --acceptance "..."
```

That extension's repository is **private**, so the install works only if you have been given
access. Without it, `gh issue create` and the board's web UI do the same job by hand; ask for
access if you will be filing issues regularly.

Commit messages are long and explain *why*: what was wrong, how it was found, what was
rejected, what it cost. Reference the issue (`#123`, or the `ZMBNI-123` key form). If your
change fits no open issue, filing one is part of the change.

Older commits cite hand-assigned `ZMBNI-` ids from when the backlog was a markdown file.
[docs/tasks_historical.md](docs/tasks_historical.md) is that file, frozen and hash-pinned,
with a map from every migrated id to the issue it became.

---

## 7. Your first change

A good first task is one where the mechanical checks teach you the conventions:

1. Pick an issue from [the board](https://github.com/users/paulcaron16k/projects/23), or file
   one.
2. `make test-docs` while you work if it touches documentation; `make format` before you
   commit.
3. `make ci` before pushing, plus whichever stack target covers what you changed.
4. Read your own diff as though someone else wrote it, and look for the thing that will be
   embarrassing later. That is rule 4, and it is not optional.

If something in this guide is wrong or out of date, fixing it is a legitimate first pull
request — and by rule 5, a document that asserts something untrue is a defect.
