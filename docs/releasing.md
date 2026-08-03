# Versioning and releases

What a version number of Zamboni promises, and the steps to cut one.

Most of this document is the first half. A release checklist is easy to write and
almost self-evident; *what counts as a breaking change* is neither, because for
this tool the answer is not "the function signatures changed".

---

## 1. The contract

**Semantic versioning, with the 0.x caveat.** `MAJOR.MINOR.PATCH`. Until 1.0 the
minor number carries the breaking changes — that is what 0.x means, and saying so
is more useful than implying guarantees that a pre-1.0 tool cannot keep.

### What the version covers

These are the surfaces an operator or a script can depend on. A change to any of
them is breaking:

| Surface | Breaking | Not breaking |
|---|---|---|
| **Verb names and flags** | removing `--yes`, renaming `remove-orphans`, making an optional flag required | adding a verb, adding an optional flag |
| **Exit codes** | reusing 4 for something other than an aborted safety check, or returning 0 where 3 was returned | adding a new code for a new condition |
| **`table-config.json` keys** | removing a key, narrowing what a value accepts, changing what a key *means* | adding an optional key |
| **Defaults that decide what gets deleted** | lowering `older_than_days`, raising `max_snapshot_age_days`, enabling a reclaim step that was off | changing a default that only affects performance, e.g. `target_file_size_bytes` |
| **What is refused** | allowing an operation previously blocked as unsafe | blocking something newly discovered to be unsafe (see below) |

**The fourth row is the one worth the effort of writing this down.** A release
that lowers the orphan-removal age guard from three days to one deletes files on
the next nightly run that the previous release would have kept — with no
signature changed, no flag renamed, and nothing in a conventional changelog to
warn anyone. For a tool whose job includes deleting files, the destructive
defaults *are* public API. They get a MAJOR bump (a MINOR, pre-1.0) and a
`BREAKING` line in the changelog.

**The fifth row is deliberately asymmetric.** Newly refusing an operation can
break a working pipeline, so by the letter of semver it is breaking. It still
ships in a patch release, because the alternative is leaving a known-corrupting
operation enabled to protect a version number. This has already happened once:
V3 row-lineage rewriting was blocked after the fact, when the upstream guard
turned out to be serialisation-only (FR-9.5). Such a change gets a `SAFETY` line
in the changelog and names the operation.

### What the version does not cover

**The Python API.** `TableCompactor`, `CompactionPlanner`, everything exported
from `zamboni/__init__.py` — importable, documented, and not stable. This is
shipped as a CLI; the classes are how the CLI is built and how the tests reach
in. They are refactored without ceremony. If that changes, it changes here first.

**Anything named with a leading underscore**, in this package or in PyIceberg.
This tool drives PyIceberg's private snapshot producers on purpose (design.md
§3), which is why `capabilities.py` probes structure rather than comparing
versions. A PyIceberg upgrade that changes those internals is a compatibility
matter, not a versioning one.

**`src/himsdemo`.** The demo is a teaching aid that ships in the same wheel. It
has no stability contract at all.

### The two version numbers

`table-config.json` carries its own `version` field, currently `1`, and it is
**independent of the package version**. A config file does not need editing when
Zamboni's minor number moves, and the config version bumps only when the file
format changes in a way that cannot be read compatibly. Keeping them separate is
what lets a fleet of pinned config files survive a tool upgrade.

---

## 2. Where the version lives

One literal, in `pyproject.toml`:

```toml
[project]
name = "zamboni"
version = "0.1.0"
```

Everything else derives from it. `zamboni.__version__` reads the installed
distribution metadata via `importlib.metadata`, so `zamboni --version` reports
what is actually installed rather than what a source file claims:

```console
$ zamboni --version
zamboni 0.1.0 (pyiceberg 0.11.1, python 3.13.14)
```

All three, because the first alone does not identify behaviour. Which operations
this tool will attempt is decided by probing PyIceberg, so the same Zamboni
refuses equality deletes against one PyIceberg and reads them against another —
a bug report quoting only `zamboni 0.1.0` is missing the deciding fact. Python is
there because `bin/` pins an interpreter.

`test_version.py` asserts the declared version, the installed metadata, the
banner and the changelog all agree, so no two of them can drift.

---

## 3. Cutting a release

```bash
# 1. The suite, on the versions CI runs
uv run pytest -q
uv run mypy && uv run ruff check src tests scripts && uv run ruff format --check src tests scripts

# 2. Bump the one literal, then re-lock
$EDITOR pyproject.toml            # version = "0.2.0"
uv sync                           # uv.lock records the project's own version too

# 3. Move Unreleased -> the new version in CHANGELOG.md, and date it.
#    Every BREAKING and SAFETY line must name what changed and what to do.
$EDITOR CHANGELOG.md

# 4. Regenerate the executables -- they embed the project reference, and CI
#    fails if bin/ is stale
uv run scripts/build-executable.py

# 5. Commit, then tag. Annotated, not lightweight: the tag message is the
#    changelog section, so `git show v0.2.0` explains the release without
#    a network round trip
VERSION=0.2.0
git commit -am "Release $VERSION"
awk -v want="## [$VERSION]" '
  index($0, want) == 1 { inside = 1; print; next }
  /^## \[/             { inside = 0 }
  inside
' CHANGELOG.md | git tag -a "v$VERSION" --cleanup=whitespace -F -
git push && git push --tags
```

**Step 2's `uv sync` is not housekeeping.** `uv.lock` carries its own
`version = "0.1.0"` for this project, and CI runs `uv sync --frozen` — which
fails on a lock that disagrees with `pyproject.toml`. Skipping it breaks the build
rather than passing quietly.

**Step 5 uses `awk` rather than `sed -n '/…/,/^## \[/p'`.** A range ending on the
next heading has no next heading for the oldest section, so it runs to EOF and any
trailing-line trim takes a real line with it. That is not a hypothetical edge:
the oldest section is the *first* release, so the naive version is wrong on its
first use and correct forever after. Verified against a changelog with three
sections, extracting both a middle one and the last one.

**`--cleanup=whitespace` is required, not stylistic.** `git tag` defaults to
stripping lines that begin with `#`, which is every markdown heading in the
extracted section — `## [0.2.0]`, `### BREAKING`, `### Added` all disappear and
the tag message becomes an unattributed list of bullets. Found by running the
command rather than reading it.

**Tag format `vMAJOR.MINOR.PATCH`** — `v0.2.0`, matching the changelog heading
`[0.2.0]` without the `v`. The prefix is there so `git tag` output is
unambiguous against any future non-release tag.

**No PyPI publication.** Not a decision to defer — this is consumed from a git
checkout via `uv sync --frozen`, and the two entry points people actually run are
the PEP 723 scripts in `bin/`, which reference the project as an editable path
install. Publishing would add a distribution channel nobody uses. If that
changes, `hatchling` is already the backend and `uv build` already works.

---

## 4. Why 0.1.0 is not tagged

There is no `v0.1.0` tag, and this is deliberate rather than an oversight.

The repository has no remote, so **CI has never run** — every command in
`.github/workflows/ci.yml` has been run locally, on one machine, on one Python,
against one live Lakekeeper. That is tracked as ZMBNI-905, and it is the single
item between "tests pass on my machine" and "tests pass". Tagging a release
whose test matrix has never executed would put a version number on exactly the
kind of unverified claim the rest of this project's checks exist to catch.

So the convention above is established and enforced by tests; the first tag
follows ZMBNI-905. `0.1.0` in `pyproject.toml` is the *in-development* version,
and `CHANGELOG.md` keeps its work under `[Unreleased]` until then.
