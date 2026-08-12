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
§3), which is why `capabilities.py` probes the installed build rather than
comparing versions — structurally where a symbol's presence settles the
question, and *behaviourally* where it does not: the manifest-pruning hazard is
decided by performing an overwrite on a transformed partition and counting the
survivors, because the same private symbol exists on both the corrupting build
and the fixed one (ZMBNI-1109). A PyIceberg upgrade that changes those internals
is a compatibility matter, not a versioning one.

**`src/himsdemo`.** The demo is a teaching aid that ships in the same wheel. It
has no stability contract at all.

### The two version numbers

`table-config.json` carries its own `version` field, currently `2`, and it is
**independent of the package version**. A config file does not need editing when
Zamboni's minor number moves, and the config version bumps only when the file
format changes in a way that cannot be read compatibly. Keeping them separate is
what lets a fleet of pinned config files survive a tool upgrade. It has moved
once, in `0.2.0`, and the two numbers moved independently exactly as intended:
`1` -> `2` added the namespace level, which no package version could have implied.

---

## 2. Where the version lives

One literal, in `pyproject.toml`:

```toml
[project]
name = "iceberg-zamboni"
version = "0.2.0"
```

**The distribution is `iceberg-zamboni`; the import is `zamboni`.** The bare name
is taken on PyPI. `__version__` reads the *distribution* metadata, so the rename
had to reach that lookup too -- it fails soft, reporting `0+unknown`, which would
have degraded `--version` silently in exactly the case where a version matters.

Everything else derives from it. `zamboni.__version__` reads the installed
distribution metadata via `importlib.metadata`, so `zamboni --version` reports
what is actually installed rather than what a source file claims:

```console
$ zamboni --version
zamboni 0.2.0 (pyiceberg 0.11.1, python 3.13.14)
```

All three, because the first alone does not identify behaviour. Which operations
this tool will attempt is decided by probing PyIceberg, so the same Zamboni
refuses equality deletes against one PyIceberg and reads them against another —
a bug report quoting only `zamboni 0.2.0` is missing the deciding fact. Python is
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
`version` for this project, and CI runs `uv sync --frozen` — which
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

**Pushing the tag is the publication.** `.github/workflows/release.yml` triggers
on `v*.*.*` and nothing else: it re-runs the suite against the tagged tree rather
than trusting the run from the commit the tag points at, builds, `twine check`s,
and uploads to PyPI as `iceberg-zamboni`. There is deliberately no
`workflow_dispatch` — a release that can be fired without a tag is one nobody can
reconstruct later. So step 5 is the point of no return, and everything before it
is reversible.

**The tag must agree with `pyproject.toml`,** and the workflow refuses when it
does not. This is the one guard worth having: a `v0.3.0` tag publishing `0.2.0`
cannot be corrected, because **a version number can never be reused on PyPI, even
after the file is deleted**. The whole number is burned.

**Credentials: there are none.** Publication uses trusted publishing — PyPI mints
a short-lived credential from GitHub's OIDC identity for this repository,
workflow and environment, so there is no long-lived token in the repository
settings to leak, rotate or forget. It needs a one-time setup on PyPI (Publishing
-> pending publisher for `iceberg-zamboni`, owner `paulcaron16k`, repo `Zamboni`,
workflow `release.yml`, environment `pypi`) and nothing afterwards. Until that
exists the publish job fails at the upload — after `verify` and `build` have
passed, and without consuming the version.

**`bin/` is not what PyPI ships,** which has already cost one bug. The PEP 723
scripts reference the project as an *editable path install*; the wheel is built
from the same `pyproject.toml` but exercises none of that. The rename in `0.2.0`
broke both `bin/` executables while the wheel installed and ran perfectly
(ZMBNI-1810). Step 4 exists because CI checks `bin/` is not stale; it does not
check that `bin/` still works, so run one of them.

---

## 4. Why this is 0.2.0 and not 1.0.0

`v0.1.0` and `v0.2.0` are tagged. Either could have been `1.0.0` — the scope is
delivered, three engines are implemented, and every operation has been verified
against a live Lakekeeper and MinIO — and deliberately was not, for one reason
and two specifics.

**The reason is asymmetry.** `0.x` costs nothing: it already permits breaking
changes, and `1.0.0` can follow at any time. A promise cannot be withdrawn. Cut
`1.0.0` too early and the first genuine correction to a destructive default has to
be either `2.0.0` or a quiet slip into a minor release — and the second of those
makes this document untrue, which is worse than a low version number.

0.2.0 is what that argument looks like when it is not free. Its BREAKING section
runs to seven entries — a distribution rename, `table-config.json` version 2,
secrets removed as command-line flags — every one of which a 1.0 would have had
to either carry forward or break a promise to make. `0.x` is what allowed them to
be fixed rather than lived with.

**CI has executed, and this argument is settled.** It ran green on GitHub across
six jobs, including a real Lakekeeper, Postgres and MinIO, and a built Spark
Connect server (ZMBNI-905). `1.0.0` reads as "verified", and that part of the
claim is now backed.

**The parts a 1.0 locks hardest have had one author.** The `table-config.json`
schema freezes its `version: 2` the moment a 1.0 tool depends on it, and config
schemas are where a second user finds the sharp edges — that number moved once
already, in this release. The defaults deciding what gets deleted — §1 argues
these are public API — were chosen against a single five-day dataset;
`older_than_days: 3` is Iceberg's own number, but nobody has yet run this against
a warehouse whose longest compaction we did not also write.

**What 1.0.0 waits on:** one maintenance cycle against a warehouse we did not
build, and a second user on the config schema.
