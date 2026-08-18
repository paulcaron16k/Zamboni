"""The docs cite tests by name as evidence. Keep those citations true.

A requirement table that names a test which no longer exists is worse than no
table at all -- it reads as verified coverage while proving nothing.

The citations live in plan.md (the requirements traceability) rather than
design.md (the rationale), so this checks every doc rather than one by name --
otherwise splitting a document silently disables the check.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
TESTS = Path(__file__).resolve().parent


def collected_test_names() -> set[str]:
    names = set()
    for path in TESTS.glob("test_*.py"):
        names |= set(re.findall(r"^def (test_[a-z0-9_]+)", path.read_text(), re.M))
    return names


def test_every_cited_test_exists():
    known = collected_test_names()
    cited_anywhere = set()
    for doc in all_docs():
        cited = set(re.findall(r"`(test_[a-z0-9_]+)`", doc.read_text()))
        cited_anywhere |= cited
        missing = sorted(cited - known)
        assert not missing, f"{doc.name} cites tests that do not exist: {missing}"

    assert cited_anywhere, "no doc cites any test; the requirement tables lost their evidence"


def test_the_requirements_table_still_carries_its_evidence():
    """plan.md is where the traceability lives; an empty one is a silent regression."""
    plan = (DOCS / "plan.md").read_text()
    cited = set(re.findall(r"`(test_[a-z0-9_]+)`", plan))
    assert len(cited) > 50, f"plan.md cites only {len(cited)} tests; expected the full matrix"


#: Root-level documents that link into docs/. SECURITY.md and CONTRIBUTING.md
#: joined the list when they were written: a contributor guide that names a test
#: which no longer exists, or points at a moved runbook, is worse than none --
#: it is the first thing an outside contributor reads.
ROOT_DOCS = ("README.md", "CHANGELOG.md", "SECURITY.md", "CONTRIBUTING.md")


def all_docs() -> list[Path]:
    """Everything whose claims are checked: docs/*.md plus the root documents."""
    return sorted(DOCS.glob("*.md")) + [
        path for path in (ROOT / name for name in ROOT_DOCS) if path.exists()
    ]


def linking_docs() -> list[Path]:
    """The documents whose relative links are resolved.

    Globbing only docs/ left README.md and CHANGELOG.md unchecked, and both link
    into docs/ -- so the file most likely to be read first was the one file whose
    links nothing verified. Relative targets resolve against each file's own
    directory, which is why this returns paths rather than names.
    """
    return all_docs()


def without_code_blocks(text: str) -> str:
    """Markdown minus fenced code.

    A link checker that reads code blocks invents links: Python like
    ``evaluators[manifest.spec_id](manifest)`` matches the same pattern as
    ``[text](target)``. The false positive is the smaller problem -- the real
    risk is that noise trains someone to ignore this test, and it exists to
    catch a genuinely broken link.
    """
    return re.sub(r"^```.*?^```", "", text, flags=re.S | re.M)


def test_doc_links_resolve():
    checked = 0
    for doc in linking_docs():
        # Skip absolute URLs and mailto:; strip anchors, which are not paths.
        for target in re.findall(
            r"\]\((?!https?:|mailto:)([^)#]+)", without_code_blocks(doc.read_text())
        ):
            resolved = (doc.parent / target).resolve()
            assert resolved.exists(), f"{doc.name} links to missing {target}"
            checked += 1

    assert checked > 20, f"only {checked} relative links found; the pattern stopped matching"


# -- the frozen backlog, and the traceability that outlived it -----------

#: sha256 of `docs/tasks_historical.md` as it was frozen. See the test below for
#: why this is a pin rather than a prohibition.
HISTORICAL_SHA256 = "859979971c60fba83d679f1a511244bf3a0834f1a894e659f4751adf93867803"


def test_the_historical_backlog_is_frozen():
    """The archive may not drift, and repairing it must be a deliberate act.

    `docs/tasks_historical.md` stopped being a tracker when epics and stories
    moved to GitHub issues. What is left is a record of decisions, cited by 147
    commit messages, and a record that can be revised quietly is one nothing else
    should cite.

    A pin rather than a read-only rule, because two invariants meet in that file
    and both are wanted. It stays inside `linking_docs()` and the cited-test
    sweep, so a document it links to cannot be renamed without something failing
    -- but the repair is an edit to a file that is supposed to be immutable. The
    pin resolves that: fix the link, re-pin the hash in the same commit, and the
    edit is visible in review instead of being either forbidden or silent.

    Four tests were deleted when this arrived, and only one was a loss:

    * the status vocabulary and the state/date consistency are what
      `gh agile validate` and GitHub's own `closedAt` now check;
    * the story-count summary existed only because the totals were hand-written,
      and a board computes them -- there is nothing left to disagree with;
    * **epic completeness has no replacement.** `gh agile validate` checks
      story-has-epic, epic-has-stories, state-against-status, type labels and
      sprint invariants, and never that an epic's children are closed before the
      epic is. Filed as a gap; recorded here so the loss is not silent.
    """
    import hashlib

    path = DOCS / "tasks_historical.md"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert digest == HISTORICAL_SHA256, (
        f"{path.name} has changed: {digest}\n"
        "It is the archive, not a tracker -- open work belongs in a GitHub issue. "
        "If the edit is a link repair, re-pin HISTORICAL_SHA256 in the same commit."
    )


def test_every_referenced_fr_exists_in_the_plan():
    """A document citing a requirement that does not exist is worse than one
    citing none: it reads as traceability.

    Every document rather than the backlog alone. The check used to read
    `tasks.md`, so it died with it -- and scoping it to one file was always the
    weaker version, since a `{Feature}_Plan_and_WBS.md` is exactly where an FR
    reference will be written from now on. Broadening it cost nothing: 186
    citations across four documents, none dangling.
    """
    plan = (DOCS / "plan.md").read_text()
    declared = set(re.findall(r"\| (FR-\d+\.\d+)", plan))
    assert declared, "no FR ids found in plan.md"

    cited: set[str] = set()
    for doc in all_docs():
        text = doc.read_text()
        # Ranges are written "FR-7.12-7.14" with an en dash. Matching only the
        # leading id would check one endpoint of each range and silently pass a
        # dangling upper bound, so expand them.
        # The upper bound may repeat the group ("FR-9.1-9.2") or not ("FR-9.1-2").
        # A greedy `(\d+)` after the dash swallows the group and turns 9.1-9.2
        # into 9.1..9.9, so the optional group prefix is consumed explicitly.
        for group, lo, hi in re.findall(r"FR-(\d+)\.(\d+)[\u2013-](?:\d+\.)?(\d+)", text):
            if int(hi) >= int(lo):
                cited |= {f"FR-{group}.{n}" for n in range(int(lo), int(hi) + 1)}
        cited |= set(re.findall(r"FR-\d+\.\d+", text))

    missing = sorted(cited - declared)
    assert not missing, f"documents cite requirements absent from plan.md: {missing}"
    assert len(cited) > 30, f"only {len(cited)} requirements cited; the ranges are not expanding"


# -- the guide's sample configurations must actually work -----------------


def config_samples() -> list[tuple[str, dict]]:
    """Whole-document config samples from the user guide.

    Scoped to user_guide.md rather than all of docs/ on purpose. table-config.md
    is the *specification*, and its json blocks are fragments -- a `retention`
    stanza on its own, a single rule -- which cannot be parsed as documents and
    should not be. The guide's blocks are things a reader will copy whole, so
    there an unparseable block is a defect rather than a style.
    """
    import json

    found = []
    for block in re.findall(r"```json\n(.*?)```", (DOCS / "user_guide.md").read_text(), re.S):
        try:
            parsed = json.loads(block)
        except json.JSONDecodeError as exc:
            raise AssertionError(
                f"user_guide.md has a ```json block that is not JSON: {exc}"
            ) from exc
        if isinstance(parsed, dict) and "namespaces" in parsed:
            found.append(("user_guide.md", parsed))
    return found


def test_the_documented_configurations_are_valid():
    """A sample config that does not parse is worse than no sample.

    The first draft of the event-data example used `from_transform`/
    `to_transform` -- the *Python attribute* names -- where the file wants
    `from`/`to`. The schema rejected it, which is what the schema is for, and
    this is what makes that rejection happen before a reader copies it.
    """
    from zamboni.tableconfig import TableConfig

    samples = config_samples()
    assert samples, "no sample configurations found; the extraction stopped matching"

    for name, raw in samples:
        try:
            TableConfig.from_dict(raw, source=name).validate()
        except Exception as exc:
            raise AssertionError(f"{name} documents a config that does not load: {exc}") from exc


def test_the_guide_documents_every_run_control():
    """The reference table has to keep up with the dataclass.

    `read_ahead_bytes` and `max_read_ahead_files` shipped as fields with no CLI
    flag and no mention here, which is a setting an operator cannot find and
    could not have used if they had.
    """
    from dataclasses import fields

    from zamboni import CompactionConfig

    guide = (DOCS / "user_guide.md").read_text()
    # A field may appear under its own name, under its flag, or -- for the
    # layout settings that belong in table-config.json -- under the key a reader
    # would actually type. `sort_expression` is spelled `--sort-by` because the
    # flag names the intent while the field names the mechanism.
    aliases = {"sort_expression": "--sort-by"}

    undocumented = [
        f.name
        for f in fields(CompactionConfig)
        if f.name not in guide
        and f"--{f.name.replace('_', '-')}" not in guide
        and aliases.get(f.name, "\0") not in guide
    ]
    assert not undocumented, f"user_guide.md does not mention: {undocumented}"


def test_no_document_carries_a_credential_shaped_literal():
    """The README shipped `credential="spark:2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"`
    in its first code sample for the whole of 0.1.0.

    Whether or not that string was ever live is beside the point: a document
    that tells operators to keep secrets out of files and command lines cannot
    open with one pasted into a code block, and a reader copying the sample
    inherits the habit. Samples read from the environment now.

    The pattern is deliberately narrow -- a long high-entropy run after a
    credential-ish keyword -- because a broad one would match hashes, commit
    ids and base64 in the demo data, and a check that cries wolf gets deleted.
    """
    suspicious = re.compile(
        r"(?:credential|password|secret[_-]?key|token)\s*[=:]\s*[\"'][^\"'\s]{20,}[\"']",
        re.I,
    )
    offenders = []
    for doc in all_docs():
        for line in doc.read_text().splitlines():
            if "os.environ" in line or "getenv" in line or "{{" in line:
                continue  # reading one is the thing we are asking for
            if match := suspicious.search(line):
                offenders.append(f"{doc.name}: {match.group(0)[:60]}")

    assert not offenders, f"credential-shaped literals in documentation: {offenders}"


# -- supply chain ---------------------------------------------------------


def test_every_github_action_is_pinned_to_a_commit_sha():
    """A movable tag is code you have not reviewed running with your tokens.

    `actions/checkout@v4` is whatever `v4` points at when the job starts, and a
    tag can be moved. That matters most in `release.yml`, whose publish job holds
    an OIDC credential able to push to PyPI -- the highest-value secret this
    project has, and one that never appears in the repository to be stolen any
    other way. Found by the first pre-release security review (§3a item 8) and
    fixed in ZMBNI-1817.

    The trailing `# v4.4.0` is required, not decorative: it is what makes a
    Dependabot pull request legible, and what tells a reader which release the
    forty hex characters are.
    """
    uses = re.compile(r"^\s*-?\s*uses:\s*(\S+)(.*)$", re.M)
    sha_pinned = re.compile(r"^[^@]+@[0-9a-f]{40}$")
    version_comment = re.compile(r"#\s*v?\d+\.\d+")

    unpinned, uncommented = [], []
    for workflow in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        for spec, rest in uses.findall(workflow.read_text()):
            if spec.startswith("./"):
                continue  # a local composite action is this repository's own code
            if not sha_pinned.match(spec):
                unpinned.append(f"{workflow.name}: {spec}")
            elif not version_comment.search(rest):
                uncommented.append(f"{workflow.name}: {spec[:20]}…")

    assert not unpinned, (
        f"these actions are pinned to a movable ref: {unpinned}. Pin the commit SHA "
        "and put the release in a trailing comment"
    )
    assert not uncommented, (
        f"these SHAs have no version comment: {uncommented}. Without it nobody can "
        "tell what is pinned, and Dependabot's pull requests are unreadable"
    )


def test_dependabot_watches_the_actions_it_pins():
    """Pinning without a bump path is how a pin becomes an old vulnerability.

    A SHA never updates itself, so the same change that makes the workflows safe
    also makes them stale by default. This asserts the other half exists.
    """
    config = ROOT / ".github" / "dependabot.yml"
    assert config.is_file(), (
        "actions are SHA-pinned with nothing proposing updates, so they will "
        "silently rot; .github/dependabot.yml is what closes that loop"
    )
    assert "github-actions" in config.read_text()
