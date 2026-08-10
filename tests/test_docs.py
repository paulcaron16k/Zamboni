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


# -- the task backlog ----------------------------------------------------

STATUSES = ("done", "inproject", "todo", "cancelled")
ROW = re.compile(r"^\| (ZMBNI-\d+) \| ([^|]+?) \|(.*?)\| (\w+) \|([^|]*)\|", re.M)


def backlog_rows() -> list[tuple[str, str, str, str, str]]:
    return ROW.findall((DOCS / "tasks.md").read_text())


def epic_of(story_id: str) -> str:
    """ZMBNI-505 -> ZMBNI-5, ZMBNI-1007 -> ZMBNI-10."""
    number = story_id.split("-")[1]
    return f"ZMBNI-{number[:-2]}"


def split_backlog():
    epics, stories = {}, {}
    for task_id, _title, _desc, status, done_at in backlog_rows():
        target = stories if len(task_id.split("-")[1]) >= 3 else epics
        target[task_id] = (status, done_at.strip())
    return epics, stories


def test_every_backlog_status_is_one_of_the_declared_four():
    _epics, stories = split_backlog()
    assert stories, "no stories parsed from tasks.md; the table format changed"
    bad = {i: s for i, (s, _) in stories.items() if s not in STATUSES}
    assert not bad, f"undeclared statuses: {bad}"


def test_the_story_count_summary_matches_the_rows():
    """A hand-written total is a hand-written error. It was wrong on all four
    numbers when first written, so it is computed and then checked."""
    from collections import Counter

    _epics, stories = split_backlog()
    counts = Counter(status for status, _ in stories.values())
    text = (DOCS / "tasks.md").read_text()
    line = next(ln for ln in text.splitlines() if ln.startswith("**Story counts:**"))

    for status in STATUSES:
        assert f"{counts[status]} {status}" in line, (
            f"summary says something other than {counts[status]} for {status!r}: {line}"
        )
    assert f"({len(stories)} stories)" in line


def test_an_epic_is_only_done_when_all_its_stories_are():
    """Otherwise the top table reads as finished work that is not finished --
    which is exactly the mistake this test was written in response to."""
    epics, stories = split_backlog()
    for epic_id, (status, _) in epics.items():
        children = [s for i, (s, _) in stories.items() if epic_of(i) == epic_id]
        assert children, f"{epic_id} has no stories"
        open_children = [s for s in children if s in ("todo", "inproject")]
        if status == "done":
            assert not open_children, (
                f"{epic_id} is marked done but has open stories: {open_children}"
            )
        else:
            assert open_children, f"{epic_id} is {status} but every story is closed"


def test_closed_stories_carry_a_completion_date_and_open_ones_do_not():
    _epics, stories = split_backlog()
    for task_id, (status, done_at) in stories.items():
        if status in ("done", "cancelled"):
            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", done_at), (
                f"{task_id} is {status} but completed-at is {done_at!r}"
            )
        else:
            assert not done_at, f"{task_id} is {status} but carries completed-at {done_at!r}"


def test_every_referenced_fr_exists_in_the_plan():
    """A backlog citing a requirement that does not exist is worse than one
    citing none: it reads as traceability."""
    plan = (DOCS / "plan.md").read_text()
    declared = set(re.findall(r"\| (FR-\d+\.\d+)", plan))
    assert declared, "no FR ids found in plan.md"

    cited: set[str] = set()
    for _id, _title, desc, _status, _done in backlog_rows():
        # Ranges are written "FR-7.12-7.14" with an en dash. Matching only the
        # leading id would check one endpoint of each range and silently pass a
        # dangling upper bound, so expand them.
        # The upper bound may repeat the group ("FR-9.1-9.2") or not ("FR-9.1-2").
        # A greedy `(\d+)` after the dash swallows the group and turns 9.1-9.2
        # into 9.1..9.9, so the optional group prefix is consumed explicitly.
        for group, lo, hi in re.findall(r"FR-(\d+)\.(\d+)[\u2013-](?:\d+\.)?(\d+)", desc):
            if int(hi) >= int(lo):
                cited |= {f"FR-{group}.{n}" for n in range(int(lo), int(hi) + 1)}
        cited |= set(re.findall(r"FR-\d+\.\d+", desc))

    missing = sorted(cited - declared)
    assert not missing, f"tasks.md cites requirements absent from plan.md: {missing}"
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
        if isinstance(parsed, dict) and "tables" in parsed:
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
