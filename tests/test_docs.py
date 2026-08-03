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
    for doc in sorted(DOCS.glob("*.md")):
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


def linking_docs() -> list[Path]:
    """docs/*.md plus the two at the repository root.

    Globbing only docs/ left README.md and CHANGELOG.md unchecked, and both link
    into docs/ -- so the file most likely to be read first was the one file whose
    links nothing verified. Relative targets resolve against each file's own
    directory, which is why this returns paths rather than names.
    """
    return sorted(DOCS.glob("*.md")) + [
        path for path in (ROOT / "README.md", ROOT / "CHANGELOG.md") if path.exists()
    ]


def test_doc_links_resolve():
    checked = 0
    for doc in linking_docs():
        # Skip absolute URLs and mailto:; strip anchors, which are not paths.
        for target in re.findall(r"\]\((?!https?:|mailto:)([^)#]+)", doc.read_text()):
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
