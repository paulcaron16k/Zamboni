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

DOCS = Path(__file__).resolve().parent.parent / "docs"
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


def test_design_doc_links_resolve():
    design_dir = DOCS
    for doc in DOCS.glob("*.md"):
        for target in re.findall(r"\]\((?!https?:)([^)#]+)", doc.read_text()):
            resolved = (design_dir / target).resolve()
            assert resolved.exists(), f"{doc.name} links to missing {target}"
