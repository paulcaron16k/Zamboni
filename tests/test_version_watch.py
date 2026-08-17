"""The cap watcher, without a network.

Everything asserted here is a pure function, which is why the script keeps its
one HTTPS request in a function of its own. The alternative -- a test that
reaches PyPI -- would assert whatever upstream published this morning and fail
for reasons that have nothing to do with this repository.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

#: A PEP 508 requirement rather than any old string: a name, an optional extras
#: list, then a comparison. `pyiceberg[pyarrow]>=0.11.1,<0.12` matches;
#: `Apache-2.0` and `src/zamboni` do not.
_REQUIREMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(\[[^]]*\])?\s*[<>=!~]")


def _all_strings(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for v in value.values() for s in _all_strings(v)]
    if isinstance(value, list):
        return [s for v in value for s in _all_strings(v)]
    return []


def _load():
    """`scripts/` is not a package, so the script is loaded by path.

    Registered in `sys.modules` *before* it executes, because `@dataclass` under
    `from __future__ import annotations` resolves its field types through
    `sys.modules[cls.__module__]` -- absent that entry it raises on a module that
    imports perfectly well when run.
    """
    spec = importlib.util.spec_from_file_location(
        "version_watch", ROOT / "scripts" / "version_watch.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


vw = _load()


@pytest.fixture(scope="module")
def pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def test_every_upper_bound_in_pyproject_is_watched(pyproject):
    """Derived from the file, not from a list maintained beside it.

    A hardcoded expectation here would keep passing after a new cap was added,
    which is the one failure that matters: an unwatched cap is invisible for
    exactly as long as nobody looks.
    """
    watched = {(c.distribution, c.operator, c.bound) for c in vw.declared_caps(pyproject)}

    # Derived by walking every string in the document, which is independent of
    # the production code's traversal of three named tables -- so a cap added to
    # a table nobody thought of still shows up here as unwatched.
    declared = {
        (name.group(1), operator, bound)
        for value in _all_strings(pyproject)
        if _REQUIREMENT.match(value) and (name := vw._NAME.match(value))
        for operator, bound in vw._UPPER_BOUND.findall(value)
    }

    assert declared, "no upper bound found in pyproject.toml; the parse is wrong, not the file"
    assert declared <= watched, f"caps written down but not watched: {sorted(declared - watched)}"


def test_a_cap_records_where_it_was_written(pyproject):
    """The report is useless without it: `pyspark-client` is capped twice, in
    the `spark` extra and in the dev group, and they lift for different reasons."""
    caps = vw.declared_caps(pyproject)
    places = {c.where for c in caps if c.distribution == "pyspark-client"}
    assert places == {"extra: spark", "group: dev"}, places


def test_floors_are_not_reported_as_caps(pyproject):
    assert all(cap.operator in ("<", "<=") for cap in vw.declared_caps(pyproject))


@pytest.mark.parametrize(
    ("operator", "bound", "version", "expected"),
    [
        # The boundary itself: `<0.12` refuses 0.12.0, so 0.12.0 exceeds it.
        ("<", "0.12", "0.12.0", True),
        ("<", "0.12", "0.11.9", False),
        ("<", "4.1", "4.0.4", False),
        ("<", "4.1", "4.1", True),
        ("<", "4.1", "4.2.0", True),
        ("<", "5", "4.9.9", False),
        ("<", "5", "5.0.0", True),
        # `<=` admits its own bound, so only something past it is a finding.
        ("<=", "1.4", "1.4.0", False),
        ("<=", "1.4", "1.4.1", True),
    ],
)
def test_exceeds_at_the_boundary(operator, bound, version, expected):
    cap = vw.Cap("d", operator, bound, "where", "d" + operator + bound)
    assert vw.exceeds(cap, version) is expected


def test_prereleases_are_not_findings():
    """`pyiceberg<0.12` is waiting for a *release*, and 0.12.0rc1 is on PyPI
    today. Counting it would open an issue about a version that cannot lift the
    cap it names."""
    cap = vw.Cap("pyiceberg", "<", "0.12", "project.dependencies", "pyiceberg<0.12")
    for version in ("0.12.0rc1", "0.12.0b1", "0.12.0.dev1", "1.0.0a1"):
        assert vw.release_ordinal(version) is None
        assert not vw.exceeds(cap, version)


def test_a_fully_yanked_release_is_not_a_finding():
    """PyPI keeps listing a yanked version, and nobody should install one."""
    payload = {
        "releases": {
            "4.0.4": [{"yanked": False}],
            "4.1.0": [{"yanked": True}, {"yanked": True}],
            "4.2.0": [{"yanked": True}, {"yanked": False}],
        }
    }
    assert sorted(vw.usable_releases(payload)) == ["4.0.4", "4.2.0"]


def test_the_newest_version_above_the_cap_is_the_one_reported():
    cap = vw.Cap("pyspark-client", "<", "4.1", "group: dev", "pyspark-client<4.1")
    assert vw.newest_above(cap, ["4.0.4", "4.1.0", "4.2.0", "4.10.0"]) == "4.10.0"
    assert vw.newest_above(cap, ["4.0.1", "4.0.4"]) is None


def test_the_marker_is_stable_for_the_same_findings_and_changes_with_them():
    """The workflow comments when this line changes and stays quiet when it does
    not, so it must not carry anything that varies on its own -- the date lives
    in the body, deliberately below it."""
    cap = vw.Cap("pyspark-client", "<", "4.1", "group: dev", "pyspark-client<4.1")
    one = [vw.Finding(cap, "4.2.0")]
    assert vw.marker(one) == vw.marker([vw.Finding(cap, "4.2.0")])
    assert vw.marker(one) != vw.marker([vw.Finding(cap, "4.3.0")])
    assert vw.marker(one) != vw.marker([])
    assert vw.marker([]).startswith(vw.MARKER_PREFIX)


def test_the_body_leads_with_the_marker_either_way():
    """The workflow reads line 1 with `head -1`; a body that buried it would
    compare prose and comment every month."""
    cap = vw.Cap("pyiceberg", "<", "0.12", "project.dependencies", "pyiceberg<0.12")
    for findings in ([], [vw.Finding(cap, "0.12.0")]):
        body = vw.build_body(findings, checked="2026-08-17", caps_checked=5)
        assert body.splitlines()[0] == vw.marker(findings)
        assert "2026-08-17" in body


def test_the_body_names_the_cap_and_the_release_that_passed_it():
    cap = vw.Cap("pyiceberg", "<", "0.12", "project.dependencies", "pyiceberg<0.12")
    body = vw.build_body([vw.Finding(cap, "0.12.0")], checked="2026-08-17", caps_checked=5)
    assert "pyiceberg" in body
    assert "<0.12" in body
    assert "0.12.0" in body
    assert "project.dependencies" in body
