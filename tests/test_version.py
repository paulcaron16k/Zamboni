"""The version is declared once and derived everywhere else. Keep that true.

Four things claim to know the version: `pyproject.toml`, the installed
distribution metadata, `zamboni --version`, and `CHANGELOG.md`. Three of them are
derived, so the interesting failures are the ones where a derivation quietly
stops deriving -- a hand-maintained `__version__` reintroduced, or a release that
bumps the changelog and forgets `pyproject.toml`.

The convention these enforce is in docs/releasing.md.
"""

from __future__ import annotations

import importlib
import re
import sys
import tomllib
from importlib.metadata import version as distribution_version
from pathlib import Path

import pytest

import zamboni

PROJECT = Path(__file__).resolve().parent.parent
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
# Keep a Changelog headings: "## [0.2.0] - 2026-08-03", or "## [Unreleased]".
RELEASED = re.compile(r"^## \[(\d+\.\d+\.\d+)\](?:\s*-\s*(\S+))?", re.M)


def declared_version() -> str:
    with (PROJECT / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["version"])


def changelog() -> str:
    return (PROJECT / "CHANGELOG.md").read_text()


def test_the_declared_version_is_semver():
    assert SEMVER.fullmatch(declared_version()), (
        f"pyproject version {declared_version()!r} is not MAJOR.MINOR.PATCH; "
        "the tag convention in docs/releasing.md assumes it is"
    )


def test_the_package_reports_the_declared_version():
    """`__version__` reads installed metadata rather than repeating a literal.

    If someone reintroduces `__version__ = "0.1.0"` as a string in __init__.py,
    this keeps passing until the next bump and then fails -- which is the point.
    """
    assert zamboni.__version__ == declared_version()
    assert distribution_version("iceberg-zamboni") == declared_version()


def test_the_version_banner_names_all_three_versions():
    """One version does not identify behaviour.

    Which operations are attempted is decided by probing the installed PyIceberg,
    so the same zamboni behaves differently against different ones. A bug report
    quoting only the zamboni version is missing the deciding fact.
    """
    banner = zamboni.version_banner()

    assert zamboni.__version__ in banner
    assert distribution_version("pyiceberg") in banner
    assert ".".join(str(part) for part in sys.version_info[:3]) in banner


@pytest.mark.parametrize("entry_point", ["zamboni.cli", "himsdemo.cli"])
def test_both_entry_points_report_the_version_and_exit_zero(entry_point, capsys):
    """Both, because they are two parsers in two packages sharing one wheel.

    `--version` was added to `zamboni` first; the demo's parser is built
    separately and would have silently lacked it.
    """
    main = importlib.import_module(entry_point).main

    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])

    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == zamboni.version_banner()


def test_the_changelog_has_somewhere_to_record_the_next_change():
    assert "## [Unreleased]" in changelog(), (
        "CHANGELOG.md has no [Unreleased] section, so there is nowhere to add an "
        "entry without inventing a release"
    )


def undated_releases(text: str) -> list[str]:
    return [
        release
        for release, date in RELEASED.findall(text)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date)
    ]


def newest_released(text: str) -> str | None:
    releases = [release for release, _date in RELEASED.findall(text)]
    return max(releases, key=_ordinal) if releases else None


def _ordinal(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def test_every_released_changelog_section_carries_a_date():
    undated = undated_releases(changelog())
    assert not undated, (
        f"CHANGELOG.md sections {undated} have no ISO date; an undated release "
        "section cannot be told apart from work in progress"
    )


def test_the_declared_version_is_not_behind_the_changelog():
    """Catches half of a half-done release.

    Bumping CHANGELOG.md to 0.2.0 and forgetting `pyproject.toml` leaves the tool
    reporting 0.1.0 while its own notes claim 0.2.0. The reverse -- pyproject
    ahead of the newest released section -- is the normal in-development state,
    so only "behind" is an error.

    Vacuous until the first release, which is why the logic it depends on is
    covered by `test_the_changelog_checks_reject_a_malformed_changelog` rather
    than left to activate for the first time during a release.
    """
    newest = newest_released(changelog())
    if newest is None:
        pytest.skip("nothing released yet; docs/releasing.md section 4 explains why")

    assert _ordinal(declared_version()) >= _ordinal(newest), (
        f"pyproject declares {declared_version()} but CHANGELOG.md already released {newest}"
    )


@pytest.mark.parametrize(
    ("heading", "undated", "newest"),
    [
        ("## [0.2.0] - 2026-08-03", [], "0.2.0"),
        ("## [0.2.0]", ["0.2.0"], "0.2.0"),
        ("## [0.2.0] - soon", ["0.2.0"], "0.2.0"),
        ("## [Unreleased]", [], None),
        ("## [0.1.0] - 2026-07-01\n## [0.10.0] - 2026-08-03", [], "0.10.0"),
    ],
)
def test_the_changelog_checks_reject_a_malformed_changelog(heading, undated, newest):
    """The two checks above are only as good as this parse.

    Both are satisfied by a changelog they fail to understand: a heading the regex
    misses is a release with no date requirement and no version to compare
    against. So the parse is pinned directly, including the 0.10.0-beats-0.1.0
    case that a string comparison would get backwards.
    """
    assert undated_releases(heading) == undated
    assert newest_released(heading) == newest


def test_the_release_convention_is_documented():
    """These tests enforce a convention; the convention has to be readable."""
    releasing = PROJECT / "docs" / "releasing.md"
    text = releasing.read_text()

    for topic in ("BREAKING", "SAFETY", "vMAJOR.MINOR.PATCH", "table-config.json"):
        assert topic in text, f"docs/releasing.md does not cover {topic}"
