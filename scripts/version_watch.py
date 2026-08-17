# SPDX-License-Identifier: Apache-2.0
"""Report upstream releases that sit above a cap this project declares.

Every ``<`` bound in ``pyproject.toml`` is a decision with an expiry date. The
two that matter today are ``pyiceberg<0.12``, which is a data-corruption
workaround (docs/upstream-0.12-upsert-regression.md), and the dev group's
``pyspark-client>=4.0.4,<4.1``, which is matched to the dev stack's server. Both
are meant to be lifted deliberately once the new release has been tested -- and
neither announces itself.

**Why this is not Dependabot's job.** Dependabot's ``uv`` ecosystem has been
generally available since March 2025, but it updates ``uv.lock`` and not
``pyproject.toml`` (dependabot/dependabot-core#12788). With ``<4.1`` written
down, the most it can ever offer is a 4.0.x patch: the day 4.1.0 publishes there
is no pull request and no notification, because the constraint it would have to
edit is the one it does not touch. A cap is invisible to the tool whose job is
telling you about new versions, which is the gap this script fills.

It is also why watching *every* declared cap costs nothing extra. The caps are
read out of the file rather than listed here, so a new one is watched the day it
is written and this script does not become a second place to remember.

**Pure detection, no side effects.** Everything except :func:`fetch_releases` is
a pure function, so the interesting behaviour -- which versions count, when a
cap is exceeded -- is tested without a network. Opening and updating the issue
belongs to the workflow, the same split the Trino and Spark maintainers use for
statement generation.

Run it locally, no install required::

    uv run --no-project scripts/version_watch.py

Exits 0 whether or not anything was found; the report is on stdout and
``findings=true|false`` goes to ``$GITHUB_OUTPUT`` when that is set. A lookup
that fails exits 2 instead, because a PyPI that cannot be reached must not read
as "every cap is fine" -- that is the one wrong answer this script could give
that nobody would notice.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

PYPI_JSON = "https://pypi.org/pypi/{distribution}/json"

#: One open issue, reused. The title is fixed so the workflow can find it again;
#: everything that varies lives in the body.
ISSUE_TITLE = "Upstream releases above our declared version caps"

#: First line of the body, and the only part the workflow compares. A body edit
#: sends no notification, so the workflow comments when this changes and stays
#: quiet when a re-check found the same thing it found last month.
MARKER_PREFIX = "<!-- version-watch:"

#: Requirement strings live in three different shapes of table.
DEPENDENCY_SOURCES = "project.dependencies", "project.optional-dependencies", "dependency-groups"

_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
_UPPER_BOUND = re.compile(r"(<=?)\s*([0-9][0-9A-Za-z.*+!-]*)")


@dataclass(frozen=True)
class Cap:
    """An upper bound this project declares, and where it is written."""

    distribution: str
    operator: str
    bound: str
    where: str
    requirement: str

    def __str__(self) -> str:
        return f"{self.distribution}{self.operator}{self.bound}"


@dataclass(frozen=True)
class Finding:
    cap: Cap
    latest: str


def declared_caps(pyproject: dict) -> list[Cap]:
    """Every ``<`` or ``<=`` bound in the file, with its provenance.

    ``>=`` floors are ignored: a floor going stale costs nothing, while a cap
    going stale is the thing that silently holds a project on an old release.
    """
    caps: list[Cap] = []
    project = pyproject.get("project", {})

    caps += _caps_in(project.get("dependencies", []), "project.dependencies")
    for extra, requirements in (project.get("optional-dependencies") or {}).items():
        caps += _caps_in(requirements, f"extra: {extra}")
    for group, requirements in (pyproject.get("dependency-groups") or {}).items():
        # A group may contain `{include-group = "..."}` tables as well as strings.
        caps += _caps_in([r for r in requirements if isinstance(r, str)], f"group: {group}")
    return caps


def _caps_in(requirements: list[str], where: str) -> list[Cap]:
    caps = []
    for requirement in requirements:
        name = _NAME.match(requirement)
        if not name:
            continue
        for operator, bound in _UPPER_BOUND.findall(requirement):
            caps.append(
                Cap(
                    distribution=name.group(1),
                    operator=operator,
                    bound=bound,
                    where=where,
                    requirement=requirement,
                )
            )
    return caps


def release_ordinal(version: str) -> tuple[int, ...] | None:
    """``"4.1.0" -> (4, 1, 0)``, and ``None`` for anything not purely numeric.

    Returning ``None`` for a non-numeric part is the pre-release filter, not a
    parsing shortcut. ``0.12.0rc1`` exists on PyPI *now* and is exactly what the
    ``pyiceberg<0.12`` cap is waiting out, so a watcher that counted it would
    open an issue about a release nobody can lift the cap for. Same for
    ``4.1.0b1``.

    Deliberately not ``packaging.version``: it is a transitive dependency of
    pytest here rather than something this project declares, and the ordering it
    would add over these five distributions is ordering this comparison does not
    need. The cost is that ``1.11.0.post1`` sorts as ``(1, 11, 0, 1)``, which is
    wrong against ``1.11.0`` in general and harmless against a cap.
    """
    parts = version.split(".")
    if not all(part.isdigit() for part in parts):
        return None
    return tuple(int(part) for part in parts)


def exceeds(cap: Cap, version: str) -> bool:
    """Has ``version`` reached or passed what ``cap`` refuses to install?"""
    released, bound = release_ordinal(version), release_ordinal(cap.bound)
    if released is None or bound is None:
        # A wildcard or epoch in the cap itself. Say nothing rather than guess.
        return False
    width = max(len(released), len(bound))
    released += (0,) * (width - len(released))
    bound += (0,) * (width - len(bound))
    return released >= bound if cap.operator == "<" else released > bound


def usable_releases(payload: dict) -> list[str]:
    """Final, non-yanked versions from a PyPI JSON response.

    A version whose every file is yanked is one PyPI still lists and nobody
    should install, so it must not trigger an issue. ``info.version`` alone is
    not enough here: it is one string, and the question is which of *all* the
    published versions clears our cap.
    """
    releases = payload.get("releases") or {}
    usable = []
    for version, files in releases.items():
        if release_ordinal(version) is None:
            continue
        if files and all(f.get("yanked") for f in files):
            continue
        usable.append(version)
    return usable


def fetch_releases(distribution: str, *, timeout: float = 20.0) -> dict:
    """The only function here that touches the network."""
    url = PYPI_JSON.format(distribution=distribution)
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def newest_above(cap: Cap, versions: list[str]) -> str | None:
    above = [v for v in versions if exceeds(cap, v)]
    if not above:
        return None
    return max(above, key=lambda v: release_ordinal(v) or ())


def marker(findings: list[Finding]) -> str:
    """A one-line fingerprint of the findings, for the workflow to compare."""
    inside = "; ".join(f"{f.cap}={f.latest}" for f in findings) or "clear"
    return f"{MARKER_PREFIX} {inside} -->"


def build_body(findings: list[Finding], *, checked: str, caps_checked: int) -> str:
    lines = [marker(findings), ""]
    if not findings:
        lines += [
            (
                "Every version cap in `pyproject.toml` is still the newest release "
                "on PyPI. Nothing to do."
            ),
        ]
    else:
        lines += [
            (
                "PyPI has published releases above caps this project declares. Each "
                "of these caps was written for a reason, so none of them lifts "
                "automatically -- test the new release, then raise the bound."
            ),
            "",
            "| distribution | our cap | declared in | newest on PyPI |",
            "|---|---|---|---|",
        ]
        lines += [
            f"| `{f.cap.distribution}` | `{f.cap.operator}{f.cap.bound}` | "
            f"{f.cap.where} | **{f.latest}** |"
            for f in findings
        ]
        lines += [
            "",
            "Pre-releases are ignored, so each version above is a final release.",
            "",
            (
                "Before raising a `pyspark-client` bound, the dev stack's server "
                "version moves with it -- `SPARK_VERSION` in `dev-stack/.env.sample` "
                "and the `spark` extra both point at the same Spark. Before raising "
                "`pyiceberg`, re-run the capability probes against the new build "
                "(CONTRIBUTING.md rule 3)."
            ),
        ]
    lines += [
        "",
        (
            f"_{caps_checked} caps checked against PyPI on {checked}. "
            "Opened and updated by `.github/workflows/version-watch.yml`; "
            "reproduce with `uv run --no-project scripts/version_watch.py`._"
        ),
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "pyproject.toml",
    )
    args = parser.parse_args(argv)

    pyproject = tomllib.loads(args.pyproject.read_text())
    caps = declared_caps(pyproject)

    findings: list[Finding] = []
    failures: list[str] = []
    # One request per distribution, not per cap: `pyspark-client` is capped in
    # two places and the answer is the same for both.
    payloads: dict[str, dict] = {}
    for distribution in sorted({cap.distribution for cap in caps}):
        try:
            payloads[distribution] = fetch_releases(distribution)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            failures.append(f"{distribution}: {exc}")

    for cap in caps:
        payload = payloads.get(cap.distribution)
        if payload is None:
            continue
        if latest := newest_above(cap, usable_releases(payload)):
            findings.append(Finding(cap, latest))

    findings.sort(key=lambda f: (f.cap.distribution, f.cap.where))
    body = build_body(
        findings,
        checked=dt.datetime.now(dt.UTC).date().isoformat(),
        caps_checked=len(caps),
    )
    print(body)

    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as handle:
            handle.write(f"findings={'true' if findings else 'false'}\n")

    if failures:
        print("could not reach PyPI for:", *failures, sep="\n  ", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
