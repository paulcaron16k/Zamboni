# SPDX-License-Identifier: Apache-2.0
"""What the sdist contains: tracked files from an allow-list, and enough to rebuild the wheel.

hatchling's default sdist is the working tree minus `.gitignore`, so a local
`uv build` shipped whatever untracked files were lying around -- `foo`,
`todo.txt`, `.claude/settings.local.json` on 2026-10-16 (ZMBNI-212). The
release builds from a clean checkout and never had the problem; the release
security review's local check (docs/releasing.md §3a item 7) did.

The builds run in-process through hatchling's builders, the same code
`uv build` drives, so these tests need neither network nor a subprocess.
"""

from __future__ import annotations

import shutil
import subprocess
import tarfile
import tomllib
import uuid
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from hatchling.builders.sdist import SdistBuilder
from hatchling.builders.wheel import WheelBuilder

ROOT = Path(__file__).resolve().parent.parent

#: Written into the sdist by hatchling itself, so never in git.
GENERATED = {"PKG-INFO"}


def tracked() -> set[str]:
    """Every git-tracked path, or skip when this is not a git checkout."""
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    result = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-z"], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        pytest.skip(f"not a git checkout: {result.stderr.strip()}")
    return set(filter(None, result.stdout.split("\0")))


def build_sdist(directory: Path) -> Path:
    return Path(next(SdistBuilder(str(ROOT)).build(directory=str(directory))))


def sdist_members(sdist: Path) -> set[str]:
    """Member paths with the `name-version/` prefix removed; files only."""
    with tarfile.open(sdist) as archive:
        return {m.name.split("/", 1)[1] for m in archive.getmembers() if m.isfile()}


def wheel_members(wheel: str) -> set[str]:
    with zipfile.ZipFile(wheel) as archive:
        return set(archive.namelist())


@pytest.fixture
def untracked_files() -> Iterator[list[str]]:
    """Untracked files where a stray one would really be: the root, and beside the demo data.

    Removed afterwards whatever happens. Without them a clean CI checkout has
    nothing untracked, and the tracked-only test would pass even with the
    allow-list deleted -- it is these that make that regression fail in CI.
    """
    tag = uuid.uuid4().hex
    paths = [ROOT / f"untracked-{tag}.txt", ROOT / "data" / "healthims" / f"untracked-{tag}.txt"]
    for path in paths:
        path.write_text("a stand-in for a local file holding a credential\n")
    try:
        yield [str(p.relative_to(ROOT)) for p in paths]
    finally:
        for path in paths:
            path.unlink(missing_ok=True)


def test_every_sdist_member_is_tracked_by_git(tmp_path):
    members = sdist_members(build_sdist(tmp_path))
    assert members, "the sdist is empty"
    untracked = members - tracked() - GENERATED
    assert not untracked, f"the sdist ships files git does not track: {sorted(untracked)}"


def test_the_sdist_leaves_out_untracked_files_outside_the_allow_list(tmp_path, untracked_files):
    members = sdist_members(build_sdist(tmp_path))
    shipped = set(untracked_files) & members
    assert not shipped, (
        f"untracked files reached the sdist: {sorted(shipped)}. "
        "[tool.hatch.build.targets.sdist] only-include must stay an allow-list."
    )


def test_the_sdist_carries_every_wheel_input(tmp_path):
    """Each force-include source and the package are in the sdist, or the release cannot build.

    release.yml runs `uv build`, which builds the wheel *from the sdist*.
    """
    members = sdist_members(build_sdist(tmp_path))
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    wheel = config["tool"]["hatch"]["build"]["targets"]["wheel"]
    sources = [*wheel["force-include"], *wheel["packages"]]
    assert sources, "no wheel inputs declared"
    for source in sources:
        assert any(m == source or m.startswith(f"{source}/") for m in members), (
            f"{source} is a wheel input but not in the sdist"
        )


def test_the_wheel_built_from_the_sdist_matches_the_wheel_built_from_the_tree(tmp_path):
    """What PyPI gets (`uv build`: sdist, then wheel from it) is what a tree build gives."""
    sdist = build_sdist(tmp_path / "sdist")
    with tarfile.open(sdist) as archive:
        archive.extractall(tmp_path / "unpacked", filter="data")
    (unpacked,) = (tmp_path / "unpacked").iterdir()

    from_sdist = next(WheelBuilder(str(unpacked)).build(directory=str(tmp_path / "a")))
    from_tree = next(WheelBuilder(str(ROOT)).build(directory=str(tmp_path / "b")))

    assert wheel_members(from_sdist) == wheel_members(from_tree)
