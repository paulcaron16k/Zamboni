# SPDX-License-Identifier: Apache-2.0
"""The schemas a release publishes: one path, promised, attached, documented.

A producer that may not depend on this package -- target-iceberg emitting
`write.completed` -- vendors the JSON Schema from the repository at a tag or from
the GitHub release (#183). Three things name each file -- the accessor that reads
it, release.yml that attaches it, and the doc that promises its path -- and a
move that updates one of them breaks every producer that fetches it. These
tests are what keep them one.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from zamboni import events, tableconfig_schema

ROOT = Path(__file__).resolve().parent.parent
RELEASE = ROOT / ".github" / "workflows" / "release.yml"
REPO_URL = "paulcaron16k/Zamboni"

#: Each published schema: the accessor's path, and the doc that promises it.
SCHEMAS = [
    pytest.param(events.SPEC_PATH, "docs/write-completed.md", id="write-completed"),
    pytest.param(tableconfig_schema.SPEC_PATH, "docs/table-config.md", id="table-config"),
]


def workflow() -> dict:
    loaded: dict = yaml.safe_load(RELEASE.read_text())
    return loaded


def attached() -> set[str]:
    """The repository paths the draft-release job uploads as assets."""
    script = "\n".join(step.get("run", "") for step in workflow()["jobs"]["draft-release"]["steps"])
    create = script.split("gh release create", 1)[1]
    return {word for word in create.split() if word.endswith(".json")}


def published(spec_path: Path) -> str:
    """The repository path of the file an accessor reads.

    Derived from the name, not from ``spec_path`` itself: installed from a
    wheel -- CI's consumer job -- the accessor reads from site-packages, which
    is not under the repository at all.
    """
    assert spec_path.parent.name == "zamboni", f"{spec_path} is not in the package"
    return f"src/zamboni/{spec_path.name}"


@pytest.mark.parametrize(("spec_path", "doc"), SCHEMAS)
def test_each_schema_is_attached_to_the_release_from_the_path_its_accessor_reads(spec_path, doc):
    relative = published(spec_path)
    assert relative in attached(), f"release.yml does not attach {relative}"
    # What a producer fetches is what the accessor returns: in a checkout the
    # same file, and installed from a wheel the copy the wheel shipped.
    assert (ROOT / relative).read_bytes() == spec_path.read_bytes()


def test_every_shipped_schema_is_a_release_asset():
    """A new `*_spec.json` is a decision -- publish it or say why not -- not an
    omission found by the producer that wanted it."""
    package = ROOT / "src" / "zamboni"
    shipped = {p.relative_to(ROOT).as_posix() for p in package.glob("*_spec.json")}
    assert shipped, "no shipped schemas found; has src/zamboni moved?"
    assert attached() == shipped


@pytest.mark.parametrize(("spec_path", "doc"), SCHEMAS)
def test_the_doc_promises_the_path_and_both_urls(spec_path, doc):
    text = (ROOT / doc).read_text()
    relative = published(spec_path)
    assert f"`{relative}`" in text, f"{doc} does not name the promised path {relative}"
    assert f"https://raw.githubusercontent.com/{REPO_URL}/v0.6.0/{relative}" in text
    assert f"https://github.com/{REPO_URL}/releases/download/v0.6.0/{spec_path.name}" in text


def artifact(job: dict, action: str) -> str | None:
    """The artifact name a job's upload- or download-artifact step names."""
    for step in job["steps"]:
        if step.get("uses", "").startswith(f"actions/{action}-artifact@"):
            name: str = step["with"]["name"]
            return name
    return None


def test_the_release_carries_the_distributions_pypi_gets_not_a_rebuild():
    """A second `uv build` would attach a separate build, and nothing promises
    it is byte-identical to PyPI's; attaching the one artifact both jobs
    download is what makes "the same file" true."""
    jobs = workflow()["jobs"]
    built = artifact(jobs["build"], "upload")
    assert built is not None
    assert artifact(jobs["publish"], "download") == built
    assert artifact(jobs["draft-release"], "download") == built
    draft = "\n".join(step.get("run", "") for step in jobs["draft-release"]["steps"])
    assert "dist/*" in draft.split("gh release create", 1)[1].split()
    assert "uv build" not in draft


def test_nothing_is_public_unless_pypi_and_the_release_both_succeed():
    """Drafted before PyPI, published after it: a version never reaches PyPI
    without its schemas, and a release never goes public for a version PyPI
    refused."""
    jobs = workflow()["jobs"]
    assert jobs["draft-release"]["needs"] == "build"
    assert jobs["publish"]["needs"] == "draft-release"
    assert jobs["release"]["needs"] == "publish"
    draft = "\n".join(step.get("run", "") for step in jobs["draft-release"]["steps"])
    assert "--draft" in draft.split("gh release create", 1)[1]
    assert "--draft=false" in jobs["release"]["steps"][-1]["run"]


def test_only_the_release_jobs_can_write_to_the_repository():
    """`contents: write` is what a compromised step would need to rewrite a
    release; it is granted to the two jobs that need it and no other."""
    workflow_ = workflow()
    assert workflow_["permissions"] == {"contents": "read"}
    writers = {
        name
        for name, job in workflow_["jobs"].items()
        if (job.get("permissions") or {}).get("contents") == "write"
    }
    assert writers == {"draft-release", "release"}
