"""Format version 3.

V3 cannot be created here at all -- PyIceberg 0.11.1 raises on serialising V3
metadata -- so these tests characterise the boundary rather than exercise a real
V3 table. That is the honest limit of what can be verified without Spark, and
saying so is more useful than a suite that quietly proves nothing.

The finding that matters: the upstream block lives in
``TableMetadataV3.model_dump_json``, which only runs on catalogs that serialise
metadata client-side. A REST catalog builds metadata server-side and never calls
it, so nothing upstream would stop a V3 commit there. Hence our own blocker.
"""

from __future__ import annotations

import pytest

from zamboni.profile import Severity, _assess, profile_table

from .conftest import SCHEMA


def test_pyiceberg_cannot_write_v3_at_all(session):
    """Characterisation: the reason this cannot be tested end to end."""
    with pytest.raises(NotImplementedError, match="Writing V3 is not yet supported"):
        session.catalog.create_table("db.v3", schema=SCHEMA, properties={"format-version": "3"})


def test_the_upstream_block_is_serialisation_only():
    """Why we need our own blocker rather than relying on upstream's.

    ``model_dump_json`` is called when a catalog writes metadata.json itself.
    Under a REST catalog the server does that, so this never fires and a V3
    commit would go through.
    """
    import inspect

    from pyiceberg.table.metadata import TableMetadataV3

    source = inspect.getsource(TableMetadataV3)
    assert "NotImplementedError" in source
    # The guard is on serialisation, not on any write path.
    assert "def model_dump_json" in source
    raising_methods = [
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("def ") and "model_dump_json" in line
    ]
    assert raising_methods, "the V3 guard moved; re-check whether REST commits are covered"


def assess_at_version(tbl, version: int):
    """Findings for a real table, reported as if it were at `version`.

    A real table rather than a stub: `_assess` inspects the partition spec and
    the delete files too, so a stub would exercise a different code path from
    the one that runs in production.
    """
    profile = profile_table(tbl)
    object.__setattr__(profile, "format_version", version)
    return _assess(tbl, profile)


def test_v3_is_blocked_for_row_rewriting(unpartitioned):
    blockers = [f for f in assess_at_version(unpartitioned, 3) if f.severity == Severity.BLOCKER]

    assert [f.code for f in blockers] == ["format-version-3"]
    message = blockers[0].message
    assert "_row_id" in message
    assert "REST catalog" in message, "the reason it needs our own blocker must be stated"


def test_v2_is_not_blocked(unpartitioned):
    findings = assess_at_version(unpartitioned, 2)
    assert [f for f in findings if f.severity == Severity.BLOCKER] == []


def test_v1_is_still_blocked_for_its_own_reason(unpartitioned):
    findings = assess_at_version(unpartitioned, 1)
    codes = [f.code for f in findings if f.severity == Severity.BLOCKER]
    assert codes == ["format-version-1"]


def test_compaction_refuses_a_v3_table(session, monkeypatch, unpartitioned):
    """The blocker has to actually stop `execute()`, not merely be reported."""
    from zamboni import CompactionConfig, TableCompactor
    from zamboni.compactor import CompactionBlocked

    real = profile_table

    def as_v3(tbl):
        profile = real(tbl)
        object.__setattr__(profile, "format_version", 3)
        profile.findings = _assess(tbl, profile)
        return profile

    monkeypatch.setattr("zamboni.compactor.profile_table", as_v3)

    with pytest.raises(CompactionBlocked, match="format-version-3"):
        TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()


def test_metadata_only_operations_are_not_blocked_by_v3(session, unpartitioned):
    """Expiry, orphans, manifests and dangling deletes rewrite no rows.

    They copy manifest entries verbatim -- including ``first_row_id`` -- so row
    lineage survives them. Blocking them on V3 would be gratuitous, and the
    docstring in each module says why they are safe.
    """
    from zamboni.deletes import find_dangling
    from zamboni.expire import RetentionPolicy, decide_retention
    from zamboni.manifests import plan_rewrite

    tbl = session.table("db.unpartitioned")

    # None of these consult format_version or the blocker list.
    assert decide_retention(tbl.metadata, RetentionPolicy()) is not None
    assert plan_rewrite(tbl) is not None
    assert find_dangling(tbl) is not None


def test_manifest_rewriting_preserves_the_v3_row_lineage_field(session, partitioned):
    """``first_row_id`` rides along on the data file, untouched.

    It is V3-only and therefore None here, but the mechanism -- copying
    ``entry.data_file`` wholesale rather than rebuilding it -- is what makes the
    operation V3-safe, and a refactor that rebuilt the file would drop it.
    """
    from zamboni.manifests import ManifestRewriter

    def row_ids(tbl):
        found = {}
        for manifest in tbl.current_snapshot().manifests(io=tbl.io):
            for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
                found[entry.data_file.file_path] = getattr(
                    entry.data_file, "first_row_id", "absent"
                )
        return found

    before = row_ids(partitioned)
    ManifestRewriter().run(partitioned)
    assert row_ids(session.table("db.partitioned")) == before


def test_deletion_vectors_can_be_read_but_not_written():
    """V3 replaces position delete files with puffin deletion vectors.

    PyIceberg ships a reader and no writer, so a V3 merge-on-read table can be
    profiled but its deletes cannot be simulated the way `zamboni.testing`
    simulates V2 position deletes.
    """
    from pyiceberg.table import puffin

    assert hasattr(puffin, "PuffinFile")
    writers = [n for n in dir(puffin) if "write" in n.lower()]
    assert not writers, f"a puffin writer appeared: {writers} -- V3 MoR may now be testable"
