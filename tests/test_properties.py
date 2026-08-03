"""metadata.json retention.

There is no operation here, only two table properties -- so the tests are
mostly *characterisation*: they pin down what Iceberg's own knobs actually do,
because the surprising one (trimming the log deletes nothing) is what leaks
files in practice.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from zamboni.orphans import OrphanCleaner
from zamboni.properties import (
    PROP_DELETE_AFTER_COMMIT,
    PROP_PREVIOUS_VERSIONS_MAX,
    apply_metadata_properties,
    desired_properties,
    unreferenced_metadata_files,
)
from zamboni.tableconfig import MetadataSettings

from .conftest import SCHEMA, batch


def metadata_json_on_disk(tbl) -> int:
    return len(list(Path(tbl.location().replace("file://", "")).rglob("*.metadata.json")))


def commit_a_few(tbl, session, name: str, n: int = 8):
    for i in range(n):
        tbl.append(batch(i, 1))
    return session.table(name)


# -- what the properties actually do -------------------------------------


def test_trimming_the_log_deletes_nothing(session):
    """The surprise worth a test: `previous-versions-max` manufactures orphans.

    It bounds the metadata *log*, not the files behind it. Every entry it drops
    becomes a metadata.json nothing points at -- which is where the demo's
    leaked files came from.
    """
    tbl = session.catalog.create_table(
        "db.trimmed",
        schema=SCHEMA,
        properties={"format-version": "2", PROP_PREVIOUS_VERSIONS_MAX: "2"},
    )
    tbl = commit_a_few(tbl, session, "db.trimmed")

    assert len(tbl.metadata.metadata_log) == 2, "the log was not trimmed"
    assert metadata_json_on_disk(tbl) == 9, "trimming unexpectedly deleted files"
    assert unreferenced_metadata_files(tbl) == 6


def test_delete_after_commit_is_what_removes_them(session):
    tbl = session.catalog.create_table(
        "db.trimmed_deleted",
        schema=SCHEMA,
        properties={
            "format-version": "2",
            PROP_PREVIOUS_VERSIONS_MAX: "2",
            PROP_DELETE_AFTER_COMMIT: "true",
        },
    )
    tbl = commit_a_few(tbl, session, "db.trimmed_deleted")

    assert len(tbl.metadata.metadata_log) == 2
    assert metadata_json_on_disk(tbl) == 3  # two logged plus the current one
    assert unreferenced_metadata_files(tbl) == 0


def test_the_default_keeps_everything(session):
    """No trimming, no deletion -- so nothing leaks either."""
    tbl = session.catalog.create_table(
        "db.untrimmed", schema=SCHEMA, properties={"format-version": "2"}
    )
    tbl = commit_a_few(tbl, session, "db.untrimmed")

    assert len(tbl.metadata.metadata_log) == 8
    assert unreferenced_metadata_files(tbl) == 0


def test_orphan_removal_reclaims_the_stranded_files(session):
    """The safe half of the pair, which is why deletion stays off by default."""
    tbl = session.catalog.create_table(
        "db.stranded",
        schema=SCHEMA,
        properties={"format-version": "2", PROP_PREVIOUS_VERSIONS_MAX: "2"},
    )
    tbl = commit_a_few(tbl, session, "db.stranded")
    assert unreferenced_metadata_files(tbl) == 6
    before_rows = tbl.scan().to_arrow().num_rows

    result = OrphanCleaner(older_than_days=0).run(tbl)

    assert result.deleted == 6
    tbl = session.table("db.stranded")
    assert unreferenced_metadata_files(tbl) == 0
    assert metadata_json_on_disk(tbl) == 3
    assert tbl.scan().to_arrow().num_rows == before_rows


# -- applying them -------------------------------------------------------


def test_unset_settings_touch_nothing(session, unpartitioned):
    """`None` means leave it alone, not 'reset to the default'.

    A table already tuned by hand must not be quietly reverted just because a
    config file exists that does not mention the property.
    """
    assert desired_properties(MetadataSettings()) == {}

    result = apply_metadata_properties(unpartitioned, MetadataSettings())
    assert result.changes == []
    assert "already as declared" in result.describe()


def test_declared_properties_are_set(session, unpartitioned):
    settings = MetadataSettings(previous_versions_max=5, delete_after_commit=False)

    result = apply_metadata_properties(unpartitioned, settings)

    assert {c.key for c in result.changes} == {PROP_PREVIOUS_VERSIONS_MAX, PROP_DELETE_AFTER_COMMIT}
    props = session.table("db.unpartitioned").properties
    assert props[PROP_PREVIOUS_VERSIONS_MAX] == "5"
    assert props[PROP_DELETE_AFTER_COMMIT] == "false"


def test_applying_twice_changes_nothing(session, unpartitioned):
    settings = MetadataSettings(previous_versions_max=5)
    apply_metadata_properties(unpartitioned, settings)

    result = apply_metadata_properties(session.table("db.unpartitioned"), settings)
    assert result.changes == []


def test_dry_run_changes_nothing(session, unpartitioned):
    settings = MetadataSettings(previous_versions_max=5)

    result = apply_metadata_properties(unpartitioned, settings, dry_run=True)

    assert result.dry_run and len(result.changes) == 1
    assert PROP_PREVIOUS_VERSIONS_MAX not in session.table("db.unpartitioned").properties


def test_a_zero_version_limit_is_rejected():
    from zamboni.tableconfig import TableConfigError

    with pytest.raises(TableConfigError, match="must be >= 1"):
        MetadataSettings(previous_versions_max=0).validate("retention.metadata")
