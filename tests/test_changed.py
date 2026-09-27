# SPDX-License-Identifier: Apache-2.0
"""Deriving the partitions written to since maintenance last ran.

Two derivations with one answer, and the tests are mostly about the ways this
must fail *wide*. It decides whether to skip work, so being wrong towards
"nothing changed" leaves a partition uncompacted -- which is the condition this
tool exists to prevent, arrived at by an optimisation meant to help.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

from zamboni.changed import (
    PARTITION_LIMIT_PROPERTY,
    UNKNOWN,
    changed_partitions,
)
from zamboni.compactor import TableCompactor
from zamboni.config import CompactionConfig

SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "category", StringType(), required=False),
)
ARROW = pa.schema(
    [pa.field("id", pa.int32(), nullable=True), pa.field("category", pa.string(), nullable=True)]
)
SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
)


def rows(categories):
    return pa.table(
        {
            "id": pa.array(range(len(categories)), type=pa.int32()),
            "category": pa.array(list(categories)),
        },
        schema=ARROW,
    )


@pytest.fixture(params=["summary", "manifests"])
def table(request, session):
    """The same table twice, once per derivation path.

    Parametrised rather than duplicated because the whole point is that the two
    paths answer identically -- `write.summary.partition-limit` is an
    optimisation, not a different meaning.
    """
    properties = {"format-version": "2"}
    if request.param == "summary":
        properties[PARTITION_LIMIT_PROPERTY] = "100"

    tbl = session.catalog.create_table(
        f"db.{request.param}", schema=SCHEMA, partition_spec=SPEC, properties=properties
    )
    for _ in range(3):
        tbl.append(rows("abcd"))
    TableCompactor(session, f"db.{request.param}", CompactionConfig(min_input_files=2)).execute()
    tbl = session.catalog.load_table(f"db.{request.param}")
    tbl.expected_source = request.param
    return tbl


def reload(session, tbl):
    return session.catalog.load_table(".".join(tbl.name()))


def test_both_paths_find_the_same_partitions(session, table):
    """`write.summary.partition-limit` is an optimisation, not a second
    meaning."""
    table.append(rows("aac"))

    changed = changed_partitions(reload(session, table))

    assert changed.paths == frozenset({"category=a", "category=c"})
    assert changed.source == table.expected_source
    assert changed.snapshots == 1


def test_the_cheap_path_opens_no_manifest(session):
    """The reason the property is worth setting. `Snapshot.manifests` raising
    proves it rather than a timing being suggestive -- the same technique as
    `test_no_manifest_is_read` in the health suite."""
    from pyiceberg.table.snapshots import Snapshot

    tbl = session.catalog.create_table(
        "db.cheap",
        schema=SCHEMA,
        partition_spec=SPEC,
        properties={"format-version": "2", PARTITION_LIMIT_PROPERTY: "100"},
    )
    for _ in range(2):
        session.catalog.load_table("db.cheap").append(rows("abcd"))
    TableCompactor(session, "db.cheap", CompactionConfig(min_input_files=2)).execute()
    tbl = session.catalog.load_table("db.cheap")
    tbl.append(rows("aa"))
    tbl = session.catalog.load_table("db.cheap")

    original = Snapshot.manifests
    Snapshot.manifests = lambda self, io: pytest.fail("a manifest was read")
    try:
        changed = changed_partitions(tbl)
    finally:
        Snapshot.manifests = original

    assert changed.paths == frozenset({"category=a"})
    assert changed.source == "summary"


def test_no_data_file_is_read_on_either_path(session, table):
    """The acceptance's own words. The partition lives in the manifest entry;
    opening a Parquet file to learn it would be a different operation with a
    different cost."""
    table.append(rows("abc"))
    tbl = reload(session, table)

    opened: list[str] = []
    original = tbl.io.new_input
    tbl.io.new_input = lambda location: (opened.append(location), original(location))[1]

    changed_partitions(tbl)

    assert not [path for path in opened if path.endswith(".parquet")], (
        f"a data file was opened: {opened}"
    )


# -- failing wide --------------------------------------------------------


def test_unknown_is_not_the_empty_set(session, table):
    """The one confusion that turns this from a saving into a loss: an empty
    set says "compact nothing", unknown says "compact whatever you would
    have"."""
    assert UNKNOWN.paths is None
    assert UNKNOWN.known is False
    assert UNKNOWN.covers("category=anything") is True

    nothing_changed = changed_partitions(reload(session, table))
    assert nothing_changed.paths == frozenset()
    assert nothing_changed.known is True
    assert nothing_changed.covers("category=a") is False


def test_a_table_never_maintained_is_unknown(session):
    """No watermark, so "since when" has no answer and every partition is
    potentially stale."""
    tbl = session.catalog.create_table(
        "db.fresh", schema=SCHEMA, partition_spec=SPEC, properties={"format-version": "2"}
    )
    tbl.append(rows("abcd"))

    changed = changed_partitions(session.catalog.load_table("db.fresh"))

    assert changed.known is False
    assert "never maintained" in changed.source


def test_an_unpartitioned_table_is_unknown_rather_than_empty(session, unpartitioned):
    """One bucket, so targeting has nothing to say -- and saying "nothing
    changed" would stop compacting the table entirely."""
    changed = changed_partitions(session.catalog.load_table("db.unpartitioned"))

    assert changed.known is False
    assert changed.covers("()") is True


def test_one_snapshot_without_partition_summaries_disqualifies_the_cheap_path(session):
    """All or nothing. A partial union reads as authoritative and is not, which
    is worse than falling back to the manifests."""
    tbl = session.catalog.create_table(
        "db.mixed",
        schema=SCHEMA,
        partition_spec=SPEC,
        properties={"format-version": "2", PARTITION_LIMIT_PROPERTY: "100"},
    )
    for _ in range(2):
        session.catalog.load_table("db.mixed").append(rows("abcd"))
    TableCompactor(session, "db.mixed", CompactionConfig(min_input_files=2)).execute()

    tbl = session.catalog.load_table("db.mixed")
    tbl.append(rows("a"))
    # A later writer that does not record partition summaries.
    with tbl.transaction() as txn:
        txn.set_properties(**{PARTITION_LIMIT_PROPERTY: "0"})
    tbl = session.catalog.load_table("db.mixed")
    tbl.append(rows("b"))
    tbl = session.catalog.load_table("db.mixed")

    changed = changed_partitions(tbl)

    assert changed.source == "manifests", "it must not report a partial union as complete"
    assert changed.paths == frozenset({"category=a", "category=b"})


def test_metadata_trouble_answers_unknown_rather_than_raising(session, table, monkeypatch):
    """This sits in front of compaction. Anything it cannot answer must widen
    the candidate set, never narrow it and never stop the run."""
    import zamboni.changed as changed_module

    def explode(*args, **kwargs):
        raise RuntimeError("catalog is having a day")

    monkeypatch.setattr(changed_module, "maintenance_watermark", explode)

    assert changed_partitions(reload(session, table)) == UNKNOWN


def test_a_burst_of_writes_is_one_union(session, table):
    """Several snapshots since the watermark, not one."""
    for categories in ("a", "b", "a"):
        reload(session, table).append(rows(categories))

    changed = changed_partitions(reload(session, table))

    assert changed.paths == frozenset({"category=a", "category=b"})
    assert changed.snapshots == 3
