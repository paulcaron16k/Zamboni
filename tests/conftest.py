"""Docker-free fixtures: a SQL catalog over a temp-directory warehouse."""

from __future__ import annotations

from typing import NamedTuple

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

from zamboni import CatalogSession

SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "category", StringType(), required=False),
    NestedField(3, "payload", StringType(), required=False),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int32(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
        pa.field("payload", pa.string(), nullable=True),
    ]
)


@pytest.fixture
def session(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    sess = CatalogSession.for_local(warehouse_path=str(warehouse))
    sess.catalog.create_namespace_if_not_exists("db")
    yield sess
    sess.close()


def batch(start: int, count: int, category: str = "a") -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(start, start + count), type=pa.int32()),
            "category": pa.array([category] * count, type=pa.string()),
            "payload": pa.array([f"p{i}" for i in range(start, start + count)], type=pa.string()),
        },
        schema=ARROW_SCHEMA,
    )


@pytest.fixture
def unpartitioned(session):
    """Six small appends -> six live data files."""
    tbl = session.catalog.create_table(
        "db.unpartitioned", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(6):
        tbl.append(batch(i * 10, 10))
    return tbl


@pytest.fixture
def partitioned(session):
    """Identity-partitioned on category, several small files per partition."""
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
    )
    tbl = session.catalog.create_table(
        "db.partitioned",
        schema=SCHEMA,
        partition_spec=spec,
        properties={"format-version": "2"},
    )
    for i in range(4):
        tbl.append(pa.concat_tables([batch(i * 10, 10, "a"), batch(500 + i * 10, 10, "b")]))
    return tbl


def add_position_deletes(tbl, target_path: str, positions: list[int]):
    """Attach a position delete file to ``target_path``.

    Delegates to :mod:`zamboni.testing`, which the demo also uses -- PyIceberg
    has no supported way to produce a merge-on-read table, so the one
    implementation of that workaround is shared rather than duplicated.
    """
    from zamboni.testing import add_position_deletes as _add

    return _add(tbl, target_path, positions, name_hint="test")


class MorFixture(NamedTuple):
    table: object
    deleted_ids: list[int]


@pytest.fixture
def mor_table(session) -> MorFixture:
    """A merge-on-read table: 40 physical rows, 3 removed by a position delete.

    Reports which ids the delete actually removed. Positions are file-relative,
    and which file sorts first is not something a test should assume, so the
    fixture reads them back rather than hardcoding.
    """
    import pyarrow.parquet as pq

    tbl = session.catalog.create_table("db.mor", schema=SCHEMA, properties={"format-version": "2"})
    for i in range(4):
        tbl.append(batch(i * 10, 10))
    tbl = session.catalog.load_table("db.mor")

    target = sorted(t.file.file_path for t in tbl.scan().plan_files())[0]
    positions = [0, 1, 2]
    with tbl.io.new_input(target).open() as fh:
        ids = pq.read_table(fh)["id"].to_pylist()
    deleted_ids = [ids[p] for p in positions]

    add_position_deletes(tbl, target, positions)
    return MorFixture(session.catalog.load_table("db.mor"), deleted_ids)


@pytest.fixture
def sorted_table(session):
    """A table with a declared sort order: id DESC NULLS LAST."""
    order = SortOrder(
        SortField(
            source_id=1,
            transform=IdentityTransform(),
            direction=SortDirection.DESC,
            null_order=NullOrder.NULLS_LAST,
        )
    )
    tbl = session.catalog.create_table(
        "db.sorted",
        schema=SCHEMA,
        sort_order=order,
        properties={"format-version": "2"},
    )
    for i in range(4):
        tbl.append(batch(i * 10, 10))
    return tbl


@pytest.fixture
def bucketed(session):
    """Bucket-partitioned: the transform PyIceberg's add_files path cannot infer."""
    spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=BucketTransform(num_buckets=2), name="id_bucket"
        )
    )
    tbl = session.catalog.create_table(
        "db.bucketed",
        schema=SCHEMA,
        partition_spec=spec,
        properties={"format-version": "2"},
    )
    for i in range(4):
        tbl.append(batch(i * 10, 10))
    return tbl
