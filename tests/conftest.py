"""Docker-free fixtures: a SQL catalog over a temp-directory warehouse."""

from __future__ import annotations

import os
import random
from collections.abc import Callable
from typing import Any, NamedTuple

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
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


def library_honours_spec_expected() -> bool:
    """Whether the *installed library* should write added files under their own spec.

    This repository resolves PyIceberg from the maintenance fork, which does.
    A consumer installs from PyPI and gets a build that does not, where
    `MultiSpecReplaceFiles` supplies the behaviour instead (ZMBNI-83).

    Both are supported and both are tested, so the expectation is declared by
    the environment rather than discovered from the build: a leg that silently
    resolved the wrong PyIceberg would otherwise agree with whatever it found
    and prove nothing. `ZAMBONI_EXPECT_BUILD=stock` is set by the consumer CI
    leg; unset means the fork, which is what a checkout has.
    """
    return os.environ.get("ZAMBONI_EXPECT_BUILD", "fork") != "stock"


def batch(start: int, count: int, category: str = "a") -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(start, start + count), type=pa.int32()),
            "category": pa.array([category] * count, type=pa.string()),
            "payload": pa.array([f"p{i}" for i in range(start, start + count)], type=pa.string()),
        },
        schema=ARROW_SCHEMA,
    )


#: Ways a catalog may list `metadata.snapshots`, none of them commit order.
#: Reversed and rotated are there because each moves every stamp's position,
#: so code reading position as commit order fails on them for certain; the
#: shuffles are what Lakekeeper actually does (ZMBNI-192).
REORDERINGS: dict[str, Callable[[list], list]] = {
    "reversed": lambda snapshots: snapshots[::-1],
    "rotated": lambda snapshots: snapshots[1:] + snapshots[:1],
    **{
        f"shuffled-{seed}": lambda snapshots, seed=seed: random.Random(seed).sample(
            snapshots, len(snapshots)
        )
        for seed in range(5)
    },
}


def reordered(tbl: Table, reorder: Callable[[list], list]) -> Table:
    """`tbl` as a catalog that does not keep snapshot order would return it.

    The SQL catalog these tests run on happens to list `metadata.snapshots` in
    append order, and Lakekeeper lists it in a different order on every load --
    so code that read position as commit order passed here and failed there.
    Only the list is rearranged; every snapshot, its sequence number included,
    is the one the catalog committed.
    """
    snapshots = reorder(list(tbl.metadata.snapshots))
    metadata = tbl.metadata.model_copy(update={"snapshots": snapshots})
    return Table(tbl.name(), metadata, tbl.metadata_location, tbl.io, tbl.catalog)


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


# -- an OTLP collector stand-in (#188, #181) --------------------------------------


class Collector:
    """What an OTLP/HTTP collector stand-in received, decoded on demand."""

    def __init__(self) -> None:
        #: (path, headers, body) per POST.
        self.requests: list[tuple[str, dict[str, str], bytes]] = []

    def metrics(self) -> list[tuple[str, Any]]:
        """Every (service.name, Metric) received, across all requests."""
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
            ExportMetricsServiceRequest,
        )

        found = []
        for path, _headers, body in self.requests:
            assert path == "/v1/metrics"
            for resource in ExportMetricsServiceRequest.FromString(body).resource_metrics:
                service = next(
                    (
                        a.value.string_value
                        for a in resource.resource.attributes
                        if a.key == "service.name"
                    ),
                    "",
                )
                for scope in resource.scope_metrics:
                    found.extend((service, metric) for metric in scope.metrics)
        return found

    def names(self) -> set[str]:
        return {metric.name for _, metric in self.metrics()}

    def services(self) -> set[str]:
        return {service for service, _ in self.metrics()}


@pytest.fixture
def collector(monkeypatch):
    """An OTLP/HTTP endpoint standing in for a collector: records each POST.

    Points the standard variables at it and clears the ones a developer's
    shell might carry, so the test sees only what it set. The environment is
    inherited by a subprocess, so a `zamboni serve` started from the test
    exports here too.
    """
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    received = Collector()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.requests.append((self.path, dict(self.headers), body))
            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    for name in (
        "OTEL_SERVICE_NAME",
        "OTEL_RESOURCE_ATTRIBUTES",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", f"http://127.0.0.1:{server.server_port}")
    yield received
    server.shutdown()
