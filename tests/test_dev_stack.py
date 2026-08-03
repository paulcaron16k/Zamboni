"""The dev stack, and the demo running on it.

Every test here **skips** when the stack is not reachable, so `pytest` stays
Docker-free by default. Bring it up with:

    cd dev-stack && docker compose up -d && uv run bootstrap.py

The point of these tests is not that Lakekeeper works -- that is Lakekeeper's
business. It is that the *stack is configured such that Zamboni can do its job*,
which is a narrower and more easily broken thing. Storage reclamation needs
`ListObjectsV2`, `HeadObject` and `DeleteObject`, and a Lakekeeper warehouse can
be configured to refuse all three; that configuration reads as perfectly healthy
until you try to free a byte. So the assertions below are mostly about
credentials and permissions rather than about round-tripping data.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from pathlib import Path

import pyarrow as pa
import pytest
import requests
from dotenv import dotenv_values
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

from zamboni import CatalogSession, CompactionConfig, TableCompactor
from zamboni.deletes import DanglingDeleteCleaner
from zamboni.expire import RetentionPolicy, SnapshotExpirer
from zamboni.manifests import ManifestRewriter
from zamboni.orphans import OrphanCleaner, list_storage, storage_roots
from zamboni.reachable import reachable_files

#: Set in CI. Without it a stack that never came up makes every test here skip,
#: and a suite of skips is indistinguishable from a suite of passes -- which is
#: precisely the failure mode these tests exist to catch elsewhere.
REQUIRE = "ZAMBONI_REQUIRE_DEV_STACK"

DEV_STACK = Path(__file__).resolve().parent.parent / "dev-stack"
ENV_FILE = DEV_STACK / ".env"
SAMPLE_FILE = DEV_STACK / ".env.sample"

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


# -- reachability ---------------------------------------------------------


def unavailable(reason: str):
    """Skip locally, fail in CI."""
    if os.environ.get(REQUIRE):
        pytest.fail(f"{REQUIRE} is set but the dev stack is unusable: {reason}")
    pytest.skip(reason)


def stack_env() -> dict[str, str] | None:
    if not ENV_FILE.exists():
        return None
    values = {k: v for k, v in dotenv_values(ENV_FILE).items() if v}
    return values or None


def stack_up(env: dict[str, str]) -> bool:
    try:
        r = requests.get(f"http://localhost:{env['LAKEKEEPER_PORT']}/management/v1/info", timeout=3)
        return r.ok and r.json().get("bootstrapped", False)
    except Exception:
        return False


@pytest.fixture(scope="module")
def env() -> dict[str, str]:
    values = stack_env()
    if values is None:
        unavailable(f"no {ENV_FILE}; copy .env.sample to run the dev-stack tests")
    if not stack_up(values):
        unavailable(
            "dev stack not reachable or not bootstrapped -- "
            "cd dev-stack && docker compose up -d && uv run bootstrap.py"
        )
    return values


@pytest.fixture(scope="module")
def warehouse(env) -> dict:
    r = requests.get(
        f"http://localhost:{env['LAKEKEEPER_PORT']}/management/v1/warehouse", timeout=10
    )
    r.raise_for_status()
    for w in r.json().get("warehouses", []):
        if w["name"] == env["WAREHOUSE_NAME"]:
            return w
    unavailable(f"warehouse {env['WAREHOUSE_NAME']} not found -- run dev-stack/bootstrap.py")


@pytest.fixture
def session(env) -> CatalogSession:
    s = CatalogSession.for_lakekeeper(
        uri=f"http://localhost:{env['LAKEKEEPER_PORT']}/catalog",
        warehouse=env["WAREHOUSE_NAME"],
    )
    yield s
    s.close()


@pytest.fixture
def table(session):
    """A partitioned table in a namespace unique to this test run."""
    namespace = f"zt_{uuid.uuid4().hex[:8]}"
    session.catalog.create_namespace(namespace)
    identifier = f"{namespace}.events"
    tbl = session.catalog.create_table(
        identifier, schema=SCHEMA, partition_spec=SPEC, properties={"format-version": "2"}
    )
    for i in range(4):
        tbl.append(
            pa.table(
                {
                    "id": pa.array([i * 2, i * 2 + 1], type=pa.int32()),
                    "category": pa.array(["a", "b"], type=pa.string()),
                },
                schema=ARROW,
            )
        )
    yield identifier
    for drop in (
        lambda: session.catalog.drop_table(identifier),
        lambda: session.catalog.drop_namespace(namespace),
    ):
        with contextlib.suppress(NoSuchTableError, NoSuchNamespaceError):
            drop()


# -- the stack itself -----------------------------------------------------


def test_env_sample_and_env_declare_the_same_keys():
    """Drift here is the classic dev-stack failure: .env.sample stops being
    a usable template and a new checkout comes up misconfigured."""
    if not ENV_FILE.exists():
        unavailable(f"no {ENV_FILE}")
    sample = set(dotenv_values(SAMPLE_FILE))
    actual = set(dotenv_values(ENV_FILE))
    assert sample == actual, (
        f"only in .env.sample: {sorted(sample - actual)}; only in .env: {sorted(actual - sample)}"
    )


def test_the_ui_is_served_by_lakekeeper_itself(env):
    """There is no separate UI image; Lakekeeper serves it at /ui/."""
    r = requests.get(f"http://localhost:{env['LAKEKEEPER_PORT']}/ui/", timeout=10)
    assert r.ok
    assert "<!doctype html" in r.text[:200].lower()


def test_lakekeeper_ships_no_maintenance_queues(env):
    """The premise of this whole project, asserted against the running server.

    If a compaction, expiry or orphan queue ever appears in Lakekeeper OSS, the
    justification in docs/design.md needs revisiting -- so fail loudly rather
    than let the claim quietly go stale.
    """
    r = requests.get(f"http://localhost:{env['LAKEKEEPER_PORT']}/management/v1/info", timeout=10)
    queues = set(r.json().get("queues", []))
    assert not any("compact" in q for q in queues), f"a compaction queue appeared: {queues}"
    assert not any("orphan" in q for q in queues), f"an orphan queue appeared: {queues}"
    assert not any("snapshot" in q for q in queues), f"a snapshot queue appeared: {queues}"


def test_the_warehouse_vends_credentials_rather_than_signing(warehouse):
    """The single setting that decides whether storage can be reclaimed.

    A warehouse with `sts-enabled: false` makes Lakekeeper remote-sign each
    request, and its signer refuses ListObjectsV2, HeadObject and multi-object
    DELETE -- so compaction fails and nothing can be freed. Note that
    `remote-signing-enabled` may *also* be true; STS takes precedence. It is the
    presence of STS that matters, not the absence of signing.
    """
    profile = warehouse["storage-profile"]
    assert profile["sts-enabled"] is True, (
        "this warehouse cannot reclaim storage. Recreate it with sts-enabled: "
        "see dev-stack/bootstrap.py and docs/live-verification.md"
    )
    assert profile["flavor"] == "s3-compat", "MinIO needs flavor s3-compat for STS"


def test_the_advertised_endpoint_is_reachable_from_the_host(env, warehouse):
    """Lakekeeper advertises one endpoint to every client, and per-table storage
    properties beat anything the client configures -- so an in-cluster hostname
    like `http://minio:9000` is unusable from a laptop and no flag can fix it.
    The dev stack advertises the compose gateway, which both sides can reach."""
    endpoint = warehouse["storage-profile"]["endpoint"].rstrip("/")
    assert env["S3_GATEWAY"] in endpoint, (
        f"endpoint {endpoint} is not the compose gateway; host clients will fail"
    )
    r = requests.get(f"{endpoint}/minio/health/live", timeout=10)
    assert r.ok, f"{endpoint} is not reachable from the host"


def test_vended_credentials_include_a_session_token(session, table):
    """What STS actually delivers, and why LIST/DELETE work.

    With STS on, Lakekeeper returns temporary credentials per table and does not
    configure a signer at all -- so the client gets PyArrowFileIO holding real
    keys. Without it, `py-io-impl` is forced to FsspecFileIO with an
    `S3V4RestSigner` and the reclaim operations cannot run.
    """
    tbl = session.table(table)
    assert type(tbl.io).__name__ == "PyArrowFileIO", (
        f"got {type(tbl.io).__name__}; the warehouse is probably remote-signing"
    )
    assert "s3.session-token" in tbl.io.properties, "no vended session token"
    assert "s3.signer" not in tbl.io.properties


# -- the permissions the reclaim operations need --------------------------


def test_bucket_listing_works(session, table):
    """Orphan removal needs `ListObjectsV2`, and nothing else does."""
    tbl = session.table(table)
    found = list_storage(tbl, storage_roots(tbl))
    assert found, "listing returned nothing; orphan removal cannot compute a complement"

    referenced = reachable_files(tbl).paths
    assert referenced <= set(found), (
        f"{len(referenced - set(found))} referenced file(s) missing from the listing"
    )


def test_object_deletion_works(session, table):
    """Expiry deletes files directly. A signer that refuses DELETE turns this
    into a silent no-op that still reports success."""
    TableCompactor(session, table, CompactionConfig()).execute()

    result = SnapshotExpirer(RetentionPolicy(max_snapshot_age_ms=0, min_snapshots_to_keep=1)).run(
        session.table(table)
    )

    assert result.deleted_files > 0, "expiry deleted nothing"
    assert result.failed_deletes == 0, (
        f"{result.failed_deletes} deletion(s) refused -- check the signing configuration"
    )


# -- the whole lifecycle, against the real stack --------------------------


def test_every_operation_runs_and_preserves_the_data(session, table):
    tbl = session.table(table)
    before = sorted(tbl.scan().to_arrow()["id"].to_pylist())
    assert len(before) == 8

    compacted = TableCompactor(session, table, CompactionConfig()).execute()
    assert compacted.rewritten_data_files == 8

    DanglingDeleteCleaner().run(session.table(table))
    ManifestRewriter().run(session.table(table))
    SnapshotExpirer(RetentionPolicy(max_snapshot_age_ms=0, min_snapshots_to_keep=1)).run(
        session.table(table)
    )
    orphans = OrphanCleaner(older_than_days=0).run(session.table(table))
    assert orphans.failed == 0, f"{orphans.failed} orphan deletion(s) refused"

    tbl = session.table(table)
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == before
    assert len(tbl.current_snapshot().manifests(io=tbl.io)) < 4


# -- the demo on the stack ------------------------------------------------


@pytest.fixture
def demo_env(env, monkeypatch, tmp_path):
    """Point the demo at the dev stack, in a namespace it can safely clear."""
    import shutil

    from himsdemo.state import TOTAL_DAYS

    source = Path(__file__).resolve().parent.parent / "data" / "healthims"
    root = tmp_path / "healthims"
    root.mkdir()
    for name in ("table_schema.json", "table-config.json"):
        shutil.copy(source / name, root / name)
    for day in range(1, TOTAL_DAYS + 1):
        shutil.copytree(source / f"day{day}", root / f"day{day}")

    monkeypatch.setenv("ZAMBONI_URI", f"http://localhost:{env['LAKEKEEPER_PORT']}/catalog")
    monkeypatch.setenv("ZAMBONI_WAREHOUSE", env["WAREHOUSE_NAME"])
    monkeypatch.delenv("ZAMBONI_DEMO_CATALOG", raising=False)
    return root


def test_the_demo_runs_on_the_dev_stack(demo_env, capsys):
    """The demo's own tables, on Lakekeeper and MinIO rather than SQLite.

    One day rather than five: this exercises the catalog switch and the full
    maintenance triad, and the five-day progression is already covered locally.
    """
    from himsdemo.catalogs import NAMESPACE
    from himsdemo.cli import main

    def run(*argv):
        return main(["--root", str(demo_env), "--catalog", "lakekeeper", *argv])

    assert run("clear") == 0
    assert run("next-day") == 0
    capsys.readouterr()

    session = CatalogSession.for_lakekeeper(
        uri=os.environ["ZAMBONI_URI"], warehouse=os.environ["ZAMBONI_WAREHOUSE"]
    )
    try:
        before = {
            name: session.table(f"{NAMESPACE}.{name}").scan().count()
            for name in ("hims_employees", "hims_discharge", "hims_events")
        }
        assert before["hims_events"] == 107, before
        files_before = len(
            session.table(f"{NAMESPACE}.hims_events")
            .current_snapshot()
            .manifests(io=session.table(f"{NAMESPACE}.hims_events").io)
        )
        assert files_before == 11, f"expected 11 hourly commits, got {files_before}"
    finally:
        session.close()

    assert run("maintenance", "--reclaim-now") == 0
    out = capsys.readouterr().out
    assert "expired" in out and "rewrote" in out

    session = CatalogSession.for_lakekeeper(
        uri=os.environ["ZAMBONI_URI"], warehouse=os.environ["ZAMBONI_WAREHOUSE"]
    )
    try:
        after = {
            name: session.table(f"{NAMESPACE}.{name}").scan().count()
            for name in ("hims_employees", "hims_discharge", "hims_events")
        }
        assert after == before, "maintenance on the dev stack changed the data"
        events = session.table(f"{NAMESPACE}.hims_events")
        assert len(events.current_snapshot().manifests(io=events.io)) == 1
    finally:
        session.close()

    assert run("clear") == 0


def test_the_demo_defaults_to_sqlite_with_no_stack(tmp_path, monkeypatch):
    """`./bin/demo` must keep working with nothing running and nothing set."""
    from himsdemo import catalogs
    from himsdemo.state import DemoState

    monkeypatch.delenv("ZAMBONI_DEMO_CATALOG", raising=False)
    assert catalogs.resolve_backend(None) == "sqlite"
    catalog = catalogs.build("sqlite", DemoState(root=tmp_path))
    assert not catalog.is_remote


def test_selecting_lakekeeper_without_config_explains_itself(tmp_path, monkeypatch):
    from himsdemo import catalogs
    from himsdemo.state import DemoState

    monkeypatch.delenv("ZAMBONI_URI", raising=False)
    monkeypatch.delenv("ZAMBONI_WAREHOUSE", raising=False)

    with pytest.raises(catalogs.CatalogUnavailable, match="docker compose up"):
        catalogs.build("lakekeeper", DemoState(root=tmp_path))
