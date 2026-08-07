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


#: Overrides for pointing the whole module at a deployment other than the dev
#: stack. `scripts/verify-live.py` sets these; nothing else does. Keeping one
#: implementation and two entry points is the point -- the alternative was a
#: second script reimplementing every operation, and this codebase has already
#: had a bug reintroduced by exactly that kind of duplicate.
OVERRIDES = {
    "LAKEKEEPER_PORT": "ZAMBONI_VERIFY_PORT",
    "WAREHOUSE_NAME": "ZAMBONI_VERIFY_WAREHOUSE",
    "S3_GATEWAY": "ZAMBONI_VERIFY_S3_HOST",
    "MINIO_PORT": "ZAMBONI_VERIFY_S3_PORT",
    "MINIO_CONSOLE_PORT": "ZAMBONI_VERIFY_S3_CONSOLE_PORT",
}


def stack_env() -> dict[str, str] | None:
    values = {k: v for k, v in dotenv_values(ENV_FILE).items() if v} if ENV_FILE.exists() else {}
    for key, var in OVERRIDES.items():
        if override := os.environ.get(var):
            values[key] = override
    # A target given entirely by environment needs no .env at all, which is what
    # lets these tests run against a deployment this repo did not create.
    required = ("LAKEKEEPER_PORT", "WAREHOUSE_NAME")
    return values if all(values.get(k) for k in required) else None


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


def _drop_namespace(session, namespace: str) -> None:
    """Remove a namespace and everything in it, reporting what it cannot."""
    with contextlib.suppress(NoSuchNamespaceError):
        for table in session.catalog.list_tables(namespace):
            with contextlib.suppress(NoSuchTableError):
                session.catalog.drop_table(table)
    try:
        session.catalog.drop_namespace(namespace)
    except NoSuchNamespaceError:
        pass
    except Exception as exc:  # pragma: no cover - reported, not swallowed
        pytest.fail(f"left namespace {namespace} behind in the target warehouse: {exc}")


@pytest.fixture
def table(session, request):
    """A partitioned table in a namespace unique to this test run.

    Cleanup is registered with `addfinalizer` immediately after the namespace
    exists, *before* the appends that may fail. A yield fixture only tears down
    if it reaches its yield, so when setup raised -- which it does against a
    warehouse that refuses writes, exactly the case worth diagnosing -- the
    namespace leaked into somebody else's catalog. Five of them, before this.
    """
    namespace = f"zt_{uuid.uuid4().hex[:8]}"
    session.catalog.create_namespace(namespace)
    request.addfinalizer(lambda: _drop_namespace(session, namespace))

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
    return identifier


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


#: Queues Lakekeeper OSS is known to ship, none of which does table maintenance.
#: `tabular_expiration` is easy to misread: it expires soft-deleted *tables* after
#: a delay (`default_tabular_expiration_delay_seconds`, 7 days), not snapshots.
KNOWN_QUEUES = {
    "tabular_expiration",
    "tabular_purge",
    "task_log_cleanup",
    "soft_deletion",  # the name latest-main uses
}


def test_lakekeeper_ships_no_maintenance_queues(env):
    """The premise of this whole project, asserted against the running server.

    Checked as an allow-list rather than by searching for words like "compact".
    A substring test passes for any queue whose name we did not think of, which
    makes it read as verification while proving very little -- and the claim
    being defended is about data-file compaction, snapshot expiry and orphan
    removal specifically.
    """
    r = requests.get(f"http://localhost:{env['LAKEKEEPER_PORT']}/management/v1/info", timeout=10)
    queues = set(r.json().get("queues", []))
    assert queues, "no queues reported; the info endpoint changed shape"

    unexpected = queues - KNOWN_QUEUES
    assert not unexpected, (
        f"Lakekeeper grew queue(s) we have not assessed: {sorted(unexpected)}. "
        "If any of them compacts data files, expires snapshots or removes orphans, "
        "the justification in docs/design.md needs revisiting."
    )


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


def test_metadata_properties_can_be_applied(session, table):
    """The one operation verify-live.py checked and these tests did not.

    It writes table properties rather than data, so it exercises a different
    commit path from every other operation here -- and against a REST catalog
    that path is the server's, not PyIceberg's.
    """
    from zamboni.properties import PROP_PREVIOUS_VERSIONS_MAX, apply_metadata_properties
    from zamboni.tableconfig import MetadataSettings

    result = apply_metadata_properties(
        session.table(table), MetadataSettings(previous_versions_max=2)
    )
    assert [c.key for c in result.changes] == [PROP_PREVIOUS_VERSIONS_MAX]
    assert session.table(table).properties[PROP_PREVIOUS_VERSIONS_MAX] == "2"


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
    """Point the demo at the dev stack, in a namespace it can safely clear.

    Skipped against any other target. Unlike every other test here, the demo
    uses a *fixed* namespace (`healthims`) and clears it -- safe on a stack this
    repo created, not safe on a warehouse that might already have one.
    """
    if os.environ.get("ZAMBONI_VERIFY_WAREHOUSE"):
        pytest.skip(
            "the demo writes a fixed `healthims` namespace and clears it, so it only "
            "runs against this repo's own dev stack, not an external target"
        )
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


# -- Trino (ZMBNI-14) -----------------------------------------------------
#
# Optional: Trino is in its own compose profile and does not start with the rest
# of the stack. These skip when its port is closed, even under
# ZAMBONI_REQUIRE_DEV_STACK -- requiring a JVM for every dev-stack run would
# make the common case slower to serve the rarer one.


def trino_up(env: dict[str, str]) -> bool:
    port = env.get("TRINO_PORT")
    if not port:
        return False
    try:
        return requests.get(f"http://localhost:{port}/v1/info", timeout=3).ok
    except Exception:
        return False


@pytest.fixture(scope="module")
def trino_env(env: dict[str, str]) -> dict[str, str]:
    if not trino_up(env):
        pytest.skip(
            "Trino not running -- cd dev-stack && docker compose --profile trino up -d trino"
        )
    return env


@pytest.fixture
def trino_table(trino_env, session, request):
    """A table with six data files, in a namespace unique to this run.

    Same addfinalizer-before-the-appends discipline as the `table` fixture, and
    for the same reason recorded there.
    """
    namespace = f"zt14_{uuid.uuid4().hex[:8]}"
    session.catalog.create_namespace(namespace)
    request.addfinalizer(lambda: _drop_namespace(session, namespace))

    identifier = f"{namespace}.events"
    tbl = session.catalog.create_table(
        identifier, schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(6):
        tbl.append(
            pa.table(
                {
                    "id": pa.array([i * 2, i * 2 + 1], type=pa.int32()),
                    "category": pa.array(["a", "b"], type=pa.string()),
                },
                schema=ARROW,
            )
        )
    return identifier


def maintainer(trino_env, session):
    from zamboni.maintainers.trino import TrinoMaintainer

    return TrinoMaintainer(
        session,
        {
            "host": "localhost",
            "port": trino_env["TRINO_PORT"],
            "catalog": "iceberg",
            "version": trino_env.get("TRINO_VERSION"),
        },
    )


def live_request():
    from zamboni.maintainers import MaintenanceRequest
    from zamboni.tableconfig import (
        ExpireSnapshotsSettings,
        MetadataSettings,
        RemoveOrphanFilesSettings,
        Retention,
    )

    # 7 days for both, because Trino refuses anything below its configured
    # min-retention floor -- which is the point of the config-translation check
    # in test_maintainers.py, exercised here against the server that enforces it.
    return MaintenanceRequest(
        retention=Retention(
            expire_snapshots=ExpireSnapshotsSettings(
                enabled=True, max_snapshot_age_days=7, min_snapshots_to_keep=2
            ),
            remove_orphan_files=RemoveOrphanFilesSettings(enabled=True, older_than_days=7),
            metadata=MetadataSettings(previous_versions_max=3, delete_after_commit=True),
        )
    )


@pytest.mark.parametrize(
    "operation_name",
    ["compact", "rewrite-manifests", "apply-properties", "expire", "remove-orphans"],
)
def test_trino_accepts_every_statement_we_generate(trino_env, session, trino_table, operation_name):
    """The claim the unit tests cannot make.

    They assert we emit the SQL we intended; only a server can say whether it is
    accepted. Two defects were found exactly here and nowhere else: the Iceberg
    metadata property names are refused by Trino outright, and `retain_last` did
    not exist before Trino 479.
    """
    from zamboni.maintainers import Operation

    operation = Operation(operation_name)
    result = maintainer(trino_env, session).execute(
        operation, trino_table, request=live_request(), dry_run=False
    )

    assert operation.value in result.describe()


def test_trino_compaction_actually_compacts(trino_env, session, trino_table):
    """Not just accepted -- effective."""
    from zamboni.maintainers import Operation

    before = len(session.table(trino_table).inspect.files().to_pylist())
    assert before == 6

    maintainer(trino_env, session).execute(
        Operation.COMPACT, trino_table, request=live_request(), dry_run=False
    )

    tbl = session.table(trino_table)
    assert len(tbl.inspect.files().to_pylist()) == 1
    assert tbl.scan().to_arrow().num_rows == 12, "compaction changed the data"


def test_trino_enforces_the_retention_floor_we_validate_against(trino_env, session, trino_table):
    """Pins the premise of TrinoMaintainer.validate(). If a deployment lowers
    the floor this fails, which is the right outcome: the guidance would then be
    wrong for that deployment."""
    from trino.exceptions import TrinoUserError

    from zamboni.maintainers.trino import qualified

    target = qualified(trino_table, catalog="iceberg")
    statement = f"ALTER TABLE {target} EXECUTE expire_snapshots(retention_threshold => '1d')"

    with pytest.raises(TrinoUserError, match="minimum retention"):
        maintainer(trino_env, session).connect().cursor().execute(statement)


# -- Spark over Spark Connect (ZMBNI-913) ---------------------------------
#
# Same opt-in posture as Trino: its own compose profile, skipped when the port
# is closed even under ZAMBONI_REQUIRE_DEV_STACK.
#
# Connect rather than a standalone master, and that choice is what makes these
# tests cheap enough to have at all. The client is `pyspark-client`, ~1.5MB of
# pure Python against pyspark's ~434MB, and it starts no JVM -- so "run the
# Spark tests" costs a gRPC connection here and a container there, instead of a
# JVM in the pytest process and a Java version this project has to pin.


def spark_up(env: dict[str, str]) -> bool:
    """A TCP connect, not an HTTP GET: Connect speaks gRPC on this port."""
    import socket

    port = env.get("SPARK_CONNECT_PORT")
    if not port:
        return False
    try:
        with socket.create_connection(("localhost", int(port)), timeout=3):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def spark_env(env: dict[str, str]) -> dict[str, str]:
    if not spark_up(env):
        pytest.skip(
            "Spark Connect not running -- cd dev-stack && "
            "docker compose --profile spark up -d --wait spark"
        )
    pytest.importorskip("pyspark", reason="install zamboni[spark-connect]")
    return env


@pytest.fixture
def spark_table(spark_env, session, request):
    """Six data files, in a namespace unique to this run."""
    namespace = f"zs913_{uuid.uuid4().hex[:8]}"
    session.catalog.create_namespace(namespace)
    request.addfinalizer(lambda: _drop_namespace(session, namespace))

    identifier = f"{namespace}.events"
    tbl = session.catalog.create_table(
        identifier, schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(6):
        tbl.append(
            pa.table(
                {
                    "id": pa.array([i * 2, i * 2 + 1], type=pa.int32()),
                    "category": pa.array(["a", "b"], type=pa.string()),
                },
                schema=ARROW,
            )
        )
    return identifier


def spark_maintainer(spark_env, session):
    from zamboni.maintainers.spark import SparkMaintainer

    return SparkMaintainer(
        session,
        {"remote": f"sc://localhost:{spark_env['SPARK_CONNECT_PORT']}", "catalog": "iceberg"},
    )


def spark_request(**overrides):
    from zamboni.maintainers import MaintenanceRequest
    from zamboni.tableconfig import (
        ExpireSnapshotsSettings,
        MetadataSettings,
        RemoveOrphanFilesSettings,
        Retention,
    )

    # 7 days for both. Spark's own floor is 24 hours for remove_orphan_files and
    # nothing for expire, but the point here is not to probe the floors -- that
    # is `validate()`'s job and test_maintainers.py's -- it is to run what a
    # sane config produces.
    return MaintenanceRequest(
        retention=Retention(
            expire_snapshots=ExpireSnapshotsSettings(
                enabled=True, max_snapshot_age_days=7, min_snapshots_to_keep=2
            ),
            remove_orphan_files=RemoveOrphanFilesSettings(enabled=True, older_than_days=7),
            metadata=MetadataSettings(previous_versions_max=3, delete_after_commit=True),
        ),
        **overrides,
    )


@pytest.mark.parametrize(
    "operation_name",
    [
        "compact",
        "rewrite-manifests",
        "apply-properties",
        "expire",
        "remove-orphans",
        "remove-dangling-deletes",
    ],
)
@pytest.mark.spark
def test_spark_accepts_every_statement_we_generate(spark_env, session, spark_table, operation_name):
    """The claim the unit tests cannot make.

    They assert we emit the SQL we intended; only a server says whether it is
    accepted. Every argument-shape defect in this maintainer was found here and
    could not have been found anywhere else: `date_sub(...)` rejected by the
    parser, a bare string rejected by the type checker, and the 24-hour floor.
    """
    from zamboni.maintainers import Operation

    operation = Operation(operation_name)
    result = spark_maintainer(spark_env, session).execute(
        operation, spark_table, request=spark_request(), dry_run=False
    )

    assert operation.value in result.describe()


@pytest.mark.spark
def test_spark_compaction_actually_compacts(spark_env, session, spark_table):
    """Not just accepted -- effective."""
    from zamboni.maintainers import Operation

    assert len(session.table(spark_table).inspect.files().to_pylist()) == 6

    spark_maintainer(spark_env, session).execute(
        Operation.COMPACT, spark_table, request=spark_request(), dry_run=False
    )

    tbl = session.table(spark_table)
    assert len(tbl.inspect.files().to_pylist()) == 1
    assert tbl.scan().to_arrow().num_rows == 12, "compaction changed the data"


@pytest.mark.spark
def test_zorder_is_accepted_by_the_server(spark_env, session, spark_table):
    """The one thing Spark does that Trino cannot, so nothing else can cover it.

    `sort_order => 'zorder(id, category)'` is a string Spark parses itself; a
    unit test can only assert we wrote it.
    """
    from zamboni.config import CompactionConfig
    from zamboni.maintainers import Operation

    request = spark_request(compaction=CompactionConfig(zorder_columns=("id", "category")))
    result = spark_maintainer(spark_env, session).execute(
        Operation.COMPACT, spark_table, request=request, dry_run=False
    )

    assert "zorder(id, category)" in result.statement
    assert session.table(spark_table).scan().to_arrow().num_rows == 12


@pytest.mark.spark
def test_only_remove_orphans_previews(spark_env, session, spark_table):
    """`can_preview` is per operation because of this asymmetry; here the server
    agrees rather than us asserting it about ourselves."""
    from zamboni.maintainers import Operation, PreviewUnavailable

    maintainer = spark_maintainer(spark_env, session)

    result = maintainer.execute(
        Operation.REMOVE_ORPHANS, spark_table, request=spark_request(), dry_run=True
    )
    assert "dry_run => true" in result.statement

    with pytest.raises(PreviewUnavailable):
        maintainer.execute(Operation.COMPACT, spark_table, request=spark_request(), dry_run=True)


@pytest.mark.spark
def test_remove_orphans_reaches_storage_on_the_servers_own_credentials(
    spark_env, session, spark_table
):
    """The operation that fails alone when the S3A configuration is wrong.

    Every other operation reads and writes through Iceberg FileIO on
    Lakekeeper's vended STS credentials. `remove_orphan_files` lists through
    Hadoop's S3A filesystem, which knows nothing about Iceberg and needs its own
    static keys on the *server* -- a Connect client cannot supply them, because
    `spark.hadoop.*` is read when the server builds its Hadoop configuration.

    A dry run is enough: listing is the part that needs the credentials, and it
    happens before anything is deleted. If S3A is misconfigured this raises
    while the other five operations keep passing, which is exactly the confusing
    symptom the assertion exists to name.
    """
    from zamboni.maintainers import Operation

    result = spark_maintainer(spark_env, session).execute(
        Operation.REMOVE_ORPHANS, spark_table, request=spark_request(), dry_run=True
    )

    assert "remove_orphan_files" in result.statement
    assert session.table(spark_table).scan().to_arrow().num_rows == 12


@pytest.mark.spark
def test_the_expiry_timestamp_is_read_as_the_instant_we_meant(spark_env, session):
    """Why this stack's Spark session is deliberately **not** UTC.

    Zamboni computes `older_than` on the client and sends it as a typed literal.
    Spark reads a bare wall-clock in `spark.sql.session.timeZone`, so on a
    UTC server a literal missing its offset is indistinguishable from a correct
    one -- a UTC dev stack would certify the ZMBNI-1507 bug as working.

    Two assertions, and the second is the one that keeps the first honest: our
    literal must land on the intended instant, *and* the naive form must land on
    a different one. If someone sets this server to UTC, the second assertion
    fails and says why.
    """
    import datetime as dt

    from zamboni.maintainers.spark import _days_ago, _utc_now

    spark = spark_maintainer(spark_env, session).connect()
    zone = spark.conf.get("spark.sql.session.timeZone")
    assert zone != "UTC", (
        f"this server is on {zone}; a UTC session cannot tell a correct "
        "timestamp literal from one missing its offset"
    )

    expected = _utc_now() - dt.timedelta(days=7)
    literal = _days_ago(7)  # TIMESTAMP '... +00:00'
    naive = literal.replace("+00:00", "")

    ours, without_offset = spark.sql(
        f"SELECT unix_timestamp({literal}) AS a, unix_timestamp({naive}) AS b"
    ).collect()[0]

    assert abs(ours - expected.timestamp()) <= 1, "our literal is not the instant we computed"
    assert ours != without_offset, (
        "the offset made no difference, so this test cannot detect its loss"
    )
