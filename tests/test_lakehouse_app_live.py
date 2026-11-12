"""The reference Lakehouse App's maintenance tier against PostgreSQL (#171).

The dev stack's PostgreSQL, a schema of its own per test, as
tests/test_dev_stack.py's reference stores use. Skipped without the stack;
failing with ``ZAMBONI_REQUIRE_DEV_STACK=1``, which is how CI's dev-stack job
runs it.

§9.5's conformance, for the app's own implementations: its claim store (point
1), its worker entries (point 2) and its request table (point 5). Then point
4: an edit to the registry reaches a running runtime through ``update_fleet``,
and re-arms the warehouse's schedule.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest

# The app needs psycopg, which the dev group has and CI's consumer job, a bare
# install of the wheel, does not: there these skip rather than error.
pytest.importorskip("psycopg")

# isort: split
from tests.conftest import SCHEMA, batch
from tests.test_dev_stack import postgres_schemas  # noqa: F401 -- a fixture, by name
from tests.test_runtime import QUIET, FakePool, eventually
from zamboni import CatalogSession
from zamboni.conformance import (
    CLAIM_STORE_CHECKS,
    ENTRY_CHECKS,
    HEALTH_ENTRY_CHECKS,
    REQUEST_SOURCE_CHECKS,
    ClaimStoreHarness,
    EntryHarness,
    RequestSourceHarness,
)
from zamboni.pool import PoolSize, WorkerConfig
from zamboni.runtime import start_in_thread
from zamboni.scheduler import WorkItem
from zamboni.tableconfig import TableConfig

APP = Path(__file__).resolve().parent.parent / "examples" / "lakehouse-app"
sys.path.insert(0, str(APP))

from lakehouse_app.claims import PostgresClaimStore  # noqa: E402
from lakehouse_app.db import Database  # noqa: E402
from lakehouse_app.maintenance import RegistryWatcher, build  # noqa: E402
from lakehouse_app.registry import Registry, WarehouseRow  # noqa: E402
from lakehouse_app.request_table import RequestTable  # noqa: E402

#: A credential the entry is given and must never repeat.
SECRET = "lakehouse-catalog-token-do-not-print"


@pytest.fixture
def new_database(postgres_schemas):  # noqa: F811
    """A fresh, migrated app database per call."""
    conninfo, new_schema = postgres_schemas

    def make() -> Database:
        database = Database(conninfo, new_schema("lakehouse"))
        database.migrate()
        return database

    return make


# -- §9.5 point 1: the claim store ---------------------------------------------------------


@pytest.mark.parametrize("check", CLAIM_STORE_CHECKS, ids=lambda c: c.__name__)
def test_the_apps_claim_store_passes(new_database, check):
    check(
        ClaimStoreHarness(
            make_store=lambda: PostgresClaimStore(new_database()),
            now=lambda store: store.now(),
        )
    )


# -- §9.5 point 5: the request table -------------------------------------------------------


@pytest.mark.parametrize("check", REQUEST_SOURCE_CHECKS, ids=lambda c: c.__name__)
def test_the_apps_request_table_passes(new_database, check):
    check(RequestSourceHarness(make_table=lambda: RequestTable(new_database(), poll_every=0.1)))


# -- §9.5 point 2: the worker entries ------------------------------------------------------


TABLE_CONFIG = {"namespaces": {"db": {"tables": {"unpartitioned": {}}}}}


@pytest.fixture
def entry_harness(new_database, tmp_path, monkeypatch) -> EntryHarness:
    """A local SQL catalog registered as warehouse ``acme``, with six small
    files a compaction would rewrite; the entry finds it through the registry."""
    database = new_database()
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    # Named after the warehouse, as the entry names the catalog it loads: a
    # SQL catalog keeps its tables under its own name.
    with CatalogSession.for_local(warehouse_path=str(warehouse), name="acme") as session:
        session.catalog.create_namespace("db")
        table = session.catalog.create_table(
            "db.unpartitioned", schema=SCHEMA, properties={"format-version": "2"}
        )
        for i in range(6):
            table.append(batch(i * 10, 10))
        catalog = {
            "type": "sql",
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        }
    Registry(database).put(WarehouseRow("acme", "0 2 * * *", TABLE_CONFIG, catalog))
    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)  # no developer's zamboni.yml or .env is read

    def table_state() -> object:
        with CatalogSession.for_local(warehouse_path=str(warehouse), name="acme") as s:
            return s.catalog.load_table("db.unpartitioned").metadata_location

    password = dict(part.split("=", 1) for part in database.url.split() if "=" in part).get(
        "password"
    )
    return EntryHarness(
        entry="lakehouse_app.entry:run",
        health_entry="lakehouse_app.entry:check",
        item=WorkItem(
            "acme",
            "db.unpartitioned",
            "schedule",
            TableConfig.from_dict({"warehouse": "acme", **TABLE_CONFIG}),
        ),
        table_state=table_state,
        base_environ={
            "PATH": os.environ.get("PATH", ""),
            "LAKEHOUSE_DATABASE_URL": database.url,
            "LAKEHOUSE_DATABASE_SCHEMA": database.schema,
            "LAKEHOUSE_CATALOG_TOKEN": SECRET,
        },
        secrets=tuple(s for s in (SECRET, password) if s),
        workdir=workdir,
    )


@pytest.mark.parametrize("check", ENTRY_CHECKS, ids=lambda c: c.__name__)
def test_the_apps_entry_passes(entry_harness, check):
    check(entry_harness)


@pytest.mark.parametrize("check", HEALTH_ENTRY_CHECKS, ids=lambda c: c.__name__)
def test_the_apps_health_entry_passes(entry_harness, check):
    check(entry_harness)


# -- §9.5 point 4: the registry reaches the runtime through update_fleet --------------------


RAW = {"namespaces": {"raw": {"tables": {"a": {}, "b": {}}}}}


def test_a_registry_edit_reaches_the_running_runtime_and_re_arms_its_schedule(
    new_database, tmp_path
):
    """Registered to fire on 1 January, the warehouse does not fire at 06:00.
    Edited in the registry to 04:00, the runtime adopts it and fires the next
    morning -- a schedule re-armed by an edit the runtime was told about,
    never by the runtime reading the registry itself."""
    database = new_database()
    registry = Registry(database)
    registry.put(WarehouseRow("acme", "0 2 1 1 *", RAW))
    now = [QUIET]  # 03:00 UTC

    async def step(stop: asyncio.Event, seconds: float) -> None:
        await asyncio.sleep(0.01)

    watcher = RegistryWatcher(registry, interval=0.05)
    pools: list[FakePool] = []
    runtime = build(
        database,
        watcher.first(registry.warehouses()),
        size=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: now[0],
        wait=step,
        pool_factory=lambda size, config: pools.append(FakePool(size, config)) or pools[-1],
    )
    handle = start_in_thread(runtime)
    watching = WatcherThread(watcher, runtime)
    try:
        now[0] = QUIET.replace(hour=6)
        time.sleep(0.5)
        # No entry at all until something happens to the warehouse.
        assert "acme" not in runtime.state().warehouses, "1 January is not today"

        registry.put(WarehouseRow("acme", "0 4 * * *", RAW))
        eventually(lambda: runtime.state().generation == 2)

        now[0] = QUIET.replace(hour=6) + timedelta(days=1)
        eventually(lambda: "acme" in runtime.state().warehouses)
        assert runtime.state().warehouses["acme"].last_fired is not None
        eventually(lambda: sorted(i.table for i in pools[0].items) == ["raw.a", "raw.b"])
    finally:
        watching.stop()
        assert handle.stop(grace=timedelta(seconds=5))

    with database.connect() as conn:
        (phase,) = conn.execute(
            "SELECT phase FROM zamboni_runtime WHERE holder = %s", (runtime.holder,)
        ).fetchone()
        runs = conn.execute("SELECT table_identifier FROM zamboni_run ORDER BY 1").fetchall()
    assert phase == "stopped", "the state rows followed the runtime"
    assert [r for (r,) in runs] == ["raw.a", "raw.b"]


class WatcherThread:
    """The registry watcher on a loop of its own: the app's side, calling the
    runtime's thread-safe control surface from outside the runtime's loop."""

    def __init__(self, watcher: RegistryWatcher, runtime) -> None:
        self._loop = asyncio.new_event_loop()
        self._stop = asyncio.Event()
        self._thread = threading.Thread(
            target=self._loop.run_until_complete, args=(watcher.run(runtime, self._stop),)
        )
        self._thread.start()

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._stop.set)
        self._thread.join(10)
        self._loop.close()


# -- the web tier (#172) -----------------------------------------------------------------


class Client:
    """The web tier's WSGI app, called directly: no server, no port."""

    def __init__(self, database: Database) -> None:
        from lakehouse_app.web import App

        self.app = App(database)

    def request(self, method: str, path: str, form: dict[str, str] | None = None):
        import io
        from urllib.parse import urlencode
        from wsgiref.util import setup_testing_defaults

        body = urlencode(form or {}).encode()
        environ: dict = {
            "REQUEST_METHOD": method,
            "PATH_INFO": path,
            "CONTENT_LENGTH": str(len(body)),
            "CONTENT_TYPE": "application/x-www-form-urlencoded",
            "wsgi.input": io.BytesIO(body),
        }
        setup_testing_defaults(environ)
        seen: dict = {}

        def start_response(status, headers):
            seen["status"], seen["headers"] = int(status.split()[0]), dict(headers)

        text = b"".join(self.app(environ, start_response)).decode()
        return seen["status"], seen["headers"], text

    def get(self, path: str) -> str:
        status, _, text = self.request("GET", path)
        assert status == 200, (path, status, text)
        return text

    def post(self, path: str, form: dict[str, str]) -> tuple[int, str]:
        status, headers, text = self.request("POST", path, form)
        return status, headers.get("Location", text)


BANNER = "Maintenance unavailable"


@pytest.fixture
def web(new_database):
    database = new_database()
    Registry(database).put(WarehouseRow("acme", "0 2 * * *", RAW))
    return database, Client(database)


def every_page(client: Client, database: Database) -> dict[str, str]:
    request = RequestTable(database).submit("acme", ["raw.a"], requested_by="test")
    return {
        path: client.get(path)
        for path in ("/", "/warehouses/acme", "/warehouses/new", f"/requests/{request}")
    }


def test_with_no_maintenance_tier_ever_every_page_renders_under_the_banner(web):
    database, client = web
    for path, text in every_page(client, database).items():
        assert f"{BANNER}: no maintenance replica has reported yet" in text, path


def test_with_the_maintenance_tier_stopped_every_page_says_since_when(web):
    """A SIGKILLed tier writes no `stopped`: its row still says `running`, and
    only the age of its last tick tells. That is what the banner reads."""
    database, client = web
    with database.connect() as conn:
        conn.execute(
            "INSERT INTO zamboni_runtime (holder, phase, last_tick, busy, queued, updated_at)"
            " VALUES ('dead-replica', 'running', now() - interval '1 hour', 1, 0, now())"
        )
    for path, text in every_page(client, database).items():
        assert f"{BANNER} since" in text, path
    assert client.get("/healthz") == "ok", "liveness never depends on maintenance"


def running_tier(database: Database, tmp_path, *, hold: bool = False):
    """A maintenance tier on this database, its pool a fake that can hold a table."""
    pools: list[FakePool] = []
    runtime = build(
        database,
        Registry(database).fleet(),
        size=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        pool_factory=lambda size, config: (
            pools.append(FakePool(size, config, hold=hold)) or pools[-1]
        ),
    )
    return runtime, pools


def test_with_the_maintenance_tier_running_there_is_no_banner(web, tmp_path):
    database, client = web
    runtime, _ = running_tier(database, tmp_path)
    handle = start_in_thread(runtime)
    try:
        eventually(lambda: BANNER not in client.get("/"), timeout=15)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert BANNER in client.get("/"), "and it comes back once the tier has stopped"


def test_run_now_writes_a_request_and_its_page_follows_the_run_to_the_end(web, tmp_path):
    """#172: "run now" is a row; the maintenance tier takes it, claims the
    table, runs it; the request's page shows it waiting, running, finished."""
    database, client = web
    status, location = client.post("/warehouses/acme/run", {"table": "raw.a"})
    assert status == 303 and location.startswith("/requests/"), (status, location)
    id = location.rsplit("/", 1)[1]
    with database.connect() as conn:
        row = conn.execute(
            "SELECT warehouse, tables, reason, requested_by FROM zamboni_request WHERE id = %s",
            (int(id),),
        ).fetchone()
    assert row == ("acme", ["raw.a"], "manual", "dashboard")
    assert "waiting for a maintenance replica" in client.get(location)

    runtime, pools = running_tier(database, tmp_path, hold=True)
    handle = start_in_thread(runtime)
    try:
        eventually(lambda: "running since" in client.get(location), timeout=15)
        assert "offered" in client.get(location)
        assert "raw.a" in client.get("/"), "and the dashboard lists it as running"
        pools[0].release.set()
        eventually(lambda: "finished" in client.get(location), timeout=15)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert ": ok" in client.get(location)
    assert [i.table for i in pools[0].items] == ["raw.a"], "only the table asked for"


def test_a_warehouse_is_registered_and_rescheduled_from_the_web_tier(web):
    database, client = web
    status, location = client.post(
        "/warehouses",
        {
            "name": "globex",
            "schedule": "30 1 * * *",
            "table_config": '{"namespaces": {"raw": {"tables": {"x": {}}}}}',
            "catalog": '{"type": "rest", "uri": "http://lakekeeper:8181/catalog"}',
        },
    )
    assert (status, location) == (303, "/warehouses/globex")
    assert client.post("/warehouses/globex/schedule", {"schedule": "0 5 * * *"})[0] == 303
    assert Registry(database).warehouse("globex").schedule == "0 5 * * *"
    assert "0 5 * * *" in client.get("/warehouses/globex")


@pytest.mark.parametrize(
    ("form", "why"),
    [
        ({"schedule": "*/0 * * * *"}, "globex"),
        ({"catalog": '{"credential": "id:secret"}'}, "credential"),
        ({"table_config": "not json"}, "not JSON"),
    ],
    ids=["schedule", "credential", "json"],
)
def test_a_registration_zamboni_would_refuse_is_refused_and_the_form_kept(web, form, why):
    _, client = web
    sent = {
        "name": "globex",
        "schedule": "0 2 * * *",
        "table_config": '{"namespaces": {"raw": {"tables": {"x": {}}}}}',
        "catalog": "{}",
        **form,
    }
    status, text = client.request("POST", "/warehouses", sent)[0::2]
    assert status == 400
    assert why in text
    assert 'value="globex"' in text, "what was typed is kept"


def test_what_a_row_holds_is_escaped_never_rendered(web):
    """Rows come from other people: a request's requester, a run's error."""
    database, client = web
    id = RequestTable(database).submit("acme", ["raw.a"], requested_by="<script>x</script>")
    text = client.get(f"/requests/{id}")
    assert "<script>x" not in text and "&lt;script&gt;x" in text
