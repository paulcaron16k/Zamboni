"""The reference Lakehouse App, end to end, its failure cases included (#174).

docs/maintenance-runtime.md §7.1 and §9, proven by running them: the app's
compose stack (examples/lakehouse-app/compose.yaml) on the dev stack, the real
image, the real Lakekeeper, and the maintenance tier and its workers killed and
degraded from outside.

Opt-in like tests/test_dev_stack.py: skipped without the dev stack or Docker,
failing with ``ZAMBONI_REQUIRE_DEV_STACK=1``. It builds the app's image and
starts its two services; the first run builds for a few minutes.

Each test registers warehouses of its own -- the registry name, a namespace in
Lakekeeper -- and removes them after, so a sweep never wanders into another
test's tables.
"""

from __future__ import annotations

import itertools
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("psycopg")  # the app's driver; CI's consumer job has none

# isort: split
import pyarrow as pa

from tests.conftest import SCHEMA, batch
from tests.test_dev_stack import stack_env, stack_up, unavailable
from zamboni import CatalogSession

ROOT = Path(__file__).resolve().parent.parent
APP = ROOT / "examples" / "lakehouse-app"
COMPOSE = APP / "compose.yaml"
sys.path.insert(0, str(APP))

from lakehouse_app.dashboard import STALE_AFTER  # noqa: E402
from lakehouse_app.db import Database  # noqa: E402
from lakehouse_app.registry import Registry, WarehouseRow  # noqa: E402
from lakehouse_app.request_table import RequestTable  # noqa: E402

#: Every minute: a sweep within a minute of registering, spread by at most
#: 5 % of the minute (zamboni.fleet.RANDOM_PCT).
EVERY_MINUTE = "* * * * *"
#: Never, in a test's lifetime.
NEW_YEAR = "0 0 1 1 *"
BANNER = "Maintenance unavailable"


def wait_until(condition: Callable[[], Any], *, seconds: float, what: str) -> Any:
    deadline = time.monotonic() + seconds
    while True:
        value = condition()
        if value:
            return value
        if time.monotonic() > deadline:
            pytest.fail(f"gave up after {seconds:.0f} s waiting for {what}")
        time.sleep(1)


class Stack:
    """The app's compose stack, from the test's side."""

    def __init__(self, env: dict[str, str]) -> None:
        self.env = env
        self.web_url = f"http://localhost:{os.environ.get('LAKEHOUSE_WEB_PORT', '8090')}"
        self.database = Database(
            f"host=localhost port={env['POSTGRES_PORT']} dbname={env['POSTGRES_DB']} "
            f"user={env['POSTGRES_USER']} password={env['POSTGRES_PASSWORD']}",
            "lakehouse",
        )
        self.registry = Registry(self.database)
        self.session = CatalogSession.for_lakekeeper(
            uri=f"http://localhost:{env['LAKEKEEPER_PORT']}/catalog",
            warehouse=env["WAREHOUSE_NAME"],
        )

    def compose(self, *args: str, workers: int | None = None) -> None:
        environ = {**os.environ}
        if workers is not None:
            environ["LAKEHOUSE_WORKERS"] = str(workers)
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), *args],
            check=True,
            cwd=ROOT,
            env=environ,
            capture_output=True,
        )

    def maintenance(self, *, replicas: int = 1, workers: int) -> None:
        """(Re)start the maintenance tier as ``replicas`` replicas of ``workers`` workers."""
        self.compose(
            "up",
            "-d",
            "--wait",
            "--scale",
            f"maintenance={replicas}",
            "maintenance",
            workers=workers,
        )

    def page(self, path: str) -> str:
        with urllib.request.urlopen(self.web_url + path, timeout=10) as response:
            assert response.status == 200, path
            return response.read().decode()

    # -- data --------------------------------------------------------------------

    def warehouse(self, tables: dict[str, int], *, schedule: str, rows: int = 10) -> Warehouse:
        """A warehouse of new tables, ``{name: files}``, registered under a fresh
        name; ``rows`` per file."""
        name = f"e2e_{uuid.uuid4().hex[:8]}"
        namespace = name
        self.session.catalog.create_namespace(namespace)
        for table, files in tables.items():
            created = self.session.catalog.create_table(
                f"{namespace}.{table}", schema=SCHEMA, properties={"format-version": "2"}
            )
            for f in range(files):
                created.append(big_batch(f * rows, rows) if rows > 1000 else batch(f * rows, rows))
        return Warehouse(self, name, namespace, list(tables), schedule)

    def runs(self, warehouse: str) -> list[dict[str, Any]]:
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT holder, table_identifier, reason, exit_code, died, dropped,"
                " record->>'started_at', record->>'ended_at', finished_at"
                " FROM zamboni_run WHERE warehouse = %s ORDER BY id",
                (warehouse,),
            ).fetchall()
        keys = (
            "holder",
            "table",
            "reason",
            "exit_code",
            "died",
            "dropped",
            "started",
            "ended",
            "finished",
        )
        return [dict(zip(keys, row, strict=True)) for row in rows]

    def claims(self, warehouse: str) -> list[tuple[str, str]]:
        with self.database.connect() as conn:
            return conn.execute(
                "SELECT table_identifier, holder FROM zamboni_claim"
                " WHERE warehouse = %s AND expires_at > now()",
                (warehouse,),
            ).fetchall()


def big_batch(start: int, count: int) -> pa.Table:
    """Rows wide enough that sorting a few million of them takes seconds."""
    return pa.table(
        {
            "id": pa.array(range(start, start + count), type=pa.int32()),
            "category": pa.array(["a"] * count),
            "payload": pa.array([f"payload-{i:012d}-{'x' * 40}" for i in range(count)]),
        }
    )


class Warehouse:
    def __init__(self, stack: Stack, name: str, namespace: str, tables: list[str], schedule: str):
        self.stack, self.name, self.namespace, self.schedule = stack, name, namespace, schedule
        self.tables = [f"{namespace}.{t}" for t in tables]
        self.table_config: dict[str, Any] = {
            "namespaces": {namespace: {"tables": {t: {} for t in tables}}}
        }

    def register(self, **table_settings: Any) -> None:
        if table_settings:
            for block in self.table_config["namespaces"][self.namespace]["tables"].values():
                block.update(table_settings)
        self.stack.registry.put(
            WarehouseRow(
                self.name,
                self.schedule,
                self.table_config,
                {
                    "type": "rest",
                    "uri": "http://lakekeeper:8181/catalog",
                    "warehouse": self.stack.env["WAREHOUSE_NAME"],
                },
            )
        )

    def remove(self) -> None:
        self.stack.registry.remove(self.name)
        for table in self.tables:
            if self.stack.session.catalog.table_exists(table):
                self.stack.session.catalog.purge_table(table)
        self.stack.session.catalog.drop_namespace(self.namespace)

    def runs(self) -> list[dict[str, Any]]:
        return self.stack.runs(self.name)

    def ran(self, *, since: datetime | None = None) -> list[dict[str, Any]]:
        """Runs that started a table: not those dropped before it started."""
        return [
            r
            for r in self.runs()
            if r["dropped"] is None and (since is None or r["finished"] >= since)
        ]


@pytest.fixture(scope="module")
def stack() -> Iterator[Stack]:
    env = stack_env()
    if env is None or not stack_up(env):
        unavailable("the dev stack is not up -- see dev-stack/README.md")
    if shutil.which("docker") is None:
        unavailable("no docker CLI to drive the app's compose stack with")
    s = Stack(env)
    try:
        s.compose("up", "-d", "--build", "--wait", "web", "maintenance", workers=2)
    except subprocess.CalledProcessError as exc:
        unavailable(f"the app's compose stack would not start: {exc.stderr.decode()[-2000:]}")
    try:
        yield s
    finally:
        s.compose("rm", "--stop", "--force", "web", "maintenance")
        s.session.close()


@pytest.fixture
def warehouses(stack: Stack) -> Iterator[list[Warehouse]]:
    """Warehouses a test makes, removed after it however it ends."""
    made: list[Warehouse] = []
    yield made
    for warehouse in made:
        warehouse.remove()


def no_table_ran_twice_at_once(runs: list[dict[str, Any]]) -> None:
    """For every table, no two runs' [started, ended) intervals overlap -- the
    claim protocol's promise, read from the run records."""
    by_table: dict[str, list[tuple[datetime, datetime, str]]] = {}
    for run in runs:
        if run["started"] and run["ended"]:
            interval = (
                datetime.fromisoformat(run["started"]),
                datetime.fromisoformat(run["ended"]),
                run["holder"],
            )
            by_table.setdefault(run["table"], []).append(interval)
    for table, intervals in by_table.items():
        intervals.sort()
        for (_, end, a), (start, _, b) in itertools.pairwise(intervals):
            assert start >= end, f"{table} ran on {a} and {b} at once"


def test_the_overlap_check_catches_two_runs_at_once():
    """The suite's central assertion, shown able to fail: without it, a
    claim store that admitted two holders would pass every case above."""

    def run(holder: str, start: str, end: str) -> dict[str, Any]:
        return {"holder": holder, "table": "raw.a", "started": start, "ended": end}

    no_table_ran_twice_at_once(
        [
            run("a", "2026-01-01T00:00:00", "2026-01-01T00:00:05"),
            run("b", "2026-01-01T00:00:05", "2026-01-01T00:00:09"),
        ]
    )
    with pytest.raises(AssertionError, match="ran on a and b at once"):
        no_table_ran_twice_at_once(
            [
                run("a", "2026-01-01T00:00:00", "2026-01-01T00:00:05"),
                run("b", "2026-01-01T00:00:04", "2026-01-01T00:00:09"),
            ]
        )


# -- §9.3: two maintenance replicas, one claim per table -----------------------------------


def test_two_replicas_claim_each_table_exactly_once_at_a_time(stack, warehouses):
    """Every replica fires and offers; exactly one wins each table's claim and
    the other drops it as `claimed elsewhere` (§9.3). Read back from the runs:
    both replicas did work, both dropped tables the other held, and no table
    was ever maintained by two at once."""
    stack.maintenance(replicas=2, workers=2)
    try:
        wh = stack.warehouse({f"t{i}": 3 for i in range(8)}, schedule=EVERY_MINUTE)
        warehouses.append(wh)
        wh.register()

        def swept() -> bool:
            done = {r["table"] for r in wh.ran() if r["exit_code"] == 0}
            dropped = [r for r in wh.runs() if r["dropped"] == "claimed elsewhere"]
            return done == set(wh.tables) and bool(dropped)

        wait_until(swept, seconds=240, what="a sweep by both replicas")
        stack.registry.remove(wh.name)
        wait_until(lambda: not stack.claims(wh.name), seconds=120, what="claims released")
    finally:
        stack.maintenance(replicas=1, workers=2)

    runs = wh.runs()
    no_table_ran_twice_at_once(runs)
    holders = {r["holder"] for r in runs}
    assert len(holders) == 2, f"both replicas took part: {holders}"
    assert {r["holder"] for r in runs if r["dropped"] == "claimed elsewhere"}, (
        "a replica found a table the other held"
    )


# -- §3.4: "run now" during a sweep starts on the reserved slot ----------------------------


def test_run_now_during_a_sweep_starts_ahead_of_the_queue(stack, warehouses):
    """Two workers, one reserved for high-class work (`reserved_high_slots`, 1
    by default): a schedule sweep -- low class -- runs its tables one at a
    time on the other, and a "run now" -- manual, high -- starts at once on
    the reserved one rather than behind the sweep's queue."""
    stack.maintenance(workers=2)
    sweep = stack.warehouse({f"s{i}": 3 for i in range(10)}, schedule=EVERY_MINUTE)
    asked = stack.warehouse({"now": 3}, schedule=NEW_YEAR)
    warehouses.extend([sweep, asked])
    asked.register()
    sweep.register()

    wait_until(lambda: stack.claims(sweep.name), seconds=150, what="the sweep to start")
    request = RequestTable(stack.database).submit(asked.name, asked.tables, requested_by="e2e")
    wait_until(
        lambda: [r for r in asked.ran() if r["exit_code"] == 0],
        seconds=120,
        what="the requested table to finish",
    )
    (manual,) = asked.ran()
    assert manual["reason"] == "manual"
    assert RequestTable(stack.database).outcome(request).startswith("offered")

    wait_until(
        lambda: {r["table"] for r in sweep.ran()} == set(sweep.tables),
        seconds=240,
        what="the sweep to finish",
    )
    started = datetime.fromisoformat(manual["started"])
    later = [r for r in sweep.ran() if datetime.fromisoformat(r["started"]) > started]
    assert later, "the sweep still had tables queued when the request started"
    assert all(r["reason"] == "schedule" for r in sweep.ran())


# -- §7.1: a worker killed as an OOM would kill it ---------------------------------------


def test_a_worker_killed_mid_table_reports_died_and_the_sweep_goes_on(stack, warehouses):
    """One worker; SIGKILL it inside the container while it compacts a table
    large enough to take seconds -- the kernel's OOM kill, which is SIGKILL. The
    table reports `died`, the runtime replaces the worker, and the sweep's
    other tables still run."""
    stack.maintenance(workers=1)
    wh = stack.warehouse({"big": 30}, schedule=EVERY_MINUTE, rows=500_000)
    small = stack.warehouse({"a": 3, "b": 3}, schedule=EVERY_MINUTE)
    warehouses.extend([wh, small])
    # Sorted, so the rewrite takes seconds: 9 s on a developer's machine.
    wh.register(ordering={"mode": "sort", "sort": [{"column": "payload", "direction": "desc"}]})
    small.register()

    big = wh.tables[0]
    wait_until(
        lambda: any(t == big for t, _ in stack.claims(wh.name)),
        seconds=150,
        what="the big table to start",
    )
    time.sleep(4)  # past the worker's acknowledgement, into the rewrite
    assert any(t == big for t, _ in stack.claims(wh.name)), "still running"
    killed = kill_workers(stack)
    assert killed, "there was a worker to kill"

    died = wait_until(
        lambda: [r for r in wh.ran() if r["died"]], seconds=60, what="the table's death"
    )
    assert died[0]["died"] == "worker died (killed by signal 9)"
    assert died[0]["exit_code"] == 1
    wait_until(
        lambda: {r["table"] for r in small.ran() if r["exit_code"] == 0} == set(small.tables),
        seconds=180,
        what="the other tables to run after the kill",
    )


def kill_workers(stack: Stack) -> list[int]:
    """SIGKILL every worker process in the maintenance container: the children
    the runtime spawned, found by their spawn command line -- never this
    script, whose own command line names what it looks for."""
    script = (
        "import os, signal\n"
        "killed = []\n"
        "for pid in filter(str.isdigit, os.listdir('/proc')):\n"
        "    try:\n"
        "        cmd = open(f'/proc/{pid}/cmdline', 'rb').read()\n"
        "    except OSError:\n"
        "        continue\n"
        "    if int(pid) != os.getpid() and cmd.startswith(b'/opt/venv/bin/python')"
        " and b'spawn_main' in cmd:\n"
        "        os.kill(int(pid), signal.SIGKILL)\n"
        "        killed.append(int(pid))\n"
        "print(killed)\n"
    )
    out = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE),
            "exec",
            "-T",
            "maintenance",
            "python",
            "-c",
            script,
        ],
        check=True,
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout
    return [int(p) for p in re.findall(r"\d+", out)]


# -- §9.2: the maintenance tier killed, the web tier carrying on -------------------------


def every_page(stack: Stack, wh: Warehouse) -> dict[str, str]:
    request = RequestTable(stack.database).submit(wh.name, wh.tables[:1], requested_by="e2e")
    return {
        path: stack.page(path)
        for path in ("/", f"/warehouses/{wh.name}", "/warehouses/new", f"/requests/{request}")
    }


def test_with_the_maintenance_tier_killed_the_web_tier_carries_on(stack, warehouses):
    """SIGKILL the whole maintenance tier: every page still renders, from the
    database, and once its tick is stale says "maintenance unavailable since".
    Restarted, it resumes, and no table is maintained twice at once -- a table
    the dead replica held stays claimed until its lease expires."""
    stack.maintenance(workers=2)
    wh = stack.warehouse({f"k{i}": 3 for i in range(3)}, schedule=EVERY_MINUTE)
    warehouses.append(wh)
    wh.register()
    wait_until(lambda: wh.ran(), seconds=150, what="a first run")
    assert all(BANNER not in text for text in every_page(stack, wh).values())

    stack.compose("kill", "-s", "SIGKILL", "maintenance")
    for path, text in every_page(stack, wh).items():
        assert "<h1>" in text, f"{path} renders with maintenance dead"
    stale = STALE_AFTER.total_seconds() + 90  # three missed ticks, and a poll's slack
    wait_until(lambda: BANNER in stack.page("/"), seconds=stale, what="the banner after the kill")
    for path, text in every_page(stack, wh).items():
        assert f"{BANNER} since" in text, path

    before = {r["holder"] for r in wh.runs()}
    stack.maintenance(workers=2)
    wait_until(
        lambda: {r["holder"] for r in wh.ran()} - before,
        seconds=150,
        what="maintenance to resume under a new replica",
    )
    wait_until(lambda: BANNER not in stack.page("/"), seconds=90, what="the banner to clear")
    no_table_ran_twice_at_once(wh.runs())
