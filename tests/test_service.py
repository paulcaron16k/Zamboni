# SPDX-License-Identifier: Apache-2.0
"""`zamboni serve`: lifecycle, the state file, the probes, the run log.

Three layers. The state and probe tests are pure. The `Service.run` tests drive
the real assembly with a fake clock and a fake pool, so a firing happens
without waiting for one. The end-to-end test runs `zamboni serve` as a process
and talks to it the way Kubernetes and an operator would -- probes, SIGHUP,
SIGTERM -- because the claims about signals are claims about a process.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from zamboni.cli import main
from zamboni.pool import ItemResult, PoolSize, WorkerConfig
from zamboni.runlog import summarise
from zamboni.scheduler import WorkItem
from zamboni.service import (
    LIVENESS_STALE,
    AlreadyRunning,
    Service,
    ServiceState,
    probe,
    read_state,
    run_record,
    single_instance,
)
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

T0 = datetime(2026, 10, 2, 2, 0, tzinfo=UTC)


def work(table: str, warehouse: str = "acme") -> WorkItem:
    return WorkItem(
        warehouse=warehouse,
        table=table,
        reason="schedule",
        table_config=TableConfig(
            warehouse=warehouse,
            namespaces={"raw": NamespaceSettings(tables={table.split(".")[1]: TableSettings()})},
        ),
    )


def result(table: str, code: int = 0, died: str | None = None) -> ItemResult:
    record = None if died else {"warehouse": "acme", "tables": [table], "exit_code": code}
    return ItemResult(work(table), code, record, "", died, 1.5)


def fleet_file(tmp_path: Path, *tables: str, schedule: str = "* * * * *") -> Path:
    path = tmp_path / "fleet.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "warehouses": [
                    {
                        "name": "acme",
                        "schedule": schedule,
                        "random": False,
                        "table_config": {
                            "namespaces": {"raw": {"tables": {t: {} for t in tables}}}
                        },
                    }
                ]
            }
        )
    )
    return path


# -- sweeps -----------------------------------------------------------------------


def test_a_sweep_closes_when_every_table_it_offered_has_a_result():
    state = ServiceState(fleet_path="f", dry_run=True, workers=2)
    state.fired("acme", ["raw.a", "raw.b"], T0)
    state.finished(result("raw.a"), T0 + timedelta(minutes=1))
    assert "last_sweep" not in state.warehouses["acme"]

    state.finished(result("raw.b", code=3), T0 + timedelta(minutes=2))
    sweep = state.warehouses["acme"]["last_sweep"]
    assert sweep["tables"] == 2
    assert sweep["failures"] == 1
    assert sweep["worst_exit_code"] == 3
    assert sweep["finished_at"] == "2026-10-02T02:02:00Z"


def test_each_firing_is_its_own_sweep_and_one_run_answers_both():
    """Two firings while a table is still queued coalesce into one run, and that
    run answers both. A schedule faster than a sweep cannot hold one open."""
    state = ServiceState(fleet_path="f", dry_run=True, workers=1)
    state.fired("acme", ["raw.a"], T0)
    state.fired("acme", ["raw.a"], T0 + timedelta(minutes=1))
    assert state.warehouses["acme"]["open_sweeps"] == 2
    state.finished(result("raw.a"), T0 + timedelta(minutes=2))
    entry = state.warehouses["acme"]
    assert entry["open_sweeps"] == 0
    assert entry["last_sweep"]["fired_at"] == "2026-10-02T02:01:00Z"


def test_a_firing_that_asked_for_nothing_opens_no_sweep():
    """Every table was already in flight: the service passes an empty list."""
    state = ServiceState(fleet_path="f", dry_run=True, workers=1)
    state.fired("acme", [], T0)
    assert state.warehouses["acme"]["open_sweeps"] == 0
    assert "last_sweep" not in state.warehouses["acme"]


def test_a_table_in_flight_at_the_firing_is_not_in_its_sweep(tmp_path):
    """Its run started before the firing, so it cannot answer it -- left in, the
    sweep would never close, because the queue refused the offer."""
    pools: list[FakePool] = []

    async def scenario():
        hold = asyncio.Event()
        svc = service(tmp_path, "a", pools=pools, hold=hold)
        task = asyncio.create_task(svc.run())
        while not pools or not pools[0].ran:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)  # several more firings while raw.a is in flight
        hold.set()
        while "last_sweep" not in read_state(tmp_path / "serve.state.json")["warehouses"].get(
            "acme", {}
        ):
            await asyncio.sleep(0.01)
        svc.request_stop()
        return await asyncio.wait_for(task, 5)

    assert asyncio.run(scenario()) == 0


def test_a_reload_that_removes_a_pending_table_closes_the_sweep():
    """Otherwise a sweep waiting on a table the fleet no longer has never ends."""
    from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse

    state = ServiceState(fleet_path="f", dry_run=True, workers=1)
    state.fired("acme", ["raw.a", "raw.gone"], T0)
    state.finished(result("raw.a"), T0)
    fleet = FleetConfig(
        warehouses=(FleetWarehouse("acme", CronSchedule("0 2 * * *"), work("raw.a").table_config),)
    )
    state.adopt(fleet, T0 + timedelta(minutes=1))
    assert state.warehouses["acme"]["last_sweep"]["tables"] == 2


def test_a_failure_is_the_last_error():
    state = ServiceState(fleet_path="f", dry_run=True, workers=1)
    state.finished(result("raw.a", code=1, died="worker died (killed by signal 9)"), T0)
    assert state.last_error == "acme/raw.a: worker died (killed by signal 9)"


# -- the state file and the probes ----------------------------------------------


def test_the_state_file_is_written_whole_and_reads_back(tmp_path):
    path = tmp_path / "serve.state.json"
    state = ServiceState(fleet_path="fleet.yaml", dry_run=False, workers=3)
    state.phase, state.generation, state.last_tick = "running", 2, T0
    state.write(path)
    assert list(tmp_path.iterdir()) == [path], "no temporary left behind"
    doc = read_state(path)
    assert (doc["phase"], doc["generation"], doc["workers"], doc["dry_run"]) == (
        "running",
        2,
        3,
        False,
    )
    assert doc["last_tick"] == "2026-10-02T02:00:00Z"
    assert doc["nats_connected"] is None


def alive(tmp_path: Path, pid: int = 4242) -> Path:
    proc = tmp_path / "proc"
    (proc / str(pid)).mkdir(parents=True)
    return proc


def state_doc(**kw) -> dict:
    return {
        "pid": 4242,
        "phase": "running",
        "generation": 1,
        "last_tick": "2026-10-02T02:00:00Z",
    } | kw


def test_liveness_is_only_the_scheduler_ticking(tmp_path):
    proc = alive(tmp_path)
    fresh = T0 + LIVENESS_STALE - timedelta(seconds=1)
    stale = T0 + LIVENESS_STALE + timedelta(seconds=1)
    assert probe(state_doc(), "liveness", now=fresh, proc=proc)[0]
    assert not probe(state_doc(), "liveness", now=stale, proc=proc)[0]
    # Busy, a refused reload, a failing table: none of them is a liveness failure.
    busy = state_doc(busy=4, reload_error="bad file", last_error="acme/raw.a: exit 2")
    assert probe(busy, "liveness", now=fresh, proc=proc)[0]


def test_every_probe_fails_when_the_pid_is_gone(tmp_path):
    proc = tmp_path / "proc"
    proc.mkdir()
    for kind in ("liveness", "readiness", "startup"):
        ok, why = probe(state_doc(), kind, now=T0, proc=proc)
        assert not ok and "not running" in why


@pytest.mark.parametrize(
    ("phase", "generation", "startup", "readiness"),
    [
        ("starting", 0, False, False),
        ("running", 1, True, True),
        ("stopping", 1, True, False),
        ("stopped", 1, False, False),
    ],
)
def test_startup_and_readiness_follow_the_phase(tmp_path, phase, generation, startup, readiness):
    proc = alive(tmp_path)
    doc = state_doc(phase=phase, generation=generation)
    assert probe(doc, "startup", now=T0, proc=proc)[0] is startup
    assert probe(doc, "readiness", now=T0, proc=proc)[0] is readiness


def test_an_unknown_probe_is_refused(tmp_path):
    with pytest.raises(ValueError, match="unknown probe"):
        probe(state_doc(), "health", now=T0, proc=alive(tmp_path))


# -- the run log ----------------------------------------------------------------


def test_a_worker_record_is_passed_through_marked_as_the_services():
    assert run_record(result("raw.a")) == {
        "warehouse": "acme",
        "tables": ["raw.a"],
        "exit_code": 0,
        "source": "serve",
    }


def test_a_dead_worker_still_leaves_a_record_runs_counts_as_failed():
    record = run_record(result("raw.a", code=1, died="worker died (killed by signal 9)"))
    counters = record["counters"]
    assert (
        counters["considered"] == counters["skipped"] + counters["maintained"] + counters["failed"]
    )
    summary = summarise([record])
    assert summary.failed_runs == 1
    assert summary.worst_exit_code == 1


# -- the single-instance lock -----------------------------------------------------


def test_a_second_service_on_the_same_pid_file_is_refused(tmp_path):
    pid = tmp_path / "serve.pid"
    with single_instance(pid):
        assert pid.read_text().strip() == str(os.getpid())
        with pytest.raises(AlreadyRunning), single_instance(pid):
            pass
    assert not pid.exists(), "the pid file goes with the service"
    with single_instance(pid):  # and the lock is free again
        pass


# -- Service.run, with a fake pool and clock ---------------------------------------


class FakePool:
    def __init__(self, size: PoolSize, config: WorkerConfig, *, hold: asyncio.Event | None = None):
        self.size = size
        self.hold = hold
        self.ran: list[str] = []
        self.killed = False
        self.closed = False

    async def run(self, item: WorkItem) -> ItemResult:
        self.ran.append(item.table)
        if self.hold is not None:
            await self.hold.wait()
        await asyncio.sleep(0)
        if self.killed:
            return ItemResult(item, 1, None, "", "worker died (killed by signal 9)", 0.0)
        return ItemResult(
            item,
            0,
            {"warehouse": item.warehouse, "tables": [item.table], "exit_code": 0},
            "",
            None,
            0.0,
        )

    def kill(self) -> None:
        self.killed = True
        if self.hold is not None:
            self.hold.set()

    async def close(self) -> None:
        self.closed = True


def clock_from(start: datetime):
    """A clock that advances a minute per reading, so `* * * * *` fires on every tick."""
    now = [start]

    def clock() -> datetime:
        now[0] += timedelta(minutes=1)
        return now[0]

    return clock


async def no_sleep(stop: asyncio.Event, seconds: float) -> None:
    await asyncio.sleep(0)


def service(tmp_path, *tables, pools: list, hold=None, check=lambda: None, **kw) -> Service:
    def factory(size, config):
        pool = FakePool(size, config, hold=hold)
        pools.append(pool)
        return pool

    return Service(
        fleet_file(tmp_path, *tables),
        size=PoolSize(2, 1, "test", None),
        worker_config=WorkerConfig(workdir=tmp_path),
        state_path=tmp_path / "serve.state.json",
        run_log=tmp_path / "runs.jsonl",
        pool_factory=factory,
        clock=clock_from(T0),
        scheduler_wait=no_sleep,
        capability_check=check,
        **kw,
    )


def test_a_service_fires_runs_logs_and_stops_cleanly(tmp_path):
    pools: list[FakePool] = []
    svc = service(tmp_path, "a", "b", pools=pools)

    async def scenario():
        task = asyncio.create_task(svc.run())
        while (
            not (tmp_path / "runs.jsonl").exists()
            or len((tmp_path / "runs.jsonl").read_text().splitlines()) < 2
        ):
            await asyncio.sleep(0.01)
        svc.request_stop()
        return await asyncio.wait_for(task, 5)

    assert asyncio.run(scenario()) == 0
    (pool,) = pools
    assert {"raw.a", "raw.b"} <= set(pool.ran)
    assert pool.closed
    records = [json.loads(line) for line in (tmp_path / "runs.jsonl").read_text().splitlines()]
    assert all(r["source"] == "serve" for r in records)
    state = read_state(tmp_path / "serve.state.json")
    assert state["phase"] == "stopped"
    assert state["dry_run"] is True, "no --yes: a preview service"
    assert state["generation"] == 1
    assert "last_sweep" in state["warehouses"]["acme"]


def test_stop_lets_the_table_in_flight_finish(tmp_path):
    pools: list[FakePool] = []

    async def scenario():
        hold = asyncio.Event()
        svc = service(tmp_path, "a", pools=pools, hold=hold)
        task = asyncio.create_task(svc.run())
        while not pools or not pools[0].ran:
            await asyncio.sleep(0.01)
        svc.request_stop()
        await asyncio.sleep(0.05)
        assert not task.done(), "still waiting on the table in flight"
        assert read_state(tmp_path / "serve.state.json")["phase"] == "stopping"
        hold.set()  # the table finishes
        return await asyncio.wait_for(task, 5)

    assert asyncio.run(scenario()) == 0
    assert not pools[0].killed
    record = json.loads((tmp_path / "runs.jsonl").read_text().splitlines()[0])
    assert record["exit_code"] == 0


def test_a_second_stop_kills_the_tables_in_flight(tmp_path):
    pools: list[FakePool] = []

    async def scenario():
        svc = service(tmp_path, "a", pools=pools, hold=asyncio.Event())
        task = asyncio.create_task(svc.run())
        while not pools or not pools[0].ran:
            await asyncio.sleep(0.01)
        svc.request_stop()
        svc.request_stop()
        return await asyncio.wait_for(task, 5)

    assert asyncio.run(scenario()) == 0
    assert pools[0].killed
    record = json.loads((tmp_path / "runs.jsonl").read_text().splitlines()[0])
    assert record["exit_code"] == 1 and "died" in record


def test_an_unusable_pyiceberg_does_not_start(tmp_path):
    pools: list[FakePool] = []
    svc = service(tmp_path, "a", pools=pools, check=lambda: "no REPLACE support")
    assert asyncio.run(svc.run()) == 2
    assert pools == [], "no workers were started"
    state = read_state(tmp_path / "serve.state.json")
    assert (state["phase"], state["last_error"]) == ("stopped", "no REPLACE support")


def test_an_invalid_fleet_file_is_exit_2_from_the_cli(tmp_path, capsys):
    bad = tmp_path / "fleet.yaml"
    bad.write_text("warehouses: []\n")
    code = main(
        [
            "serve",
            "--fleet",
            str(bad),
            "--state-file",
            str(tmp_path / "state.json"),
            "--pid-file",
            str(tmp_path / "serve.pid"),
        ]
    )
    assert code == 2
    assert "no warehouses" in capsys.readouterr().err


def test_service_status_without_a_state_file(tmp_path, capsys):
    missing = str(tmp_path / "absent.json")
    assert main(["service-status", "--state-file", missing, "--probe", "liveness"]) == 1
    assert main(["service-status", "--state-file", missing]) == 2


# -- the real thing -----------------------------------------------------------------


def wait_for(predicate, timeout: float, what: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}")


def test_zamboni_serve_end_to_end(tmp_path):
    """Probes, a refused second instance, SIGHUP and SIGTERM against a real
    `zamboni serve`. The schedule never fires during the test; the subject is
    the lifecycle, which the fake-pool tests cannot prove about a process."""
    run = tmp_path / "run"
    fleet = fleet_file(tmp_path, "events", schedule="0 2 1 1 *")
    state, pid = run / "serve.state.json", run / "serve.pid"
    workdir = tmp_path / "cwd"
    workdir.mkdir()  # no stray zamboni.yml or .env
    env = {**os.environ, "ZAMBONI_LOCAL_WAREHOUSE": str(tmp_path / "wh")}
    command = [
        sys.executable,
        "-c",
        "from zamboni.cli import main; raise SystemExit(main())",
        "serve",
        "--fleet",
        str(fleet),
        "--state-file",
        str(state),
        "--pid-file",
        str(pid),
    ]
    status = ["service-status", "--state-file", str(state), "--probe"]
    proc = subprocess.Popen(command, cwd=workdir, env=env, stderr=subprocess.PIPE, text=True)
    try:
        wait_for(lambda: state.exists() and main([*status, "startup"]) == 0, 90, "startup")
        assert main([*status, "readiness"]) == 0
        assert main([*status, "liveness"]) == 0
        assert read_state(state)["generation"] == 1

        second = subprocess.run(
            command, cwd=workdir, env=env, capture_output=True, text=True, timeout=60
        )
        assert second.returncode == 2
        assert "another `zamboni serve`" in second.stderr

        assert main(["config-reload", "--pid-file", str(pid)]) == 0
        wait_for(lambda: read_state(state)["generation"] == 2, 30, "the forced reload")

        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0
        assert read_state(state)["phase"] == "stopped"
        assert not pid.exists()
        assert main([*status, "liveness"]) == 1
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
