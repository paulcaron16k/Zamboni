# SPDX-License-Identifier: Apache-2.0
"""Reloading the fleet file while the service runs.

The property everything here serves: a reload can change the next decision and
can never stop maintenance by being wrong. So most of these tests feed the
watcher something bad -- a half-written file, an invalid one, a stale pid --
and check that the running config, or the unrelated process, survives it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from zamboni import reload as reload_module
from zamboni.cli import main
from zamboni.fleet import FleetConfig
from zamboni.reload import (
    FleetWatcher,
    PidFileError,
    fingerprint,
    send_reload,
    watch,
    write_pid_file,
)
from zamboni.scheduler import Scheduler


def fleet_doc(*names: str, schedule: str = "0 2 * * *") -> dict:
    return {
        "warehouses": [
            {
                "name": n,
                "schedule": schedule,
                "random": False,
                "table_config": {"namespaces": {"raw": {"tables": {"events": {}}}}},
            }
            for n in names
        ]
    }


def write(path: Path, doc: dict | str) -> None:
    path.write_text(doc if isinstance(doc, str) else yaml.safe_dump(doc))
    # Force a visible mtime step even on a filesystem with coarse timestamps.
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))


@pytest.fixture
def fleet_file(tmp_path):
    path = tmp_path / "fleet.yaml"
    write(path, fleet_doc("acme"))
    return path


def settle(watcher: FleetWatcher):
    """Two polls: the first sees the change, the second adopts it."""
    first = watcher.check()
    assert first.fleet is None, "a change is never adopted on first sight"
    return watcher.check()


# -- detecting a change -----------------------------------------------------------


def test_an_unchanged_file_is_not_reloaded(fleet_file):
    watcher = FleetWatcher(fleet_file)
    assert not watcher.check().changed
    assert not watcher.check().changed
    assert watcher.generation == 1


def test_a_change_is_adopted_once_it_settles(fleet_file):
    watcher = FleetWatcher(fleet_file)
    write(fleet_file, fleet_doc("acme", "globex"))

    outcome = settle(watcher)

    assert outcome.changed and outcome.fleet is not None
    assert watcher.fleet.names == ("acme", "globex")
    assert watcher.generation == 2
    assert watcher.last_reload is not None


def test_a_half_written_file_is_never_adopted(fleet_file):
    """A list cut short is still a valid list. Seen mid-write, it must wait for
    the next poll -- which sees the finished file instead."""
    watcher = FleetWatcher(fleet_file)
    full = fleet_doc("acme", "globex", "initech")

    write(fleet_file, fleet_doc("acme"))  # the writer is part-way through
    write(fleet_file, yaml.safe_dump(fleet_doc("acme"))[:-1] + "\n")  # still valid, still short
    assert watcher.check().fleet is None
    write(fleet_file, full)  # the writer finishes before the next poll
    assert watcher.check().fleet is None, "a different fingerprint restarts the settle"
    outcome = watcher.check()

    assert outcome.fleet is not None
    assert watcher.fleet.names == ("acme", "globex", "initech")
    assert watcher.generation == 2, "the short version was never a generation"


def test_a_configmap_symlink_swap_is_seen(tmp_path):
    """Kubernetes projects `fleet.yaml -> ..data/fleet.yaml` and swaps `..data`.
    The link `fleet.yaml` never changes; what it resolves to does -- here with an
    identical mtime, so only the inode and resolved path can tell."""
    mount = tmp_path / "config"
    mount.mkdir()
    old = mount / "..2026_10_01_a"
    old.mkdir()
    write(old / "fleet.yaml", fleet_doc("acme"))
    (mount / "..data").symlink_to(old.name)
    (mount / "fleet.yaml").symlink_to("..data/fleet.yaml")

    watcher = FleetWatcher(mount / "fleet.yaml")

    new = mount / "..2026_10_01_b"
    new.mkdir()
    (new / "fleet.yaml").write_text(yaml.safe_dump(fleet_doc("acme", "globex")))
    st = (old / "fleet.yaml").stat()
    os.utime(new / "fleet.yaml", ns=(st.st_atime_ns, st.st_mtime_ns))
    (mount / "..data.tmp").symlink_to(new.name)
    os.replace(mount / "..data.tmp", mount / "..data")  # the atomic swap

    assert settle(watcher).fleet is not None
    assert watcher.fleet.names == ("acme", "globex")


def test_a_referenced_table_config_is_watched_too(tmp_path):
    (tmp_path / "acme.json").write_text(
        json.dumps({"warehouse": "acme", "namespaces": {"raw": {"tables": {"events": {}}}}})
    )
    fleet_file = tmp_path / "fleet.yaml"
    write(
        fleet_file,
        {"warehouses": [{"name": "acme", "schedule": "0 2 * * *", "table_config": "acme.json"}]},
    )
    watcher = FleetWatcher(fleet_file)

    write(
        tmp_path / "acme.json",
        json.dumps(
            {"warehouse": "acme", "namespaces": {"raw": {"tables": {"events": {}, "new": {}}}}}
        ),
    )

    assert settle(watcher).fleet is not None
    assert set(watcher.fleet["acme"].table_config.tables) == {"raw.events", "raw.new"}


def test_the_watch_set_follows_the_reload(tmp_path):
    """A reload that stops referencing a file stops watching it."""
    (tmp_path / "acme.json").write_text(
        json.dumps({"warehouse": "acme", "namespaces": {"raw": {"tables": {"events": {}}}}})
    )
    fleet_file = tmp_path / "fleet.yaml"
    write(
        fleet_file,
        {"warehouses": [{"name": "acme", "schedule": "0 2 * * *", "table_config": "acme.json"}]},
    )
    watcher = FleetWatcher(fleet_file)
    write(fleet_file, fleet_doc("acme"))  # now inline
    settle(watcher)

    (tmp_path / "acme.json").unlink()
    assert not watcher.check().changed
    assert not watcher.check().changed


# -- an invalid file ------------------------------------------------------------------


def test_an_invalid_file_keeps_the_running_config_and_says_so_once(fleet_file, caplog, monkeypatch):
    watcher = FleetWatcher(fleet_file)
    running = watcher.fleet
    write(fleet_file, {"warehouses": []})  # what a generator that fails open writes

    loads = []
    real_load = FleetConfig.load
    monkeypatch.setattr(
        reload_module.FleetConfig, "load", lambda p: loads.append(p) or real_load(p)
    )
    with caplog.at_level(logging.ERROR, logger="zamboni.reload"):
        outcomes = [watcher.check() for _ in range(6)]

    assert watcher.fleet is running
    assert watcher.generation == 1
    assert any(o.error and "no warehouses" in o.error for o in outcomes)
    assert "no warehouses" in (watcher.last_error or "")
    assert len([r for r in caplog.records if "invalid" in r.message]) == 1, "reported once"
    assert len(loads) == 1, "a known-bad file is not re-parsed every poll"


def test_fixing_an_invalid_file_is_adopted(fleet_file):
    watcher = FleetWatcher(fleet_file)
    write(fleet_file, "warehouses: [")
    settle(watcher)
    assert watcher.last_error

    write(fleet_file, fleet_doc("acme", "globex"))
    assert settle(watcher).fleet is not None
    assert watcher.last_error is None
    assert watcher.generation == 2


def test_a_deleted_file_keeps_the_running_config(fleet_file):
    watcher = FleetWatcher(fleet_file)
    fleet_file.unlink()
    settle(watcher)
    assert watcher.fleet.names == ("acme",)
    assert "cannot read" in (watcher.last_error or "")


def test_force_loads_without_waiting_to_settle(fleet_file):
    """SIGHUP means the operator says the file is finished."""
    watcher = FleetWatcher(fleet_file)
    write(fleet_file, fleet_doc("acme", "globex"))
    assert watcher.check(force=True).fleet is not None
    assert watcher.generation == 2


# -- the watch loop -------------------------------------------------------------------


def test_the_loop_rearms_the_scheduler_on_reload(fleet_file):
    watcher = FleetWatcher(fleet_file)
    now = datetime(2026, 10, 1, 0, 0, tzinfo=UTC)
    scheduler = Scheduler(watcher.fleet, now)

    async def scenario():
        stop, hangup = asyncio.Event(), asyncio.Event()

        def on_reload(fleet):
            scheduler.rearm(fleet, now)
            stop.set()

        task = asyncio.create_task(
            watch(watcher, stop, on_reload=on_reload, hangup=hangup, interval=60)
        )
        write(fleet_file, fleet_doc("acme", "globex"))
        hangup.set()  # not 60 s: the hangup wakes it
        await asyncio.wait_for(task, 5)

    asyncio.run(scenario())
    assert scheduler.fleet.names == ("acme", "globex")
    assert scheduler.next_firing("globex") == datetime(2026, 10, 1, 2, 0, tzinfo=UTC)


def test_a_failing_reload_hook_does_not_stop_the_watcher(fleet_file):
    watcher = FleetWatcher(fleet_file)
    calls = []

    async def scenario():
        stop, hangup = asyncio.Event(), asyncio.Event()

        def on_reload(fleet):
            calls.append(fleet.names)
            if len(calls) == 1:
                raise RuntimeError("rearm failed")
            stop.set()

        task = asyncio.create_task(watch(watcher, stop, on_reload=on_reload, hangup=hangup))
        for names in (("acme", "globex"), ("acme", "globex", "initech")):
            write(fleet_file, fleet_doc(*names))
            hangup.set()
            await asyncio.sleep(0.05)
        await asyncio.wait_for(task, 5)

    asyncio.run(scenario())
    assert calls == [("acme", "globex"), ("acme", "globex", "initech")]


# -- the pid file -----------------------------------------------------------------------


def test_the_pid_file_is_written_whole(tmp_path):
    path = tmp_path / "serve.pid"
    write_pid_file(path)
    assert path.read_text() == f"{os.getpid()}\n"
    assert list(tmp_path.iterdir()) == [path], "no temporary left behind"


HUP_CHILD = """
import os, signal, sys, time
marker = sys.argv[1]
def hup(*_):
    # Written then renamed, so the test never sees the file before its content.
    with open(marker + ".tmp", "w") as out:
        out.write("reloaded")
    os.replace(marker + ".tmp", marker)
signal.signal(signal.SIGHUP, hup)
print("ready", flush=True)
time.sleep(30)
"""


def test_config_reload_signals_a_zamboni_process(tmp_path):
    marker = tmp_path / "marker"
    child = subprocess.Popen(
        [sys.executable, "-c", HUP_CHILD, str(marker), "zamboni-serve"],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "ready"
        pid_file = tmp_path / "serve.pid"
        pid_file.write_text(f"{child.pid}\n")

        assert main(["config-reload", "--pid-file", str(pid_file)]) == 0

        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert marker.read_text() == "reloaded"
        assert child.poll() is None, "SIGHUP reloaded it; it did not kill it"
    finally:
        child.kill()
        child.wait()


def test_a_stale_pid_naming_another_process_is_not_signalled(tmp_path):
    """SIGHUP's default action terminates. A reused pid must be left alone."""
    bystander = subprocess.Popen(["sleep", "30"])
    try:
        pid_file = tmp_path / "serve.pid"
        pid_file.write_text(f"{bystander.pid}\n")
        with pytest.raises(PidFileError, match="not a zamboni process"):
            send_reload(pid_file)
        time.sleep(0.1)
        assert bystander.poll() is None, "the bystander is still alive"
        assert main(["config-reload", "--pid-file", str(pid_file)]) == 2
    finally:
        bystander.kill()
        bystander.wait()


@pytest.mark.parametrize("content", [None, "", "not-a-pid\n", "999999999\n"])
def test_no_service_to_signal_is_exit_2(tmp_path, content, capsys):
    pid_file = tmp_path / "serve.pid"
    if content is not None:
        pid_file.write_text(content)
    assert main(["config-reload", "--pid-file", str(pid_file)]) == 2
    assert "error:" in capsys.readouterr().err


def test_fingerprint_reports_a_missing_file_rather_than_raising(tmp_path):
    (entry,) = fingerprint((tmp_path / "absent.yaml",))
    assert entry[1:] == (None, None, None)
