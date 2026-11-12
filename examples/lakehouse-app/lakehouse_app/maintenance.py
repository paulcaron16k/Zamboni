# SPDX-License-Identifier: Apache-2.0
"""The maintenance tier: ``python -m lakehouse_app.maintenance``.

The runtime is this process's main work (docs/maintenance-runtime.md §9.2), so
it runs on the main thread's loop rather than through ``start_in_thread``.
Everything it shares with the web tier is a row in the app's database:

- the fleet, from the warehouse registry -- re-read every
  ``LAKEHOUSE_REGISTRY_POLL_SECONDS`` and pushed through
  ``MaintenanceRuntime.update_fleet`` on any change (§9.5 point 4);
- table claims, so any number of replicas share the fleet (§6, §9.3);
- requests, read through a `RequestSource` (§9.2);
- state, written by an observer (:mod:`lakehouse_app.state`).

Settings, from the environment, besides the database's (:mod:`lakehouse_app.db`):

- ``LAKEHOUSE_MAINTENANCE_COMMIT=yes`` lets maintenance commit. Anything else
  previews every table and commits nothing -- `zamboni serve`'s ``--yes``, and
  for the same reason: consent is given, never assumed.
- ``LAKEHOUSE_REGISTRY_POLL_SECONDS`` (default 10).
- ``LAKEHOUSE_RESERVED_MEMORY_BYTES`` (default 0): this process's own baseline,
  kept out of what the pool plans its workers against (§9.4).
- ``LAKEHOUSE_WORKDIR``: where workers write scratch files (default: a
  temporary directory).
- ``LAKEHOUSE_WORKERS``: at most this many workers. The pool sizes itself from
  the container's limits (§9.4); this can lower that and never raise it.
- ``LAKEHOUSE_STATE_FILE``: write ``zamboni serve``'s state file here, so
  ``zamboni service-status --probe liveness|readiness`` answers for this
  process as it does for ``serve`` -- the same file, written by the same
  observer, read by the same probe (the Kubernetes manifests use it). While no
  warehouse is registered yet the file says ``starting`` with a fresh tick:
  alive, and not ready.

SIGTERM or SIGINT stops it gracefully, letting tables in flight finish; a
second one kills them, which is safe for the reason any kill is (§7.1).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from zamboni import CompactionConfig
from zamboni.fleet import FleetConfig
from zamboni.observe import handed_off
from zamboni.pool import PoolSize, WorkerConfig, plan_pool
from zamboni.runstate import RuntimeState
from zamboni.runtime import CREATED, MaintenanceRuntime, RuntimeSettings, StaticFleet, now_utc
from zamboni.service import EVENTS_OBSERVE_SECONDS, ServiceState, StateFileObserver

from .claims import PostgresClaimStore
from .db import Database
from .registry import Registry, RegistryError, WarehouseRow, build_fleet
from .request_table import RequestTable
from .state import StateRows

logger = logging.getLogger("lakehouse_app.maintenance")

ENTRY = "lakehouse_app.entry:run"
HEALTH_ENTRY = "lakehouse_app.entry:check"
COMMIT = "LAKEHOUSE_MAINTENANCE_COMMIT"
POLL = "LAKEHOUSE_REGISTRY_POLL_SECONDS"
RESERVED_MEMORY = "LAKEHOUSE_RESERVED_MEMORY_BYTES"
WORKDIR = "LAKEHOUSE_WORKDIR"
WORKERS = "LAKEHOUSE_WORKERS"
STATE_FILE = "LAKEHOUSE_STATE_FILE"
DEFAULT_POLL_SECONDS = 10.0


class ProbeFile:
    """``zamboni serve``'s state file, for this process.

    Written by serve's own `StateFileObserver` once the runtime exists, so
    ``zamboni service-status --probe`` reads it exactly as it reads serve's.
    Before that -- while no warehouse is registered -- :meth:`waiting` writes
    ``starting`` with a fresh tick on every registry poll, so a liveness
    probe passes and a readiness probe does not.
    """

    def __init__(self, path: Path, *, dry_run: bool, database: Database) -> None:
        self.path = path
        self.service = ServiceState(fleet_path=f"registry:{database.schema}", dry_run=dry_run)

    def waiting(self) -> None:
        state = RuntimeState(
            phase=CREATED,
            generation=0,
            workers=0,
            last_tick=now_utc(),
            queued=(),
            in_flight=(),
            dirty=(),
        )
        try:
            self.service.write(self.path, state)
        except OSError:
            logger.exception("could not write the state file %s", self.path)

    def observer(self, state: Callable[[], RuntimeState]) -> StateFileObserver:
        self._observer = StateFileObserver(self.path, self.service, state)
        return self._observer

    async def watch_sources(self, runtime: MaintenanceRuntime, stop: asyncio.Event) -> None:
        """Rewrite the file when the request source connects or disconnects.

        Nothing the runtime tells observers says so, and a scheduler can sleep
        a minute between ticks: without this, a fresh replica stayed unready
        for up to a minute after its source connected -- seen in this image,
        unready 15 s after start -- which is why `zamboni serve` does the same.
        A write happens only on a change.
        """
        last = None
        while not stop.is_set():
            connected = runtime.state().sources_connected
            if connected != last:
                last = connected
                self._observer.write()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), EVENTS_OBSERVE_SECONDS)


class RegistryWatcher:
    """Pushes each registry change to the runtime through ``update_fleet``.

    The registry is the app's, and the runtime is told about it -- the
    runtime never reads the app's tables itself (§9.1). A registry the runtime
    would refuse -- emptied, or made invalid by hand -- is logged and the fleet
    in use is kept: stopping all maintenance because someone deleted the last
    row by mistake is the one thing a reload must never do.
    """

    def __init__(self, registry: Registry, *, interval: float) -> None:
        self.registry = registry
        self.interval = interval
        self._seen: list[dict[str, Any]] | None = None

    def first(self, rows: list[WarehouseRow]) -> FleetConfig:
        fleet = build_fleet(rows)
        self._seen = [row.fleet_entry() for row in rows]
        return fleet

    async def run(self, runtime: MaintenanceRuntime, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), self.interval)
            if stop.is_set():
                return
            try:
                rows = await asyncio.to_thread(self.registry.warehouses)
            except Exception as exc:  # the database is down: keep the fleet, try again
                logger.warning("cannot read the warehouse registry: %s", exc)
                continue
            entries = [row.fleet_entry() for row in rows]
            if entries == self._seen:
                continue
            try:
                fleet = build_fleet(rows)
            except RegistryError as exc:
                logger.error(
                    "the registry changed, but not to a usable fleet; keeping the last: %s", exc
                )
                self._seen = entries  # said once, not on every poll
                continue
            generation = runtime.update_fleet(fleet)
            self._seen = entries
            logger.info("fleet generation %d: %s", generation, ", ".join(fleet.names))


async def wait_for_rows(
    registry: Registry,
    stop: asyncio.Event,
    interval: float,
    waiting: Callable[[], None] = lambda: None,
) -> list[WarehouseRow]:
    """The registry's rows, once there is at least one. A fresh install has
    none until someone registers a warehouse in the web tier. ``waiting`` is
    called on every poll that finds none."""
    said = False
    while True:
        rows = await asyncio.to_thread(registry.warehouses)
        if rows:
            return rows
        waiting()
        if not said:
            logger.info("no warehouse registered yet; waiting for one")
            said = True
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), interval)
        if stop.is_set():
            raise asyncio.CancelledError


def worker_config(size: PoolSize, environ: Mapping[str, str], *, workdir: Path) -> WorkerConfig:
    return WorkerConfig.from_pool(
        size,
        entry=ENTRY,
        health_entry=HEALTH_ENTRY,
        commit=environ.get(COMMIT, "").lower() == "yes",
        workdir=workdir,
    )


def build(
    database: Database,
    fleet: FleetConfig,
    *,
    size: PoolSize,
    worker: WorkerConfig,
    settings: RuntimeSettings | None = None,
    observers: Sequence[Any] = (),
    **runtime_options: Any,
) -> MaintenanceRuntime:
    """The runtime, wired to the app's database. ``observers`` are in
    addition to the state rows; ``runtime_options`` are `MaintenanceRuntime`'s
    own, for a test that drives the clock or the pool."""
    rows = StateRows(database)
    runtime = MaintenanceRuntime(
        fleet=StaticFleet(fleet),
        pool=size,
        worker=worker,
        claims=PostgresClaimStore(database),
        requests=[RequestTable(database).source()],
        observers=[handed_off(rows, name="state rows"), *observers],
        settings=settings,
        **runtime_options,
    )
    rows.attach(runtime)
    return runtime


async def serve(
    environ: Mapping[str, str],
    *,
    install_signals: Callable[[list[MaintenanceRuntime], asyncio.Event], None] | None = None,
) -> int:
    database = Database.from_environ(environ)
    await asyncio.to_thread(database.migrate)
    registry = Registry(database)
    interval = float(environ.get(POLL) or DEFAULT_POLL_SECONDS)
    watcher = RegistryWatcher(registry, interval=interval)

    stopping = asyncio.Event()
    holder: list[MaintenanceRuntime] = []
    (install_signals or _install_signals)(holder, stopping)
    commit = environ.get(COMMIT, "").lower() == "yes"
    probe = None
    if path := environ.get(STATE_FILE):
        probe = ProbeFile(Path(path), dry_run=not commit, database=database)
    try:
        rows = await wait_for_rows(
            registry, stopping, interval, probe.waiting if probe else lambda: None
        )
    except asyncio.CancelledError:
        return 0
    fleet = watcher.first(rows)

    size = plan_pool(
        memory_budget_bytes=CompactionConfig().memory_budget_bytes,
        requested=int(environ[WORKERS]) if environ.get(WORKERS) else None,
        reserved_memory_bytes=int(environ.get(RESERVED_MEMORY) or 0),
    )
    workdir = Path(environ.get(WORKDIR) or tempfile.mkdtemp(prefix="lakehouse-app-"))
    worker = worker_config(size, environ, workdir=workdir)
    if not worker.commit:
        logger.warning("%s is not 'yes': every table previews and nothing is committed", COMMIT)
    observers = [] if probe is None else [probe.observer(lambda: holder[0].state())]
    runtime = build(database, fleet, size=size, worker=worker, observers=observers)
    holder.append(runtime)
    if stopping.is_set():  # a signal while starting still counts
        runtime.request_stop()
    logger.info(
        "maintaining %s with %d worker(s) as %s",
        ", ".join(fleet.names),
        size.workers,
        runtime.holder,
    )

    tasks = [asyncio.create_task(watcher.run(runtime, stopping))]
    if probe is not None:
        tasks.append(asyncio.create_task(probe.watch_sources(runtime, stopping)))
    try:
        await runtime.run()
    finally:
        stopping.set()
        await asyncio.gather(*tasks)
    return 0


def _install_signals(holder: list[MaintenanceRuntime], stopping: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    count = [0]

    def stop() -> None:
        count[0] += 1
        stopping.set()
        if holder:
            holder[0].request_stop(force=count[0] > 1)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    return asyncio.run(serve(os.environ))


if __name__ == "__main__":
    raise SystemExit(main())
