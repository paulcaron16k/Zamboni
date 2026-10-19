# SPDX-License-Identifier: Apache-2.0
"""``MaintenanceRuntime``: deciding *when*, for every deployment that is not cron.

::

  FleetProvider ─update─┐
  EventSource ─▶ EventFeed ─┐
  pressure checks ──────────┼─offer─▶ CandidateQueue ─▶ run_workers ─▶ WorkerPool
  Scheduler (cron) ─────────┘                                │
  offer() / update_fleet() / state() / request_stop()        └─▶ per-table results
          (the control surface: any thread)

docs/maintenance-runtime.md §3. One object owning one asyncio loop's worth of
work -- the scheduler loop, one feed loop per event source, the pressure checks,
the take-and-run loop and the fleet provider -- with its parts injected.
``zamboni serve`` is one assembly of it (`zamboni.service`); an application
embedding Zamboni is another. Nothing here decides whether a table needs work:
that is still ``maintain()``'s due-check, reached through each worker.

**The control surface is the only thing another thread may touch.** Each method
hands its work to the runtime's loop with ``call_soon_threadsafe`` and waits for
the loop to *apply* it -- never for maintenance. Before :meth:`run` starts, and
after it returns, there is no loop and the call is applied in the caller's
thread under a lock. Nothing else on the runtime is thread-safe, and nothing
else needs to be: every other part runs on the loop.

**Provisional (0.8.0).** The constructor, the control surface and
:func:`start_in_thread` are new public API, marked provisional until the first
embedding application has adopted them. Claims (§6, #167) and the public
``Observer`` protocol (§8, #166) are not here yet; ``zamboni serve`` reaches its
state file and run log through private hooks until #166 replaces them.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, TypeVar

from .fleet import FleetConfig, FleetWarehouse
from .pool import ItemResult, PoolSize, WorkerConfig, WorkerPool, run_workers
from .scheduler import (
    MAX_SLEEP,
    PRESSURE,
    Candidate,
    CandidateQueue,
    PressureCheck,
    Scheduler,
    _wait,
    operations_for,
    run_scheduler,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: :attr:`RuntimeState.phase`: ``created`` -> ``running`` -> ``stopping`` -> ``stopped``.
CREATED, RUNNING, STOPPING, STOPPED = "created", "running", "stopping", "stopped"


def now_utc() -> datetime:
    return datetime.now(UTC)


# -- the parts --------------------------------------------------------------------


class FleetProvider(Protocol):
    """Where the fleet comes from, and how changes to it arrive.

    ``fleet`` is read once, when the runtime is built. ``run`` is awaited for
    the runtime's life and calls ``adopt`` with each new fleet -- on the
    runtime's loop -- and ``refused`` with the reason a candidate fleet was not
    adopted. A provider with nothing to watch returns from ``run`` at once and
    leaves changes to :meth:`MaintenanceRuntime.update_fleet`.
    """

    @property
    def fleet(self) -> FleetConfig: ...

    async def run(
        self,
        adopt: Callable[[FleetConfig], int],
        refused: Callable[[str], None],
        stop: asyncio.Event,
    ) -> None: ...


@dataclass(frozen=True)
class StaticFleet:
    """A fleet that changes only through :meth:`MaintenanceRuntime.update_fleet`.

    The provider for an application that keeps its fleet in its own database
    and pushes each change (docs/maintenance-runtime.md §9.5, rule 4).
    """

    fleet: FleetConfig

    async def run(
        self,
        adopt: Callable[[FleetConfig], int],
        refused: Callable[[str], None],
        stop: asyncio.Event,
    ) -> None:
        return None


@dataclass(frozen=True)
class OfferReceipt:
    """What one :meth:`MaintenanceRuntime.offer` did, table by table."""

    warehouse: str
    reason: str
    #: Newly queued.
    queued: tuple[str, ...] = ()
    #: Merged into work already queued or in flight (docs/maintenance-runtime.md §3.2).
    coalesced: tuple[str, ...] = ()
    #: Named by the offer but not in the warehouse's table config: dropped.
    unknown: tuple[str, ...] = ()
    #: Why nothing was offered -- the runtime is stopping, or the warehouse is
    #: not in the fleet. ``None`` when the offer was considered table by table.
    refused: str | None = None


@dataclass(frozen=True)
class RuntimeState:
    """A consistent snapshot of the runtime, taken on its loop.

    #166 widens it to everything ``zamboni serve``'s state file carries.
    """

    phase: str
    generation: int
    workers: int
    last_tick: datetime | None
    #: ``(warehouse, table)``, in the order they will be taken.
    queued: tuple[tuple[str, str], ...]
    in_flight: tuple[tuple[str, str], ...]
    #: In flight, and offered again by an event meanwhile: re-offered when done.
    dirty: tuple[tuple[str, str], ...]

    @property
    def busy(self) -> int:
        return len(self.in_flight)


def _ignore(*_: Any) -> None:
    return None


def _settle(
    done: concurrent.futures.Future[T], fn: Callable[..., T], args: tuple[Any, ...]
) -> None:
    try:
        done.set_result(fn(*args))
    except BaseException as exc:
        done.set_exception(exc)


@dataclass(frozen=True)
class _Hooks:
    """What ``zamboni serve`` needs to keep its state file and run log.

    **Private until #166**, which replaces it with the public ``Observer``
    protocol (docs/maintenance-runtime.md §8). Called on the runtime's loop.
    Failures propagate exactly where the loops they sit in let them: a
    ``finished`` or ``fleet_changed`` that raises is logged and the runtime
    carries on, as before the extraction.
    """

    #: The pool exists and the loops are about to start.
    running: Callable[[], None] = _ignore
    #: Every scheduler step, fired or not: the liveness signal.
    tick: Callable[[datetime], None] = _ignore
    #: Per warehouse a step fired: the tables it asked for, excluding any in flight.
    fired: Callable[[datetime, dict[str, list[str]]], None] = _ignore
    #: A table's result, before the queue releases it.
    finished: Callable[[ItemResult], None] = _ignore
    fleet_changed: Callable[[int, FleetConfig], None] = _ignore
    #: A candidate fleet the provider did not adopt, and why.
    fleet_refused: Callable[[str], None] = _ignore
    pressure_checked: Callable[[str, dict[str, Any]], None] = _ignore
    #: The first stop request has arrived.
    stopping: Callable[[], None] = _ignore


# -- delete pressure (#175) -------------------------------------------------------


def pressure_offers(warehouse: FleetWarehouse, record: dict[str, Any] | None) -> dict[str, Any]:
    """What one warehouse's check found: which tables are over, and why.

    ``record`` is the ``health --json`` line the worker wrote, or ``None`` if
    the worker died. Pure, so the decision is tested without a pool.
    """
    over: dict[str, str] = {}
    errors: dict[str, str] = {}
    settings = warehouse.pressure
    tables = (record or {}).get("tables") or {}
    if record is None:
        errors["*"] = "the check wrote no record"
    for table, signals in tables.items():
        if "error" in signals:
            errors[table] = signals["error"]
        elif settings is not None and (why := settings.triggered(signals)):
            over[table] = why
    return {"over": over, "errors": errors, "checked": len(tables)}


async def run_pressure_checks(
    fleet: Callable[[], FleetConfig],
    pool: Any,
    offer: Callable[[Candidate], bool],
    stop: asyncio.Event,
    *,
    clock: Callable[[], datetime] = now_utc,
    on_check: Callable[[str, dict[str, Any]], None] | None = None,
    wait: Callable[[asyncio.Event, float], Any] | None = None,
) -> None:
    """Measure each warehouse every ``check_every_minutes`` and offer what is over.

    The first check of a warehouse is one interval after it appears -- at
    start or by reload -- so a restart is not a burst of checks on top of
    whatever the schedule fires. One check at a time: each takes a worker for
    one metadata load per table (~25 ms on the dev stack), and a fleet's checks
    queuing behind each other cost latency, not correctness. A check that fails
    is reported and retried at the next interval; the schedule still maintains
    every table.
    """
    wait = wait or _wait
    due: dict[str, datetime] = {}
    while not stop.is_set():
        now = clock()
        current = fleet()
        for warehouse in current.warehouses:
            if warehouse.pressure is None:
                due.pop(warehouse.name, None)
                continue
            soonest = now + timedelta(minutes=warehouse.pressure.check_every_minutes)
            # A reload that shortened the interval takes effect now, not after
            # the old, longer one.
            if warehouse.name not in due or due[warehouse.name] > soonest:
                due[warehouse.name] = soonest
        for name in set(due) - set(current.names):
            del due[name]

        for name in sorted(n for n, at in due.items() if at <= now):
            if stop.is_set():
                break
            warehouse = current[name]
            result = await pool.run(
                PressureCheck(warehouse.name, warehouse.table_config, warehouse.uri)
            )
            found = pressure_offers(warehouse, result.record)
            offered = 0
            for table, why in sorted(found["over"].items()):
                accepted = offer(Candidate(name, table, PRESSURE))
                offered += accepted
                logger.info(
                    "pressure: %s/%s: %s; %s",
                    name,
                    table,
                    why,
                    "offered" if accepted else "already queued or running",
                )
            if found["errors"]:
                logger.warning("pressure check of %s could not read: %s", name, found["errors"])
            if on_check is not None:
                on_check(name, {**found, "offered": offered, "exit_code": result.exit_code})
            if warehouse.pressure is not None:
                interval = timedelta(minutes=warehouse.pressure.check_every_minutes)
                due[name] = clock() + interval

        upcoming = min(due.values(), default=None)
        seconds = MAX_SLEEP.total_seconds()
        if upcoming is not None:
            seconds = min(max((upcoming - clock()).total_seconds(), 0.0), seconds)
        await wait(stop, seconds)


# -- the runtime ------------------------------------------------------------------


class MaintenanceRuntime:
    """Schedules, events, pressure checks, the queue and the worker pool, on one loop.

    ``pool_factory`` builds the pool when :meth:`run` starts (a test passes a
    fake); ``wait`` replaces the scheduler's sleep, for a test driving
    ``clock``. ``feed_observer`` is told what each event feed does
    (`zamboni.events.feed.FeedObserver`). A runtime runs once.
    """

    def __init__(
        self,
        *,
        fleet: FleetProvider,
        pool: PoolSize,
        worker: WorkerConfig,
        events: Sequence[Any] = (),
        feed_observer: Any = None,
        clock: Callable[[], datetime] = now_utc,
        pool_factory: Callable[[PoolSize, WorkerConfig], Any] = WorkerPool,
        wait: Callable[[asyncio.Event, float], Awaitable[None]] | None = None,
        _hooks: _Hooks | None = None,
    ) -> None:
        self._provider = fleet
        self._fleet = fleet.fleet
        self._size = pool
        self._worker = worker
        self._clock = clock
        self._pool_factory = pool_factory
        self._wait = wait
        self._hooks = _hooks or _Hooks()
        self._events = tuple(events)
        self._queue = CandidateQueue()
        self._stop = asyncio.Event()
        self._generation = 1
        self._phase = CREATED
        self._last_tick: datetime | None = None
        self._scheduler: Scheduler | None = None
        self._pool: Any = None
        self._forced = False
        # Guards the loop's arrival and departure, and every call made while
        # there is no loop. Re-entrant: a hook may read `state()`.
        self._lock = threading.RLock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: int | None = None
        #: Set once the loop is running and the pool exists; `start_in_thread` waits on it.
        self._started = threading.Event()
        self.feeds: list[Any] = []
        if self._events:
            from .events.feed import EventFeed

            self.feeds = [
                EventFeed(
                    source,
                    fleet=lambda: self._fleet,
                    offer=self._queue.offer,
                    observer=feed_observer,
                )
                for source in self._events
            ]

    # -- the control surface: safe from any thread ---------------------------------

    def offer(
        self, warehouse: str, tables: Sequence[str] | None = None, *, reason: str
    ) -> OfferReceipt:
        """Offer ``tables`` of ``warehouse`` -- every table when ``None`` -- for ``reason``.

        An offer is not a decision: each table still runs only the operations
        its reason names and that the due-check finds work for. An unknown
        ``reason`` is refused here, in the caller's thread.
        """
        operations_for(reason)
        wanted = None if tables is None else tuple(tables)
        return self._call(self._offer, warehouse, wanted, reason)

    def update_fleet(self, fleet: FleetConfig) -> int:
        """Adopt ``fleet``; the new generation.

        Changes every decision not yet started and none in flight: schedules
        re-arm (`Scheduler.rearm`), and a queued table is resolved against the
        newest fleet when a worker takes it.
        """
        return self._call(self._adopt, fleet)

    def state(self) -> RuntimeState:
        return self._call(self._snapshot)

    def request_stop(self, *, force: bool = False) -> None:
        """First call: take no new tables and let those in flight finish.

        A second call, or ``force``, also kills the in-flight tables' workers.
        That is safe for the reason a hard kill is: a compaction commits one
        snapshot at the end, so a rewrite killed before it commits nothing, and
        the files it had written are orphans for the next ``remove-orphans``.
        Does not wait.
        """
        self._post(self._request_stop, force)

    def _call(self, fn: Callable[..., T], *args: Any) -> T:
        """``fn(*args)`` on the loop, and its result; the loop's own thread calls it directly."""
        done: concurrent.futures.Future[T] = concurrent.futures.Future()
        with self._lock:
            loop = self._loop
            if loop is None:
                return fn(*args)
            elsewhere = threading.get_ident() != self._loop_thread
            if elsewhere:
                loop.call_soon_threadsafe(_settle, done, fn, args)
        return done.result() if elsewhere else fn(*args)

    def _post(self, fn: Callable[..., None], *args: Any) -> None:
        """``fn(*args)`` on the loop, without waiting for it."""
        with self._lock:
            loop = self._loop
            if loop is None:
                fn(*args)
                return
            if threading.get_ident() != self._loop_thread:
                loop.call_soon_threadsafe(fn, *args)
                return
        fn(*args)

    # -- applied on the loop --------------------------------------------------------

    def _offer(self, warehouse: str, tables: tuple[str, ...] | None, reason: str) -> OfferReceipt:
        if self._stop.is_set():
            return OfferReceipt(warehouse, reason, refused="the runtime is stopping")
        try:
            known = self._fleet[warehouse].table_config.tables
        except KeyError:
            return OfferReceipt(
                warehouse, reason, refused=f"warehouse {warehouse!r} is not in the fleet"
            )
        queued, coalesced, unknown = [], [], []
        for table in sorted(known) if tables is None else dict.fromkeys(tables):
            if table not in known:
                unknown.append(table)
            elif self._queue.offer(Candidate(warehouse, table, reason)):
                queued.append(table)
            else:
                coalesced.append(table)
        return OfferReceipt(warehouse, reason, tuple(queued), tuple(coalesced), tuple(unknown))

    def _adopt(self, fleet: FleetConfig) -> int:
        self._fleet = fleet
        self._generation += 1
        if self._scheduler is not None:
            self._scheduler.rearm(fleet, self._clock())
        self._hooks.fleet_changed(self._generation, fleet)
        return self._generation

    def _snapshot(self) -> RuntimeState:
        return RuntimeState(
            phase=self._phase,
            generation=self._generation,
            workers=self._size.workers,
            last_tick=self._last_tick,
            queued=self._queue.queued,
            in_flight=tuple(sorted(self._queue.in_flight)),
            dirty=tuple(sorted(self._queue.dirty)),
        )

    def _request_stop(self, force: bool) -> None:
        first = not self._stop.is_set()
        if first:
            logger.info(
                "stopping: no new tables; letting %d in flight finish", len(self._queue.in_flight)
            )
            self._stop.set()
        if (force or not first) and not self._forced and hasattr(self._pool, "kill"):
            self._forced = True
            logger.warning("stop forced: killing in-flight workers")
            self._pool.kill()

    # -- the loops ------------------------------------------------------------------

    async def run(self) -> None:
        """Run until stopped. Returns after the tables in flight finish."""
        with self._lock:
            if self._phase != CREATED:
                raise RuntimeError(f"a MaintenanceRuntime runs once; this one is {self._phase}")
            self._loop = asyncio.get_running_loop()
            self._loop_thread = threading.get_ident()
            self._phase = RUNNING
        try:
            self._scheduler = Scheduler(self._fleet, self._clock())
            self._pool = self._pool_factory(self._size, self._worker)
            self._hooks.running()
            self._started.set()
            await asyncio.gather(
                *(feed.run(self._stop) for feed in self.feeds),
                run_scheduler(
                    self._scheduler,
                    self._queue,
                    self._stop,
                    clock=self._clock,
                    on_tick=self._ticked,
                    on_fired=self._fired,
                    wait=self._wait,
                ),
                run_workers(
                    self._queue,
                    lambda: self._fleet,
                    self._pool,
                    self._stop,
                    on_result=self._hooks.finished,
                ),
                self._provider.run(self._adopt, self._hooks.fleet_refused, self._stop),
                self._stopping(),
                run_pressure_checks(
                    lambda: self._fleet,
                    self._pool,
                    self._queue.offer,
                    self._stop,
                    clock=self._clock,
                    on_check=self._hooks.pressure_checked,
                ),
            )
        finally:
            if hasattr(self._pool, "close"):
                await self._pool.close()
            with self._lock:
                self._loop = self._loop_thread = None
                self._phase = STOPPED
            # A control call queued just before the loop left is still in the
            # loop's ready queue; one step runs it, so no caller waits forever.
            await asyncio.sleep(0)

    def _ticked(self, at: datetime) -> None:
        self._last_tick = at
        self._hooks.tick(at)

    def _fired(self, at: datetime, fired: list[str]) -> None:
        # Called in the same loop step as the tick, so `in_flight` is exactly
        # what the queue refused these offers for.
        asked = {}
        for name in fired:
            busy = {table for wh, table in self._queue.in_flight if wh == name}
            asked[name] = sorted(set(self._fleet[name].table_config.tables) - busy)
        self._hooks.fired(at, asked)

    async def _stopping(self) -> None:
        await self._stop.wait()
        self._phase = STOPPING
        self._hooks.stopping()


# -- running it beside other work -------------------------------------------------


class RuntimeHandle:
    """A runtime running on its own thread, from :func:`start_in_thread`."""

    def __init__(self, runtime: MaintenanceRuntime, thread: threading.Thread) -> None:
        self._runtime = runtime
        self._thread = thread

    @property
    def runtime(self) -> MaintenanceRuntime:
        return self._runtime

    def stop(self, *, grace: timedelta) -> bool:
        """Stop, and wait; ``True`` if the tables in flight drained within ``grace``.

        Past ``grace`` the in-flight tables' workers are killed and ``False``
        is returned once the runtime has exited.
        """
        self._runtime.request_stop()
        self._thread.join(grace.total_seconds())
        if not self._thread.is_alive():
            return True
        self._runtime.request_stop(force=True)
        self._thread.join()
        return False


def start_in_thread(runtime: MaintenanceRuntime, *, name: str = "zamboni-runtime") -> RuntimeHandle:
    """Run ``runtime`` on a dedicated thread with its own event loop (§9.2).

    Returns once the runtime's loop is running, so the control surface reaches
    the loop from the first call. Raises if the runtime exits while starting.
    """

    def main() -> None:
        try:
            asyncio.run(runtime.run())
        except BaseException:
            logger.exception("the maintenance runtime on thread %s failed", name)

    thread = threading.Thread(target=main, name=name, daemon=True)
    thread.start()
    while not runtime._started.wait(0.05):
        if not thread.is_alive():
            raise RuntimeError("the maintenance runtime exited while starting; see the log")
    return RuntimeHandle(runtime, thread)
