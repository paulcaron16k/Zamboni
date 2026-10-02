# SPDX-License-Identifier: Apache-2.0
"""The scheduler and the candidate queue: the front half of ``zamboni serve``.

::

  fleet ──▶ Scheduler.tick ──┐
                             ├──▶ CandidateQueue ──▶ WorkItem ──▶ worker ──▶ maintain()
       (#110) NATS consumer ─┘     (coalesced)

**Nothing here decides whether a table needs work.** A tick offers every table in
a due warehouse; later an event offers one table (#110). Both only *offer*, and
the decision stays where it already is -- the due-check inside
:func:`zamboni.maintenance.maintain` -- so scheduled and event-driven maintenance
cannot drift apart, and an event can never cause work the schedule would not
eventually have done (docs/event-driven-maintenance.md §3). That is why a
:class:`WorkItem` has no "force" field: there is no way to say "skip the check"
through this layer, because nothing in it could honour one.

**The queue holds keys, not config.** A candidate is ``(warehouse, table)``; the
:class:`WorkItem` a worker receives is built from the fleet *at the moment it is
taken*. So a reload (#119) changes every decision not yet started and none in
flight, and a warehouse or table removed by a reload drops out of the queue
without anyone having to find and cancel it.

Single-threaded by design: the scheduler, the queue and (later) the event
consumer share one asyncio loop, so none of this locks. The work itself runs
elsewhere -- a process pool over tables (#121) -- and reports back through
:meth:`CandidateQueue.finish`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .fleet import FleetConfig, FleetWarehouse
from .tableconfig import TableConfig

logger = logging.getLogger(__name__)

#: The longest the scheduler loop sleeps in one step, whatever the next firing.
#: Bounds two things: how late a firing can be after a wall-clock jump the sleep
#: did not see (a suspended VM, an NTP step), and how stale the "last tick"
#: liveness signal (#122) can get while the loop is healthy.
MAX_SLEEP = timedelta(seconds=60)

SCHEDULE = "schedule"
EVENT = "event"


@dataclass(frozen=True)
class Candidate:
    """A table someone thinks is worth looking at. Not a decision."""

    warehouse: str
    table: str
    #: ``"schedule"`` or ``"event"``: why it was offered, for reporting only.
    reason: str = SCHEDULE

    @property
    def key(self) -> tuple[str, str]:
        return (self.warehouse, self.table)


@dataclass(frozen=True)
class WorkItem:
    """What a worker receives: config, never a live session.

    A ``CatalogSession`` holds a DuckDB connection and a catalog client and
    cannot cross a process boundary, so the worker builds its own from this
    plus the operator profile (#121). Everything here pickles.
    """

    warehouse: str
    table: str
    reason: str
    table_config: TableConfig
    #: The warehouse's own catalog URI, or ``None`` for the profile's.
    uri: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.warehouse, self.table)


class CandidateQueue:
    """FIFO over tables, coalesced per table.

    * Offering a table already queued does nothing: fifty events during one
      load are one candidate.
    * Offering a table **being maintained** does nothing either. A tick or an
      event that lands mid-run is dropped rather than queued behind it. For a
      tick that is exactly right -- the run in flight is the work the tick
      asked for. For an event it can lose a write that landed after the run
      read its watermark; the next scheduled tick bounds that, and #110 is
      where it gets revisited if the bound proves too loose.
    """

    def __init__(self) -> None:
        self._queued: OrderedDict[tuple[str, str], Candidate] = OrderedDict()
        self._in_flight: set[tuple[str, str]] = set()
        self._ready = asyncio.Event()

    def __len__(self) -> int:
        return len(self._queued)

    @property
    def in_flight(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._in_flight)

    def offer(self, candidate: Candidate) -> bool:
        """Queue ``candidate``; ``False`` if it coalesced into existing work."""
        if candidate.key in self._queued or candidate.key in self._in_flight:
            return False
        self._queued[candidate.key] = candidate
        self._ready.set()
        return True

    def take(self, fleet: FleetConfig) -> WorkItem | None:
        """The next item, resolved against ``fleet``, and mark it in flight.

        Candidates for a warehouse or table ``fleet`` no longer declares are
        discarded on the way -- the reload that removed them is the decision.
        """
        while self._queued:
            key, candidate = self._queued.popitem(last=False)
            item = _resolve(candidate, fleet)
            if item is None:
                logger.info(
                    "dropping %s/%s: no longer in the fleet", candidate.warehouse, candidate.table
                )
                continue
            self._in_flight.add(key)
            if not self._queued:
                self._ready.clear()
            return item
        self._ready.clear()
        return None

    async def get(self, fleet: Callable[[], FleetConfig]) -> WorkItem:
        """Wait for the next item. ``fleet`` is called at take time, not before."""
        while True:
            item = self.take(fleet())
            if item is not None:
                return item
            await self._ready.wait()

    def finish(self, item: WorkItem) -> None:
        """The worker is done with ``item``; the table may be offered again."""
        self._in_flight.discard(item.key)


def _resolve(candidate: Candidate, fleet: FleetConfig) -> WorkItem | None:
    try:
        warehouse = fleet[candidate.warehouse]
    except KeyError:
        return None
    if candidate.table not in warehouse.table_config.tables:
        return None
    return WorkItem(
        warehouse=warehouse.name,
        table=candidate.table,
        reason=candidate.reason,
        table_config=warehouse.table_config,
        uri=warehouse.uri,
    )


@dataclass(frozen=True)
class _Armed:
    #: The firing the cron expression names.
    nominal: datetime
    #: When it is actually offered: ``nominal`` plus the schedule's offset,
    #: which is zero unless the schedule is ``random``.
    due: datetime


def _arm(warehouse: FleetWarehouse, after: datetime) -> _Armed:
    nominal = warehouse.schedule.next_after(after)
    return _Armed(nominal, nominal + warehouse.schedule.offset(nominal, warehouse.name))


class Scheduler:
    """Per-warehouse cron firings, as pure functions of the time it is given.

    **A missed firing fires once, late, rather than never or many times.** If a
    tick arrives after two firings have passed -- the loop was busy, the host
    was suspended -- the warehouse is offered once and re-armed from now. Every
    table would coalesce to one candidate anyway; the point is not to lose the
    firing, which a strict "only at the exact minute" rule would.

    **A new warehouse, and a fresh start, arm from now.** A service started at
    02:01 does not run the 02:00 schedule it just missed; it waits for the next
    one, as cron would. Running everything at startup would make every restart
    -- a rollout, a reschedule by Kubernetes -- a full fleet sweep. (With
    ``random``, a firing whose window has already opened -- started at 01:45,
    due at 01:35 for a nominal 02:00 -- is offered on the first tick: it is
    this firing, not a missed one.)

    **A firing moved early is still that firing.** The next one is armed from
    after the *nominal* time, so a 02:00 offered at 01:40 does not find 02:00
    still ahead of it and fire twice.
    """

    def __init__(self, fleet: FleetConfig, now: datetime) -> None:
        self._fleet = fleet
        self._armed: dict[str, _Armed] = {w.name: _arm(w, now) for w in fleet.warehouses}

    @property
    def fleet(self) -> FleetConfig:
        return self._fleet

    def next_firing(self, warehouse: str) -> datetime:
        """When ``warehouse`` is next offered, offset included."""
        return self._armed[warehouse].due

    def wake_at(self) -> datetime:
        """When the earliest warehouse is next due."""
        return min(armed.due for armed in self._armed.values())

    def tick(self, now: datetime, queue: CandidateQueue) -> list[str]:
        """Offer every table of every due warehouse; the warehouses that fired."""
        fired = []
        for warehouse in self._fleet.warehouses:
            armed = self._armed[warehouse.name]
            if armed.due > now:
                continue
            fired.append(warehouse.name)
            self._armed[warehouse.name] = _arm(warehouse, max(now, armed.nominal))
            for table in sorted(warehouse.table_config.tables):
                queue.offer(Candidate(warehouse.name, table, SCHEDULE))
        return fired

    def rearm(self, fleet: FleetConfig, now: datetime) -> None:
        """Adopt a reloaded fleet.

        A warehouse whose schedule is unchanged keeps the firing it was already
        waiting for -- re-arming it from now would silently push a 02:00 run to
        tomorrow whenever an unrelated tenant was provisioned at 01:59. A changed
        or new schedule arms from now -- and turning ``random`` on or off is a
        change -- while a removed warehouse is forgotten.

        Seen by :func:`run_scheduler` at its next step, so a reload that brings
        a firing forward takes effect at most :data:`MAX_SLEEP` late.
        """
        armed = {}
        for warehouse in fleet.warehouses:
            name = warehouse.name
            try:
                unchanged = self._fleet[name].schedule == warehouse.schedule
            except KeyError:
                unchanged = False
            armed[name] = self._armed[name] if unchanged else _arm(warehouse, now)
        self._fleet = fleet
        self._armed = armed


async def run_scheduler(
    scheduler: Scheduler,
    queue: CandidateQueue,
    stop: asyncio.Event,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    on_tick: Callable[[datetime], None] | None = None,
    on_fired: Callable[[datetime, list[str]], None] | None = None,
    wait: Callable[[asyncio.Event, float], Awaitable[None]] | None = None,
) -> None:
    """Tick until ``stop`` is set.

    ``on_tick`` is called after every tick, fired or not -- the liveness signal
    #122 writes to the state file. Liveness asserts that this loop runs, and
    nothing else, so it is called here rather than from anything that touches
    a catalog. ``on_fired`` receives the warehouses each tick fired, for the
    service's sweep tracking. ``wait`` replaces the sleep, for a test driving
    ``clock``.
    """
    wait = wait or _wait
    while not stop.is_set():
        now = clock()
        fired = scheduler.tick(now, queue)
        if fired:
            logger.info("schedule fired for %s; %d table(s) queued", fired, len(queue))
            if on_fired is not None:
                on_fired(now, fired)
        if on_tick is not None:
            on_tick(now)
        delay = min(scheduler.wake_at() - clock(), MAX_SLEEP).total_seconds()
        await wait(stop, max(delay, 0.0))


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    """Sleep ``seconds``, returning early if ``stop`` is set."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
