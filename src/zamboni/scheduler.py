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
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .fleet import FleetConfig, FleetWarehouse
from .settings import DEFAULT_OPERATIONS
from .tableconfig import TableConfig

logger = logging.getLogger(__name__)

#: The longest the scheduler loop sleeps in one step, whatever the next firing.
#: Bounds two things: how late a firing can be after a wall-clock jump the sleep
#: did not see (a suspended VM, an NTP step), and how stale the "last tick"
#: liveness signal (#122) can get while the loop is healthy.
MAX_SLEEP = timedelta(seconds=60)

SCHEDULE = "schedule"
EVENT = "event"
PRESSURE = "pressure"
MANUAL = "manual"
RECLAIM = "reclaim"

#: Which operations each reason runs (docs/maintenance-runtime.md §3.1). `None`
#: is "every operation the profile enables". Names, not `Operation` members, so
#: this module stays free of the engine; a test checks them against
#: `zamboni.maintenance.WRITE_DRIVEN` and `RUNBOOK_ORDER`. The due-check still
#: decides, within these, whether each operation has anything to do.
#:
#: The sets do not nest -- `reclaim` and `event` share nothing -- so two offers
#: for one table merge by **unioning** their operations (:func:`operations_of`),
#: never by keeping one reason's set.
REASON_OPERATIONS: dict[str, tuple[str, ...] | None] = {
    SCHEDULE: None,
    EVENT: ("compact", "remove-dangling-deletes", "rewrite-manifests"),
    PRESSURE: ("compact", "remove-dangling-deletes"),
    MANUAL: None,
    RECLAIM: ("expire", "remove-orphans"),
}

#: The reason a merged candidate *reports*, first wins (§3.2). Reporting only:
#: what runs is the union of every merged reason's operations. `manual` leads
#: because an operator asked; `reclaim` outranks the write-driven pair because it
#: deletes, and a log line should say so.
_REPORT_ORDER = (MANUAL, SCHEDULE, RECLAIM, EVENT, PRESSURE)

#: Reasons that run a table in flight once more when it finishes even if the run
#: in flight covers their operations. Only an event: it reports a new write the
#: run in flight may have read the table before. Any other offer is re-run only
#: for operations the run in flight is not doing (:meth:`CandidateQueue.offer`).
#: So a schedule tick during a scheduled run is dropped -- that run *is* the
#: tick's work, and re-running it would repeat `expire` and the orphan scan for
#: nothing -- and pressure during any run is dropped (#175): every run that
#: pressure asks for compacts, and the next check measures the table it left.
_REOFFER_AFTER_RUN = frozenset({EVENT})


class UnknownReasonError(ValueError):
    """A work reason :data:`REASON_OPERATIONS` does not name."""


def operations_for(reason: str) -> tuple[str, ...] | None:
    """The operations ``reason`` runs; ``None`` is every one the profile enables.

    **Refuses a reason it does not know** rather than answering ``None``. A
    lookup that defaulted would read a misspelt ``"Event"`` as "every
    operation" -- `expire` and `remove-orphans` included, and with ``--yes``
    when the service has it -- so a typo would widen a run to the reclaim
    operations instead of failing it (ZMBNI-195, found by the 0.6.0 security
    review). Unreachable while every reason is one of the constants above; it
    stops being so the first time a reason arrives as a string.
    """
    try:
        return REASON_OPERATIONS[reason]
    except KeyError:
        raise UnknownReasonError(
            f"unknown work reason {reason!r}; known: {', '.join(sorted(REASON_OPERATIONS))}"
        ) from None


def operations_of(reasons: Iterable[str]) -> tuple[str, ...] | None:
    """The union of ``reasons``' operations, in runbook order; ``None`` is every one.

    Runbook order because `maintain()` runs operations in the order given, and
    the gaps between them are load-bearing (docs/runbook-dev.md §1).
    """
    wanted: set[str] = set()
    for reason in reasons:
        operations = operations_for(reason)
        if operations is None:
            return None
        wanted.update(operations)
    return tuple(op for op in DEFAULT_OPERATIONS if op in wanted)


def covers(running: tuple[str, ...] | None, asked: tuple[str, ...] | None) -> bool:
    """Whether a run of ``running`` does every operation ``asked`` names."""
    if running is None:
        return True
    return asked is not None and set(asked) <= set(running)


def stronger(a: str, b: str) -> str:
    """The reason a merge of ``a`` and ``b`` reports (:data:`_REPORT_ORDER`).

    An unknown reason is refused, not ranked: it used to rank as a schedule,
    the strongest, and so win every merge.
    """
    for reason in (a, b):
        operations_for(reason)
    return min(a, b, key=_REPORT_ORDER.index)


def check_offer(
    reason: str, also: frozenset[str] = frozenset(), not_after: datetime | None = None
) -> None:
    """Refuse an offer that names an unknown reason or a naive deadline."""
    for each in (reason, *also):
        operations_for(each)
    if not_after is not None and not_after.utcoffset() is None:
        # Compared with an aware clock on the loop, a naive deadline raises
        # there -- in the take-and-run loop -- rather than here, at the offer.
        raise ValueError(f"not_after must carry a time zone: {not_after!r}")


@dataclass(frozen=True)
class Candidate:
    """A table someone thinks is worth looking at. Not a decision."""

    warehouse: str
    table: str
    #: Why it was offered (:data:`REASON_OPERATIONS`). Decides which operations
    #: run, never whether. After a merge, the reason reported (:func:`stronger`).
    reason: str = SCHEDULE
    #: The other reasons merged into this candidate, for reporting; their
    #: operations run too.
    also: frozenset[str] = frozenset()
    #: A worker does not start this table after this instant (§3.3). ``None``:
    #: no deadline.
    not_after: datetime | None = None

    def __post_init__(self) -> None:
        check_offer(self.reason, self.also, self.not_after)

    @property
    def key(self) -> tuple[str, str]:
        return (self.warehouse, self.table)

    @property
    def reasons(self) -> frozenset[str]:
        return self.also | {self.reason}

    @property
    def operations(self) -> tuple[str, ...] | None:
        """What a run for this candidate does: every merged reason's operations."""
        return operations_of(self.reasons)


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
    #: As on :class:`Candidate`.
    also: frozenset[str] = frozenset()
    not_after: datetime | None = None

    def __post_init__(self) -> None:
        check_offer(self.reason, self.also, self.not_after)

    @property
    def key(self) -> tuple[str, str]:
        return (self.warehouse, self.table)

    @property
    def reasons(self) -> frozenset[str]:
        return self.also | {self.reason}

    @property
    def operations(self) -> tuple[str, ...] | None:
        return operations_of(self.reasons)

    def candidate(self) -> Candidate:
        """This item as the offer it was taken from, to put back in the queue."""
        return Candidate(self.warehouse, self.table, self.reason, self.also, self.not_after)


#: The ``reason`` a :class:`PressureCheck` reports, for logs and run records.
#: Not a reason a table is *offered* with -- a check offers nothing to the
#: queue itself; the service offers what it finds with :data:`PRESSURE`.
PRESSURE_CHECK = "pressure-check"


@dataclass(frozen=True)
class PressureCheck:
    """A worker job that measures every table of one warehouse (#175).

    Shaped like :class:`WorkItem` where the pool reads one -- warehouse, a
    ``table`` to name in logs, a reason, the table config and the URI -- so the
    same worker, crash handling and recycling serve both. The worker runs
    ``zamboni health --table-config ... --json ...``: one metadata load per
    table, and the engine stays in the worker, out of the service process.
    """

    warehouse: str
    table_config: TableConfig
    uri: str | None = None
    table: str = "(pressure check)"
    reason: str = PRESSURE_CHECK

    @property
    def key(self) -> tuple[str, str]:
        return (self.warehouse, self.table)


class CandidateQueue:
    """FIFO over tables, coalesced per table.

    * Offering a table already queued **merges** (:func:`_merged`): fifty
      events during one load are one candidate, which runs the union of every
      merged reason's operations, under the later deadline or none.
    * Offering a table **being maintained** never queues it behind itself. An
      offer whose operations the run in flight already does is dropped -- a
      schedule tick during a scheduled run, pressure during any run that
      compacts. Any other offer marks the table **dirty**, and when the run
      finishes the table is offered once more with every such offer merged:
      an event because the run in flight may have read the table before the
      write it reports (decided on #110, 2026-10-06), anything else for the
      operations the run in flight is not doing (#163). The due-check makes a
      re-run that finds nothing new cost one metadata load.
    """

    def __init__(self) -> None:
        self._queued: OrderedDict[tuple[str, str], Candidate] = OrderedDict()
        #: Each table in flight, and what its run does.
        self._in_flight: dict[tuple[str, str], tuple[str, ...] | None] = {}
        self._dirty: dict[tuple[str, str], Candidate] = {}
        self._ready = asyncio.Event()

    def __len__(self) -> int:
        return len(self._queued)

    @property
    def in_flight(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._in_flight)

    @property
    def queued(self) -> tuple[tuple[str, str], ...]:
        """The queued tables, in the order they will be taken."""
        return tuple(self._queued)

    def offer(self, candidate: Candidate) -> bool:
        """Queue ``candidate``; ``False`` if it coalesced into existing work."""
        key = candidate.key
        if key in self._in_flight:
            if candidate.reasons & _REOFFER_AFTER_RUN or not covers(
                self._in_flight[key], candidate.operations
            ):
                earlier = self._dirty.get(key)
                self._dirty[key] = candidate if earlier is None else _merged(earlier, candidate)
            return False
        if key in self._queued:
            # Merge in place: the entry keeps its position.
            self._queued[key] = _merged(self._queued[key], candidate)
            return False
        self._queued[key] = candidate
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
            self._in_flight[key] = item.operations
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
        """The worker is done with ``item``; re-offer it once if it went dirty."""
        self._in_flight.pop(item.key, None)
        dirty = self._dirty.pop(item.key, None)
        if dirty is not None:
            self.offer(dirty)

    @property
    def dirty(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._dirty)


def _merged(a: Candidate, b: Candidate) -> Candidate:
    """One candidate doing everything ``a`` and ``b`` asked for (§3.2).

    Operations: the union. Deadline: none if either has none -- one offer that
    may run at any time is not narrowed by another's window -- else the later.
    """
    reasons = a.reasons | b.reasons
    reason = stronger(a.reason, b.reason)
    not_after = (
        None if a.not_after is None or b.not_after is None else max(a.not_after, b.not_after)
    )
    return Candidate(a.warehouse, a.table, reason, reasons - {reason}, not_after)


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
        also=candidate.also,
        not_after=candidate.not_after,
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
