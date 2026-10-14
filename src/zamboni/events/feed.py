# SPDX-License-Identifier: Apache-2.0
"""Turning received events into offers: the transport-free half of the consumer.

docs/maintenance-runtime.md §5.3-§5.4. An :class:`EventSource` is anything that
yields received messages -- NATS (``zamboni.events.nats``), or an adapter over
another broker. An :class:`EventFeed` reads one source and, for each message:

    parse -> map to a managed table -> dedup -> debounce -> offer(reason=event) -> ack

Nothing here decides whether a table needs work. A feed only *offers*; the
due-check decides, and an event-offered table runs only the write-driven
operations (``zamboni.scheduler.REASON_OPERATIONS``). So an event can never
cause work the schedule would not eventually have done.

This module is shared by `zamboni serve` today and by the maintenance runtime
(docs/maintenance-runtime.md) tomorrow, which is why it takes its fleet and its
queue as callables rather than reaching for either -- and why telemetry is a
:class:`FeedObserver` it is handed, not an OpenTelemetry import (#181).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from ..scheduler import EVENT, Candidate
from . import EventError, Mapped, map_event, parse_event

logger = logging.getLogger(__name__)

#: How long a table waits after its last event before it is offered -- a burst
#: of writes settles into one run. Decided on #110 (2026-10-06).
QUIET = timedelta(minutes=5)
#: ...and the longest it waits after its *first* event, so a table written
#: every minute is still maintained.
CAP = timedelta(minutes=30)
#: Recent (warehouse, table, snapshot_id) remembered for dedup. Bounded so a
#: long-running service's memory is too; an older duplicate falls to the
#: due-check, which makes it cost one metadata load.
DEDUP_SIZE = 10_000

#: What happened to one message: :meth:`EventFeed.handle`'s answers, plus
#: ``error`` when handling raised. Pending and coalesced are both accepted -- the
#: first event for a table starts its quiet period, a later one extends it.
PENDING = "pending"
COALESCED = "coalesced"
DUPLICATE = "duplicate"
UNMAPPED = "unmapped"
REJECTED = "rejected"
ERROR = "error"
OUTCOMES = (PENDING, COALESCED, DUPLICATE, UNMAPPED, REJECTED, ERROR)


@runtime_checkable
class ReceivedEvent(Protocol):
    """One message from a source, with its acknowledgement."""

    payload: bytes

    async def ack(self) -> None: ...

    async def nak(self) -> None: ...


@runtime_checkable
class EventSource(Protocol):
    """Where events come from. A source with no acknowledgement (core NATS)
    implements `ack` and `nak` as no-ops."""

    name: str

    #: For readiness. True for a source with no connection to lose.
    @property
    def connected(self) -> bool: ...

    def __aiter__(self) -> AsyncIterator[ReceivedEvent]: ...


class FeedObserver(Protocol):
    """Told what the feed did -- for telemetry, without the feed importing any.

    Called inline from the feed and **never allowed to affect it**: anything an
    observer raises is logged and swallowed, as for every reporter.
    """

    def consumed(
        self, source: EventSource, outcome: str, seconds: float, error: BaseException | None
    ) -> None:
        """One message handled: its outcome (:data:`OUTCOMES`), the seconds from
        receiving it to settling it, and the exception when ``outcome`` is
        :data:`ERROR`."""

    def offered(self, source: EventSource, candidate: Candidate) -> None:
        """A table offered once its quiet period, or cap, passed."""


@dataclass
class FeedCounters:
    received: int = 0
    offered: int = 0
    coalesced: int = 0
    duplicates: int = 0
    unmapped: int = 0
    rejected: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


@dataclass
class _Pending:
    first: datetime
    last: datetime

    def due(self, quiet: timedelta, cap: timedelta) -> datetime:
        return min(self.last + quiet, self.first + cap)


@dataclass
class EventFeed:
    """Reads one :class:`EventSource` and offers the tables it reports."""

    source: EventSource
    fleet: Callable[[], Any]
    offer: Callable[[Candidate], bool]
    quiet: timedelta = QUIET
    cap: timedelta = CAP
    dedup_size: int = DEDUP_SIZE
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    counters: FeedCounters = field(default_factory=FeedCounters)
    observer: FeedObserver | None = None
    #: Monotonic seconds, for how long a message took; separate from ``clock``,
    #: which is wall time and may jump.
    timer: Callable[[], float] = time.perf_counter
    _seen: OrderedDict[tuple[str, str, int], None] = field(default_factory=OrderedDict)
    _pending: dict[tuple[str, str], _Pending] = field(default_factory=dict)
    #: Set when a table first becomes pending, so the flusher wakes for it
    #: instead of finishing a sleep chosen before it arrived.
    _woken: asyncio.Event = field(default_factory=asyncio.Event)
    _observer_failed: bool = False

    # -- the pure half: one message in, a decision out --------------------------------

    def handle(self, payload: bytes) -> str:
        """Process one payload. Returns what happened, for counting and tests."""
        self.counters.received += 1
        try:
            event = parse_event(payload)
        except EventError as exc:
            # Rejected once, never redelivered: a malformed message does not
            # improve by being sent again.
            self.counters.rejected += 1
            logger.warning("rejected an event from %s: %s", self.source.name, exc)
            return REJECTED
        mapped = map_event(event, self.fleet())
        if not isinstance(mapped, Mapped):
            self.counters.unmapped += 1
            logger.debug("unmapped event %s: %s", event.id, mapped.reason)
            return UNMAPPED
        identity = (mapped.warehouse, mapped.table, event.data.snapshot_id)
        if identity in self._seen:
            self.counters.duplicates += 1
            return DUPLICATE
        self._seen[identity] = None
        while len(self._seen) > self.dedup_size:
            self._seen.popitem(last=False)
        key = (mapped.warehouse, mapped.table)
        now = self.clock()
        if key in self._pending:
            self._pending[key].last = now
            self.counters.coalesced += 1
            return COALESCED
        self._pending[key] = _Pending(first=now, last=now)
        self._woken.set()
        return PENDING

    def flush_due(self, now: datetime | None = None) -> list[tuple[str, str]]:
        """Offer every pending table whose quiet period, or cap, has passed."""
        now = now or self.clock()
        due = [key for key, p in self._pending.items() if p.due(self.quiet, self.cap) <= now]
        for key in due:
            del self._pending[key]
            candidate = Candidate(key[0], key[1], EVENT)
            self.offer(candidate)
            self.counters.offered += 1
            self._observe("offered", self.source, candidate)
        return due

    def next_due(self) -> datetime | None:
        if not self._pending:
            return None
        return min(p.due(self.quiet, self.cap) for p in self._pending.values())

    @property
    def pending(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._pending)

    # -- the async half: the source in, flushes on time --------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        """Consume the source and flush on time until ``stop``.

        Tables still in their quiet period when the feed stops are not offered:
        a stopping service takes no new work, and the schedule is the backstop.
        """
        reader = asyncio.ensure_future(self._read())
        reported = False
        try:
            while not stop.is_set():
                self.flush_due()
                upcoming = self.next_due()
                # Sleep until the next table is due -- capped, so a clock jump
                # is noticed -- or until stop, or until a new table is pending.
                # With nothing pending there is nothing to time: wait for either.
                timeout = (
                    None
                    if upcoming is None
                    else min(max((upcoming - self.clock()).total_seconds(), 0.0), 60.0)
                )
                self._woken.clear()
                waits = {
                    asyncio.ensure_future(stop.wait()),
                    asyncio.ensure_future(self._woken.wait()),
                }
                # The reader too, while it runs, so a source that fails is
                # reported when it fails -- not at the next event. Never once it
                # is done: a finished task would return every wait at once.
                watch = waits | ({reader} if not reader.done() else set())
                await asyncio.wait(watch, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                for task in waits:
                    task.cancel()
                if reader.done() and not reported:
                    # A source that ends or fails stops being read, but tables
                    # already in their quiet period are still offered, and the
                    # schedule keeps maintaining the fleet. Say why, once.
                    reported = True
                    exc = None if reader.cancelled() else reader.exception()
                    if exc is not None:
                        logger.error("event source %s failed: %r", self.source.name, exc)
        finally:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await reader

    async def _read(self) -> None:
        async for message in self.source:
            started = self.timer()
            error: BaseException | None = None
            try:
                outcome = self.handle(message.payload)
            except Exception as exc:  # an internal error, not the message's fault
                self.counters.errors += 1
                logger.exception("handling an event from %s", self.source.name)
                outcome, error = ERROR, exc
                await message.nak()
            else:
                await message.ack()
            self._observe("consumed", self.source, outcome, self.timer() - started, error)

    def _observe(self, method: str, *args: Any) -> None:
        """Tell the observer, which can never fail the feed. Its first failure
        is logged with the traceback; later ones only at debug, so a broken
        exporter does not log once per message."""
        if self.observer is None:
            return
        try:
            getattr(self.observer, method)(*args)
        except Exception:
            logger.log(
                logging.DEBUG if self._observer_failed else logging.WARNING,
                "feed observer %s failed; the feed carries on",
                method,
                exc_info=True,
            )
            self._observer_failed = True
