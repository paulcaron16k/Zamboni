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
queue as callables rather than reaching for either.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
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
    _seen: OrderedDict[tuple[str, str, int], None] = field(default_factory=OrderedDict)
    _pending: dict[tuple[str, str], _Pending] = field(default_factory=dict)
    #: Set when a table first becomes pending, so the flusher wakes for it
    #: instead of finishing a sleep chosen before it arrived.
    _woken: asyncio.Event = field(default_factory=asyncio.Event)

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
            return "rejected"
        mapped = map_event(event, self.fleet())
        if not isinstance(mapped, Mapped):
            self.counters.unmapped += 1
            logger.debug("unmapped event %s: %s", event.id, mapped.reason)
            return "unmapped"
        identity = (mapped.warehouse, mapped.table, event.data.snapshot_id)
        if identity in self._seen:
            self.counters.duplicates += 1
            return "duplicate"
        self._seen[identity] = None
        while len(self._seen) > self.dedup_size:
            self._seen.popitem(last=False)
        key = (mapped.warehouse, mapped.table)
        now = self.clock()
        if key in self._pending:
            self._pending[key].last = now
            self.counters.coalesced += 1
            return "coalesced"
        self._pending[key] = _Pending(first=now, last=now)
        self._woken.set()
        return "pending"

    def flush_due(self, now: datetime | None = None) -> list[tuple[str, str]]:
        """Offer every pending table whose quiet period, or cap, has passed."""
        now = now or self.clock()
        due = [key for key, p in self._pending.items() if p.due(self.quiet, self.cap) <= now]
        for key in due:
            del self._pending[key]
            self.offer(Candidate(key[0], key[1], EVENT))
            self.counters.offered += 1
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
            try:
                self.handle(message.payload)
            except Exception:  # an internal error, not the message's fault
                self.counters.errors += 1
                logger.exception("handling an event from %s", self.source.name)
                await message.nak()
                continue
            await message.ack()
