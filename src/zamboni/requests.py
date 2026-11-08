# SPDX-License-Identifier: Apache-2.0
"""Requests from another process: "run now" and "drop with reclaim" (§9.2).

A web tier never runs maintenance and never imports :mod:`zamboni.runtime`. It
writes a request -- a row in the application's database -- and the maintenance
tier reads it through a :class:`RequestSource`::

    RequestSource -> RequestFeed -> offer(reason=manual|reclaim) -> ack(outcome)

A source is an event source in all but two respects. **Its message is already
a request**, not a payload to parse, since the source reads its own rows. And
**its acknowledgement carries an outcome** -- ``offered``, ``unmapped`` or
``refused``, with why -- because the request's row is where the person who
asked finds out what became of it. The rules are otherwise the event rules
(§5.3): ack once the request is handled, whatever became of it, so a request
naming a table the fleet does not manage is marked and never retried; nak only
on an internal error, or when the runtime is stopping and did not consider it.

**Not debounced.** An event waits for a burst of writes to settle; a person
who asked for "run now" asked for now. A request is offered as it arrives, and
two requests for one table coalesce in the queue as any two offers do (§3.2).

**At least once.** A source must deliver each request to one replica at a time,
and never again once acknowledged (``zamboni.conformance``'s request-source
checks). A replica that dies holding one has not acknowledged it, and another
replica takes it. A request offered just as its replica died may so be offered
twice; that costs the second replica one due-check.

:class:`LocalRequests` is an in-memory request table and its sources: the
reference the conformance checks are shown against, and enough for one
process. ``tests/requests_postgres.py`` runs §9.2's SQL against PostgreSQL.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import threading
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .scheduler import MANUAL, RECLAIM

if TYPE_CHECKING:
    from .runtime import OfferReceipt

logger = logging.getLogger(__name__)

#: What a request may ask for. ``reclaim`` is "drop with reclaim"; every other
#: reason belongs to the runtime's own triggers.
REQUEST_REASONS = (MANUAL, RECLAIM)

#: The first word of every outcome: what a dashboard filters on.
OFFERED = "offered"
UNMAPPED = "unmapped"
REFUSED = "refused"

#: How long the feed waits after an internal error before reading on: a
#: request that fails every time is retried this often, not in a hot loop.
RETRY_AFTER = 1.0

#: Recent request ids remembered, so one delivered again -- its acknowledgement
#: lost -- is acknowledged with the outcome it had rather than offered again.
DEDUP_SIZE = 10_000


@dataclass(frozen=True)
class Request:
    """One request, as the source read it."""

    #: The source's identity for it -- a row's key. Unique within the source.
    id: str
    warehouse: str
    #: Fully qualified table names, ``None`` for every table of the warehouse.
    tables: tuple[str, ...] | None
    reason: str
    requested_by: str = ""


@runtime_checkable
class ReceivedRequest(Protocol):
    """One request from a source, with its acknowledgement."""

    request: Request

    async def ack(self, outcome: str) -> None:
        """Handled: record ``outcome``; never deliver it again."""

    async def nak(self) -> None:
        """Not handled: deliver it again, here or to another replica."""


@runtime_checkable
class RequestSource(Protocol):
    """Where requests come from: an :class:`~zamboni.events.feed.EventSource`
    whose messages are :class:`ReceivedRequest`."""

    name: str

    #: For readiness. True for a source with no connection to lose.
    @property
    def connected(self) -> bool: ...

    def __aiter__(self) -> AsyncIterator[ReceivedRequest]: ...


def request_outcome(receipt: OfferReceipt) -> str:
    """What became of a request, from the receipt of its offer.

    Clauses in order -- ``offered``, then ``refused``, then ``unmapped`` -- so
    the first word is the best that happened to any table: ``offered;
    unmapped: raw.typo`` ran something, ``unmapped: ...`` ran nothing.
    """
    if receipt.refused is not None:
        # The one refusal a stopping runtime does not make: the feed naks those.
        return f"{UNMAPPED}: {receipt.refused}"
    clauses = []
    if receipt.queued or receipt.coalesced:
        clauses.append(OFFERED)
    if receipt.quarantined:
        clauses.append(f"{REFUSED}: quarantined: {', '.join(receipt.quarantined)}")
    if receipt.unknown:
        clauses.append(f"{UNMAPPED}: not in the table config: {', '.join(receipt.unknown)}")
    return "; ".join(clauses) or f"{UNMAPPED}: the warehouse has no tables"


@dataclass
class RequestCounters:
    received: int = 0
    offered: int = 0
    unmapped: int = 0
    refused: int = 0
    duplicates: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


@dataclass
class RequestFeed:
    """Reads one :class:`RequestSource` and offers what each request asks for.

    ``offer(warehouse, tables, reason)`` is the runtime's own, returning its
    :class:`~zamboni.runtime.OfferReceipt`; ``stopping`` says the runtime takes
    no more, when a request is nak'd for the next replica, or the next start.
    """

    source: RequestSource
    offer: Callable[[str, tuple[str, ...] | None, str], OfferReceipt]
    stopping: Callable[[], bool]
    dedup_size: int = DEDUP_SIZE
    retry_after: float = RETRY_AFTER
    counters: RequestCounters = field(default_factory=RequestCounters)
    _seen: OrderedDict[str, str] = field(default_factory=OrderedDict)

    def handle(self, request: Request) -> str | None:
        """The outcome to acknowledge ``request`` with; ``None`` to nak it."""
        self.counters.received += 1
        seen = self._seen.get(request.id)
        if seen is not None:
            self.counters.duplicates += 1
            return seen
        if self.stopping():
            return None
        if request.reason not in REQUEST_REASONS:
            wanted = " or ".join(REQUEST_REASONS)
            outcome = f"{REFUSED}: a request asks for {wanted}, not {request.reason!r}"
        elif request.tables is not None and not request.tables:
            outcome = f"{REFUSED}: names no table; NULL asks for every table"
        else:
            outcome = request_outcome(self.offer(request.warehouse, request.tables, request.reason))
        head = outcome.partition(";")[0].partition(":")[0]
        if head == OFFERED:
            self.counters.offered += 1
        elif head == UNMAPPED:
            self.counters.unmapped += 1
        else:
            self.counters.refused += 1
        self._seen[request.id] = outcome
        while len(self._seen) > self.dedup_size:
            self._seen.popitem(last=False)
        return outcome

    async def run(self, stop: asyncio.Event) -> None:
        """Read the source until ``stop``.

        A request in hand when ``stop`` is set is finished -- acknowledged, or
        nak'd if it was not yet offered -- before the source is closed: cut
        off mid-acknowledgement, a source would deliver again a request this
        runtime already offered. A source that ends or fails is logged, once,
        and the schedule carries on; requests wait in their table.
        """
        settled = asyncio.Event()
        settled.set()
        reader = asyncio.ensure_future(self._read(settled))
        stopped = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({reader, stopped}, return_when=asyncio.FIRST_COMPLETED)
            if reader.done() and not reader.cancelled() and reader.exception() is not None:
                logger.error("request source %s failed: %r", self.source.name, reader.exception())
            await stopped
            await settled.wait()
        finally:
            stopped.cancel()
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await reader

    async def _read(self, settled: asyncio.Event) -> None:
        async for message in self.source:
            settled.clear()
            failed = False
            try:
                try:
                    outcome = self.handle(message.request)
                except Exception:  # an internal error, not the request's fault
                    self.counters.errors += 1
                    logger.exception("handling a request from %s", self.source.name)
                    outcome, failed = None, True
                if outcome is None:
                    await message.nak()
                else:
                    await message.ack(outcome)
            finally:
                settled.set()
            if failed:
                await asyncio.sleep(self.retry_after)
            elif outcome is None:
                return  # stopping: read no more


# -- in memory --------------------------------------------------------------------


@dataclass
class _Row:
    request: Request
    holder: object | None = None
    outcome: str | None = None


class LocalRequests:
    """An in-memory request table, safe from any thread: requests submitted
    here are read by any of its :meth:`source`\\ s, each by one at a time."""

    def __init__(self) -> None:
        self._rows: dict[str, _Row] = {}
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    def submit(
        self,
        warehouse: str,
        tables: Sequence[str] | None = None,
        *,
        reason: str = MANUAL,
        requested_by: str = "",
    ) -> str:
        """Add a request; its id."""
        with self._lock:
            id = str(next(self._ids))
            wanted = None if tables is None else tuple(tables)
            self._rows[id] = _Row(Request(id, warehouse, wanted, reason, requested_by))
        return id

    def outcome(self, id: str) -> str | None:
        """What became of request ``id``: ``None`` until it is handled."""
        with self._lock:
            return self._rows[id].outcome

    def source(
        self, name: str = "local requests", *, poll_every: float = 0.2
    ) -> LocalRequestSource:
        return LocalRequestSource(self, name, poll_every)

    def _take(self, holder: object) -> _Row | None:
        with self._lock:
            for row in self._rows.values():
                if row.outcome is None and row.holder is None:
                    row.holder = holder
                    return row
        return None

    def _settle(self, row: _Row, holder: object, outcome: str | None) -> None:
        with self._lock:
            if row.holder is holder:
                row.holder, row.outcome = None, outcome


class LocalRequestSource:
    """One replica's reader of a :class:`LocalRequests`. Polls, as a reader of
    a database table does; nothing held between polls."""

    connected = True

    def __init__(self, requests: LocalRequests, name: str, poll_every: float) -> None:
        self.name = name
        self._requests = requests
        self._poll_every = poll_every

    async def __aiter__(self) -> AsyncIterator[ReceivedRequest]:
        holder = object()
        while True:
            row = self._requests._take(holder)
            if row is None:
                await asyncio.sleep(self._poll_every)
                continue
            await asyncio.sleep(0)  # a read from a table is never instant
            received = _LocalReceived(self._requests, row, holder)
            try:
                yield received
            finally:
                # Closed, or asked for the next, with this one unsettled: it
                # goes back for another reader, as a crashed replica's would.
                if not received.settled:
                    self._requests._settle(row, holder, None)


class _LocalReceived:
    def __init__(self, requests: LocalRequests, row: _Row, holder: object) -> None:
        self.request = row.request
        self._requests, self._row, self._holder = requests, row, holder
        self.settled = False

    async def ack(self, outcome: str) -> None:
        self._requests._settle(self._row, self._holder, outcome)
        self.settled = True

    async def nak(self) -> None:
        self._requests._settle(self._row, self._holder, None)
        self.settled = True
