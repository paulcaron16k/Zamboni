# SPDX-License-Identifier: Apache-2.0
"""Per-table claims: "this replica is maintaining this table now" (docs/maintenance-runtime.md §6).

A claim stops two runtimes maintaining one table at once. That is a safety
property, not an economy: orphan removal on one replica can delete the files of
a compaction still in flight on another, before it commits.

:class:`ClaimStore` is the protocol a deployment implements against whatever
both replicas can reach -- the reference is a PostgreSQL table (§6.1).
:class:`LocalClaimStore` is the in-process store ``zamboni serve`` uses, one
replica. Every implementation must pass
:func:`zamboni.conformance.claim_store_conformance`.

**Provisional (0.8.0)**, with the rest of the runtime.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import logging
import os
import socket
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class Claim:
    warehouse: str
    table: str
    #: Who holds it: ``<host>:<pid>:<runtime-uuid>`` (:func:`new_holder`).
    holder: str
    #: Fencing token: strictly increasing over every successful acquire of the
    #: table, across releases, so a stale holder is never taken for the current one.
    token: int
    #: On the store's clock, never the caller's.
    expires_at: datetime


class ClaimStore(Protocol):
    """Where claims live. **Implement exactly** (§6):

    1. ``acquire`` is one atomic compare-and-set: it succeeds if the table has
       no claim or its claim has expired, and returns ``None`` otherwise.
    2. Time is the store's clock: expiry is compared against the store's own
       ``now``, so replicas with skewed clocks agree.
    3. ``renew`` and ``release`` act only on the caller's own claim -- matching
       ``holder`` *and* ``token``. ``renew`` returns ``None`` if the claim is
       gone or expired.
    4. The token increases on every successful ``acquire``, across releases too.
    5. Claims are per table; a warehouse is never claimed as a whole.

    Called from worker threads, never the runtime's loop, so a store may block;
    it must be safe to call from several threads at once.
    """

    def acquire(
        self, warehouse: str, table: str, *, holder: str, lease: timedelta, reason: str
    ) -> Claim | None: ...

    def renew(self, claim: Claim, *, lease: timedelta) -> Claim | None: ...

    def release(self, claim: Claim) -> None: ...


def new_holder() -> str:
    """``<host>:<pid>:<uuid>``: unique to one runtime, and says where it runs."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class _Row:
    holder: str
    token: int
    reason: str
    expires_at: datetime


class LocalClaimStore:
    """Claims held in this process: one replica's, which is ``zamboni serve``'s.

    Within one runtime the queue already keeps a table from running twice, so
    every acquire here succeeds; what this store adds is that the runtime's
    claim path is the same one a shared store takes. Several runtimes in one
    process sharing one instance exclude each other. ``clock`` is the store's
    clock (§6 rule 2); a test injects one.
    """

    def __init__(self, *, clock: Callable[[], datetime] = _now) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._rows: dict[tuple[str, str], _Row] = {}
        #: The sequence: never reset, so a token never repeats after a release.
        self._token = 0

    def now(self) -> datetime:
        """The store's clock -- what expiry is decided against."""
        return self._clock()

    def acquire(
        self, warehouse: str, table: str, *, holder: str, lease: timedelta, reason: str
    ) -> Claim | None:
        _check_lease(lease)
        with self._lock:
            now = self._clock()
            row = self._rows.get((warehouse, table))
            if row is not None and row.expires_at > now:
                return None
            self._token += 1
            row = _Row(holder, self._token, reason, now + lease)
            self._rows[(warehouse, table)] = row
            return Claim(warehouse, table, holder, row.token, row.expires_at)

    def renew(self, claim: Claim, *, lease: timedelta) -> Claim | None:
        _check_lease(lease)
        with self._lock:
            now = self._clock()
            row = self._own(claim)
            if row is None or row.expires_at <= now:
                return None
            row = dataclasses.replace(row, expires_at=now + lease)
            self._rows[(claim.warehouse, claim.table)] = row
            return dataclasses.replace(claim, expires_at=row.expires_at)

    def release(self, claim: Claim) -> None:
        with self._lock:
            if self._own(claim) is not None:
                del self._rows[(claim.warehouse, claim.table)]

    def _own(self, claim: Claim) -> _Row | None:
        row = self._rows.get((claim.warehouse, claim.table))
        if row is None or (row.holder, row.token) != (claim.holder, claim.token):
            return None
        return row


def _check_lease(lease: timedelta) -> None:
    if lease <= timedelta(0):
        raise ValueError(f"a claim's lease must be positive, not {lease}")


# -- holding a claim while a table runs ------------------------------------------

#: The share of a lease by which a renewal must have succeeded, or the table's
#: worker is killed (§6): with a third of the lease still to run, the kill lands
#: before another replica can acquire the table.
KILL_AT_LEASE_FRACTION = 2 / 3


async def _off_loop(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """``fn(*args, **kwargs)`` on a daemon thread of its own, awaited on the loop.

    Not ``asyncio.to_thread``: ``asyncio.run`` waits for the default executor
    to drain when it ends -- up to five minutes -- so a store call that hangs
    would hold up the runtime's stop long after the kill it was racing.
    Abandoned, a daemon thread holds up nothing, not even the process's exit.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[T] = loop.create_future()

    def settle(outcome: Callable[[], None]) -> None:
        if not future.done():  # a caller that stopped waiting cancelled it
            outcome()

    def call() -> None:
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            settled = functools.partial(settle, functools.partial(future.set_exception, exc))
        else:
            settled = functools.partial(settle, functools.partial(future.set_result, result))
        # The loop may have closed while the call hung; nobody is waiting then.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(settled)

    threading.Thread(target=call, name="zamboni-claim-store", daemon=True).start()
    return await future


@dataclass
class Holding:
    """One table's claim, as the runtime holds it while the table runs."""

    claim: Claim
    #: ``monotonic`` just before the acquire: the lease runs from no earlier.
    since: float
    #: Why the table's worker was killed, once it has been.
    lost: str | None = None


class Claimer:
    """Acquires, renews and releases one runtime's claims. Used on the runtime's loop.

    Every store call is made on a worker thread, because a store may block on
    the network; the deadline that kills a table never waits for one.
    """

    def __init__(
        self,
        store: ClaimStore,
        *,
        holder: str,
        lease: timedelta,
        renew_every: timedelta,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        _check_lease(lease)
        if not timedelta(0) < renew_every < lease * KILL_AT_LEASE_FRACTION:
            raise ValueError(
                f"claims must be renewed more often than {KILL_AT_LEASE_FRACTION:.0%} of the "
                f"lease ({lease}), or every table is killed before its first renewal; "
                f"got every {renew_every}"
            )
        self.store = store
        self.holder = holder
        self.lease = lease
        self.renew_every = renew_every
        self._monotonic = monotonic

    async def acquire(self, warehouse: str, table: str, *, reason: str) -> Holding | None:
        """The table's claim, or ``None`` if another holder has it.

        A store error propagates: the table must not start unclaimed.
        """
        since = self._monotonic()
        claim = await _off_loop(
            self.store.acquire,
            warehouse,
            table,
            holder=self.holder,
            lease=self.lease,
            reason=reason,
        )
        return None if claim is None else Holding(claim, since)

    @contextlib.asynccontextmanager
    async def kept(self, holding: Holding, *, kill: Callable[[], None]) -> AsyncIterator[Holding]:
        """Renew ``holding`` for the life of the block; release it however the block ends.

        A renewal answered ``None`` means the claim is gone -- expired, and
        maybe already another replica's -- so ``kill`` is called at once. A
        renewal that fails or hangs is retried until
        :data:`KILL_AT_LEASE_FRACTION` of the lease has passed since the last
        success, and then ``kill`` is called. Either way ``holding.lost`` says why.
        """
        renewing = asyncio.create_task(self._renew(holding, kill))
        try:
            yield holding
        finally:
            renewing.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewing
            try:
                await _off_loop(self.store.release, holding.claim)
            except Exception:
                # It expires on its own; nothing is unsafe, only late.
                claim = holding.claim
                logger.exception(
                    "could not release the claim on %s/%s", claim.warehouse, claim.table
                )

    async def _renew(self, holding: Holding, kill: Callable[[], None]) -> None:
        window = (self.lease * KILL_AT_LEASE_FRACTION).total_seconds()
        deadline = holding.since + window
        # Retrying sooner than the cadence leaves room for more than one
        # attempt before the deadline. A quarter of it: chosen, not measured.
        retry = self.renew_every.total_seconds() / 4
        wait = self.renew_every.total_seconds()
        claim = holding.claim
        while True:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                self._kill(holding, kill, "not renewed in time")
                return
            await asyncio.sleep(min(wait, remaining))
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                continue
            asked = self._monotonic()
            try:
                renewed = await asyncio.wait_for(
                    _off_loop(self.store.renew, holding.claim, lease=self.lease), remaining
                )
            except TimeoutError:
                continue
            except Exception:
                logger.exception("renewing the claim on %s/%s", claim.warehouse, claim.table)
                wait = retry
                continue
            if renewed is None:
                self._kill(holding, kill, "the claim is gone")
                return
            holding.claim = renewed
            # From when it was asked: the store's lease runs from no earlier.
            deadline = asked + window
            wait = self.renew_every.total_seconds()

    def _kill(self, holding: Holding, kill: Callable[[], None], why: str) -> None:
        claim = holding.claim
        holding.lost = why
        logger.error(
            "lost the claim on %s/%s (%s): killing its worker before another replica takes it",
            claim.warehouse,
            claim.table,
            why,
        )
        try:
            kill()
        except Exception:
            logger.exception("killing the worker for %s/%s", claim.warehouse, claim.table)
