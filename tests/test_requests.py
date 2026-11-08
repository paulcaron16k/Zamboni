# SPDX-License-Identifier: Apache-2.0
"""Requests from another process (docs/maintenance-runtime.md §9.2, #168).

The feed's decisions are tested against a real, unstarted runtime's ``offer``,
so an outcome is what the runtime actually did with the request. The in-memory
request table passes the same conformance checks the PostgreSQL reference
passes in tests/test_dev_stack.py.
"""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta

import pytest

from tests.test_priorities import wide_fleet
from tests.test_runtime import QUIET, eventually
from zamboni.conformance import (
    REQUEST_SOURCE_CHECKS,
    ConformanceError,
    RequestSourceHarness,
    check_a_nakd_request_is_delivered_again,
    check_a_request_held_by_a_replica_that_stops_goes_to_another,
    check_an_acknowledged_request_is_recorded_and_never_delivered_again,
    check_each_request_reaches_one_replica,
)
from zamboni.pool import ItemResult, PoolSize, WorkerConfig
from zamboni.requests import (
    OFFERED,
    REFUSED,
    REQUEST_REASONS,
    UNMAPPED,
    LocalRequests,
    Request,
    RequestFeed,
    request_outcome,
)
from zamboni.runtime import MaintenanceRuntime, OfferReceipt, StaticFleet, start_in_thread
from zamboni.scheduler import EVENT, HIGH, MANUAL, REASON_CLASS, RECLAIM


def runtime(tmp_path, **kw) -> MaintenanceRuntime:
    return MaintenanceRuntime(
        fleet=StaticFleet(wide_fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: QUIET,
        **kw,
    )


def feed(rt: MaintenanceRuntime, *, stopping: bool = False) -> RequestFeed:
    return RequestFeed(
        LocalRequests().source(),
        offer=lambda w, t, r: rt.offer(w, t, reason=r),
        stopping=lambda: stopping,
    )


def request(tables=("raw.t000",), *, warehouse="acme", reason=MANUAL, id="1") -> Request:
    return Request(id, warehouse, None if tables is None else tuple(tables), reason)


# -- what a request may ask for -------------------------------------------------------


def test_a_request_asks_for_the_high_class_reasons_and_no_other():
    """The reasons a person asks for are the ones §3.4 runs first."""
    assert {r for r, cls in REASON_CLASS.items() if cls == HIGH} == set(REQUEST_REASONS)


@pytest.mark.parametrize("reason", [EVENT, "rebuild"])
def test_a_request_for_another_reason_is_refused_not_offered(tmp_path, reason):
    rt = runtime(tmp_path)
    outcome = feed(rt).handle(request(reason=reason))
    assert outcome.startswith(f"{REFUSED}: ") and repr(reason) in outcome
    assert rt.state().queued == ()


def test_a_request_naming_no_table_is_refused_rather_than_read_as_every_table(tmp_path):
    rt = runtime(tmp_path)
    assert feed(rt).handle(request(())).startswith(f"{REFUSED}: ")
    assert rt.state().queued == ()


# -- what became of it ----------------------------------------------------------------


def test_a_request_is_offered_for_its_reason(tmp_path):
    rt = runtime(tmp_path)
    f = feed(rt)
    assert f.handle(request(["raw.t000", "raw.t001"], reason=RECLAIM)) == OFFERED
    assert set(rt.state().queued) == {("acme", "raw.t000"), ("acme", "raw.t001")}
    assert f.counters.offered == 1


def test_a_request_for_every_table_offers_the_warehouse(tmp_path):
    rt = runtime(tmp_path)
    assert feed(rt).handle(request(None)) == OFFERED
    assert len(rt.state().queued) == len(wide_fleet()["acme"].table_config.tables)


def test_a_request_coalescing_into_queued_work_is_offered(tmp_path):
    rt = runtime(tmp_path)
    rt.offer("acme", ["raw.t000"], reason=EVENT)
    assert feed(rt).handle(request()) == OFFERED


def test_a_request_for_an_unmanaged_warehouse_is_unmapped(tmp_path):
    rt = runtime(tmp_path)
    f = feed(rt)
    outcome = f.handle(request(warehouse="globex"))
    assert outcome.startswith(f"{UNMAPPED}: ") and "globex" in outcome
    assert f.counters.unmapped == 1


def test_a_request_for_an_unmanaged_table_is_unmapped(tmp_path):
    rt = runtime(tmp_path)
    assert feed(rt).handle(request(["raw.nope"])) == (
        f"{UNMAPPED}: not in the table config: raw.nope"
    )


def test_a_request_partly_unmapped_says_what_ran_first(tmp_path):
    rt = runtime(tmp_path)
    outcome = feed(rt).handle(request(["raw.t000", "raw.nope"]))
    assert outcome == f"{OFFERED}; {UNMAPPED}: not in the table config: raw.nope"
    assert rt.state().queued == (("acme", "raw.t000"),)


def test_a_quarantined_table_is_refused_and_says_so():
    receipt = OfferReceipt("acme", RECLAIM, queued=("raw.a",), quarantined=("raw.b",))
    assert request_outcome(receipt) == f"{OFFERED}; {REFUSED}: quarantined: raw.b"
    alone = OfferReceipt("acme", RECLAIM, quarantined=("raw.b",))
    assert request_outcome(alone) == f"{REFUSED}: quarantined: raw.b"


def test_a_request_while_the_runtime_stops_is_given_back_not_marked(tmp_path):
    """Not handled, so not acknowledged: the next replica, or the next start, has it."""
    rt = runtime(tmp_path)
    assert feed(rt, stopping=True).handle(request()) is None
    assert rt.state().queued == ()


def test_a_request_delivered_again_is_acknowledged_as_before_not_offered_again(tmp_path):
    """Its first acknowledgement was lost: the outcome it had, without a second offer."""
    rt = runtime(tmp_path)
    offers: list[tuple] = []
    f = RequestFeed(
        LocalRequests().source(),
        offer=lambda w, t, r: offers.append((w, t, r)) or rt.offer(w, t, reason=r),
        stopping=lambda: False,
    )
    first = f.handle(request(["raw.t000", "raw.nope"]))
    assert f.handle(request(["raw.t000", "raw.nope"])) == first
    assert len(offers) == 1
    assert f.counters.duplicates == 1


# -- the feed, reading a source ---------------------------------------------------------


def test_the_feed_acknowledges_each_request_with_its_outcome(tmp_path):
    rt = runtime(tmp_path)
    table = LocalRequests()
    offered = table.submit("acme", ["raw.t000"])
    unmapped = table.submit("globex", None)
    refused = table.submit("acme", ["raw.t001"], reason=EVENT)
    ids = (offered, unmapped, refused)
    f = RequestFeed(
        table.source(poll_every=0.01),
        offer=lambda w, t, r: rt.offer(w, t, reason=r),
        stopping=lambda: False,
    )

    async def scenario():
        stop = asyncio.Event()
        running = asyncio.ensure_future(f.run(stop))
        while any(table.outcome(i) is None for i in ids):
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(running, 10)

    asyncio.run(scenario())
    assert table.outcome(offered) == OFFERED
    assert table.outcome(unmapped) == f"{UNMAPPED}: warehouse 'globex' is not in the fleet"
    assert (table.outcome(refused) or "").startswith(f"{REFUSED}: ")
    assert f.counters.as_dict() == {
        "received": 3,
        "offered": 1,
        "unmapped": 1,
        "refused": 1,
        "duplicates": 0,
        "errors": 0,
    }


def test_a_request_the_feed_failed_on_is_given_back(tmp_path):
    """An internal error is not the request's fault: nak'd, for another try."""
    table = LocalRequests()
    wanted = table.submit("acme", ["raw.t000"])
    tries: list[str] = []

    def offer(w, t, r):
        tries.append(w)
        raise RuntimeError("the queue is having a day")

    f = RequestFeed(
        table.source(poll_every=0.01), offer=offer, stopping=lambda: False, retry_after=0.05
    )

    async def scenario():
        stop = asyncio.Event()
        running = asyncio.ensure_future(f.run(stop))
        while len(tries) < 2:
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(running, 10)

    started = time.monotonic()
    asyncio.run(scenario())
    assert table.outcome(wanted) is None
    assert f.counters.errors == len(tries) >= 2
    assert time.monotonic() - started >= f.retry_after, "retried after a wait, not at once"


def test_a_feed_stopping_with_a_request_in_hand_settles_it_first(tmp_path):
    """Stop arrives while the request is being offered: it is acknowledged,
    not cut off mid-acknowledgement and delivered again."""
    table = LocalRequests()
    wanted = table.submit("acme", ["raw.t000"])
    stop = asyncio.Event()
    rt = runtime(tmp_path)

    def offer(w, t, r):
        stop.set()  # arrives mid-handling
        return rt.offer(w, t, reason=r)

    f = RequestFeed(table.source(poll_every=0.01), offer=offer, stopping=lambda: False)

    async def scenario():
        await asyncio.wait_for(f.run(stop), 10)

    asyncio.run(scenario())
    assert table.outcome(wanted) == OFFERED


class SlowAcks:
    """A source whose acknowledgement takes a while, as a database's does."""

    connected = True

    def __init__(self, source, acking: asyncio.Event) -> None:
        self.name = source.name
        self._source, self._acking = source, acking

    async def __aiter__(self):
        async for message in self._source:
            yield _SlowAck(message, self._acking)


class _SlowAck:
    def __init__(self, message, acking: asyncio.Event) -> None:
        self.request, self._message, self._acking = message.request, message, acking

    async def ack(self, outcome: str) -> None:
        self._acking.set()
        await asyncio.sleep(0.2)
        await self._message.ack(outcome)

    async def nak(self) -> None:
        await self._message.nak()


def test_a_feed_stopped_mid_acknowledgement_finishes_it(tmp_path):
    """Cut off there, the source would deliver again a request this runtime
    already offered."""
    table = LocalRequests()
    wanted = table.submit("acme", ["raw.t000"])
    rt = runtime(tmp_path)

    async def scenario():
        stop, acking = asyncio.Event(), asyncio.Event()
        f = RequestFeed(
            SlowAcks(table.source(poll_every=0.01), acking),
            offer=lambda w, t, r: rt.offer(w, t, reason=r),
            stopping=lambda: False,
        )
        running = asyncio.ensure_future(f.run(stop))
        await asyncio.wait_for(acking.wait(), 10)
        stop.set()
        await asyncio.wait_for(running, 10)

    asyncio.run(scenario())
    assert table.outcome(wanted) == OFFERED


# -- the in-memory reference passes the conformance checks ---------------------------


@pytest.mark.parametrize("check", REQUEST_SOURCE_CHECKS, ids=lambda c: c.__name__)
def test_the_in_memory_request_table_passes(check):
    check(RequestSourceHarness(make_table=LocalRequests, timeout=10))


class Redelivers(LocalRequests):
    """Broken: an acknowledgement is not recorded, so the request comes again."""

    def _settle(self, row, holder, outcome):
        super()._settle(row, holder, None)


class Loses(LocalRequests):
    """Broken: a request given back is marked handled, so it is lost."""

    def _settle(self, row, holder, outcome):
        super()._settle(row, holder, outcome or "lost")


class Shares(LocalRequests):
    """Broken: every reader is handed every request, held or not."""

    def _take(self, holder):
        with self._lock:
            for row in self._rows.values():
                if row.outcome is None and holder not in getattr(row, "seen", set()):
                    row.seen = getattr(row, "seen", set()) | {holder}
                    row.holder = holder
                    return row
        return None


@pytest.mark.parametrize(
    ("broken", "check", "rule"),
    [
        (Redelivers, check_an_acknowledged_request_is_recorded_and_never_delivered_again, "ack"),
        (Loses, check_a_nakd_request_is_delivered_again, "nak"),
        (Loses, check_a_request_held_by_a_replica_that_stops_goes_to_another, "delivery"),
        (Shares, check_each_request_reaches_one_replica, "one replica per request"),
    ],
    ids=lambda x: getattr(x, "__name__", x),
)
def test_each_check_rejects_a_table_broken_its_way(broken, check, rule):
    with pytest.raises(ConformanceError, match=rule):
        check(RequestSourceHarness(make_table=broken, timeout=2))


# -- in the runtime ----------------------------------------------------------------------


class InstantPool:
    def __init__(self, size, config) -> None:
        self.ran: list[tuple[str, str]] = []

    async def run(self, item):
        self.ran.append((item.table, item.reason))
        return ItemResult(item, 0, {"exit_code": 0}, "", None, 0.0)

    def kill(self) -> None:
        pass

    async def close(self) -> None:
        pass


def test_a_runtime_runs_what_a_request_asks_for_and_reports_its_source(tmp_path):
    table = LocalRequests()
    pools: list[InstantPool] = []
    rt = runtime(
        tmp_path,
        requests=[table.source("app requests", poll_every=0.01)],
        pool_factory=lambda size, config: pools.append(InstantPool(size, config)) or pools[-1],
    )
    handle = start_in_thread(rt)
    try:
        wanted = table.submit("acme", ["raw.t007"], reason=RECLAIM, requested_by="someone")
        eventually(lambda: table.outcome(wanted) is not None)
        eventually(lambda: ("raw.t007", RECLAIM) in pools[0].ran)
        state = rt.state()
    finally:
        assert handle.stop(grace=timedelta(seconds=10))
    assert table.outcome(wanted) == OFFERED
    assert state.events["app requests"]["offered"] == 1
    assert state.sources_connected is True
