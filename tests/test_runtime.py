# SPDX-License-Identifier: Apache-2.0
"""`MaintenanceRuntime`: the control surface, from other threads (#162).

The runtime's own loops are the ones `zamboni serve` has always run, and
test_service.py, test_scheduler.py and test_pool.py still drive them through
`Service`. What is new is that an application's threads reach a running
runtime, so most tests here start one with `start_in_thread` and call it from
the test's own thread -- a plain thread with no event loop, which is what a
web request handler or a Kafka consumer is.

The clock is pinned to an hour after every warehouse's 02:00 firing, so the
schedule never fires and every table that runs was offered by the test.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse
from zamboni.pool import ItemResult, PoolSize, WorkerConfig
from zamboni.runtime import (
    CREATED,
    RUNNING,
    STOPPED,
    MaintenanceRuntime,
    OfferReceipt,
    RuntimeHandle,
    StaticFleet,
    _Hooks,
    start_in_thread,
)
from zamboni.scheduler import (
    EVENT,
    MANUAL,
    RECLAIM,
    SCHEDULE,
    UnknownReasonError,
    WorkItem,
    operations_of,
)
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

#: An hour after the 02:00 firing: the next is tomorrow, so nothing fires.
QUIET = datetime(2026, 10, 2, 3, 0, tzinfo=UTC)


def warehouse(name: str = "acme", tables=("a", "b", "c"), schedule="0 2 * * *"):
    return FleetWarehouse(
        name=name,
        schedule=CronSchedule(schedule, random=False),
        table_config=TableConfig(
            warehouse=name,
            namespaces={"raw": NamespaceSettings(tables={t: TableSettings() for t in tables})},
        ),
    )


def fleet(*warehouses: FleetWarehouse) -> FleetConfig:
    return FleetConfig(warehouses=warehouses or (warehouse(),))


class FakePool:
    """The pool's interface without processes.

    ``release`` is a *threading* event so the test's thread can hold tables in
    flight and let them go; the pool polls it rather than blocking the loop.
    """

    def __init__(self, size: PoolSize, config: WorkerConfig, *, hold: bool = False) -> None:
        self.size = size
        self.release = threading.Event()
        if not hold:
            self.release.set()
        #: ``(table, the thread it ran on)``.
        self.ran: list[tuple[str, str]] = []
        self.items: list[WorkItem] = []
        self.killed = False
        self.closed = False

    async def run(self, item: WorkItem) -> ItemResult:
        self.ran.append((item.table, threading.current_thread().name))
        self.items.append(item)
        while not (self.release.is_set() or self.killed):
            await asyncio.sleep(0.005)
        if self.killed:
            return ItemResult(item, 1, None, "", "worker died (killed by signal 9)", 0.0)
        return ItemResult(item, 0, {"exit_code": 0}, "", None, 0.0)

    def kill(self) -> None:
        self.killed = True

    async def close(self) -> None:
        self.closed = True


def runtime(
    tmp_path,
    *,
    workers: int = 1,
    hold: bool = False,
    pools: list[FakePool] | None = None,
    hooks: _Hooks | None = None,
    clock: Callable[[], datetime] = lambda: QUIET,
    wait=None,
) -> MaintenanceRuntime:
    pools = [] if pools is None else pools

    def factory(size: PoolSize, config: WorkerConfig) -> FakePool:
        pool = FakePool(size, config, hold=hold)
        pools.append(pool)
        return pool

    return MaintenanceRuntime(
        fleet=StaticFleet(fleet()),
        pool=PoolSize(workers, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=clock,
        pool_factory=factory,
        wait=wait,
        _hooks=hooks,
    )


def eventually(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out waiting for the runtime"
        time.sleep(0.005)


def stop_within(handle: RuntimeHandle, grace: timedelta, limit: float = 10.0) -> bool:
    """``handle.stop(grace=...)``, failing rather than hanging if it never returns."""
    returned: list[bool] = []
    caller = threading.Thread(target=lambda: returned.append(handle.stop(grace=grace)))
    caller.start()
    caller.join(limit)
    assert returned, f"RuntimeHandle.stop did not return within {limit}s"
    return returned[0]


# -- the control surface, from a plain thread --------------------------------------


def test_an_offer_from_another_thread_runs_on_the_runtimes_loop(tmp_path):
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools))
    try:
        receipt = handle.runtime.offer("acme", ["raw.a"], reason=SCHEDULE)
        assert receipt == OfferReceipt("acme", SCHEDULE, queued=("raw.a",))
        eventually(lambda: bool(pools[0].ran))
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert pools[0].ran == [("raw.a", "zamboni-runtime")]


def test_concurrent_offers_of_one_table_queue_it_once(tmp_path):
    """Sixteen threads offer the same table at once: one queues, fifteen coalesce.

    The table is held behind another in flight on the only worker, so it stays
    queued for every offer -- the property is the queue's, reached from threads.
    """
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools, hold=True))
    rt = handle.runtime
    try:
        rt.offer("acme", ["raw.a"], reason=SCHEDULE)
        eventually(lambda: rt.state().in_flight == (("acme", "raw.a"),))
        start = threading.Barrier(16)
        receipts: list[OfferReceipt] = []

        def offer() -> None:
            start.wait()
            receipts.append(rt.offer("acme", ["raw.b"], reason=SCHEDULE))

        threads = [threading.Thread(target=offer) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sum(r.queued == ("raw.b",) for r in receipts) == 1
        assert sum(r.coalesced == ("raw.b",) for r in receipts) == 15
        assert rt.state().queued == (("acme", "raw.b"),)
        pools[0].release.set()
    finally:
        assert handle.stop(grace=timedelta(seconds=5))


def test_state_from_another_thread_is_the_loops_view(tmp_path):
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools, hold=True))
    rt = handle.runtime
    try:
        rt.offer("acme", ["raw.a"], reason=SCHEDULE)
        eventually(lambda: bool(pools[0].ran))
        rt.offer("acme", ["raw.c", "raw.b"], reason=SCHEDULE)
        # An event for the table in flight marks it dirty rather than queuing it.
        assert rt.offer("acme", ["raw.a"], reason=EVENT).coalesced == ("raw.a",)

        state = rt.state()
        assert state.phase == RUNNING
        assert state.generation == 1
        assert state.workers == 1
        assert state.last_tick == QUIET
        assert state.in_flight == (("acme", "raw.a"),) and state.busy == 1
        assert state.queued == (("acme", "raw.c"), ("acme", "raw.b")), "in take order"
        assert state.dirty == (("acme", "raw.a"),)
        pools[0].release.set()
    finally:
        assert handle.stop(grace=timedelta(seconds=5))


def test_update_fleet_from_another_thread_is_adopted_on_the_loop(tmp_path):
    changed: list[tuple[int, str]] = []
    hooks = _Hooks(
        fleet_changed=lambda gen, _: changed.append((gen, threading.current_thread().name))
    )
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools, hooks=hooks))
    rt = handle.runtime
    try:
        assert rt.offer("acme", ["raw.d"], reason=SCHEDULE).unknown == ("raw.d",)
        assert rt.offer("zenith", reason=SCHEDULE).refused is not None

        generation = rt.update_fleet(fleet(warehouse(tables=("a", "d")), warehouse("zenith")))

        assert generation == 2 == rt.state().generation
        assert changed == [(2, "zamboni-runtime")]
        assert rt.offer("acme", ["raw.d"], reason=SCHEDULE).queued == ("raw.d",)
        assert rt.offer("zenith", ["raw.a"], reason=SCHEDULE).queued == ("raw.a",)
        eventually(lambda: len(pools[0].ran) == 2)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))


def test_update_fleet_rearms_the_schedule(tmp_path):
    """A schedule changed by `update_fleet` fires on the new schedule, from the change.

    The clock stands at 03:00, after the 02:00 firing, and the scheduler polls
    it rather than sleeping. Moved to every minute, the warehouse is re-armed
    from 03:00 and fires when the clock reaches 03:01 -- not tomorrow at 02:00.
    """
    now = [QUIET]

    async def poll(stop: asyncio.Event, seconds: float) -> None:
        await asyncio.sleep(0.005)

    pools: list[FakePool] = []
    handle = start_in_thread(
        runtime(tmp_path, pools=pools, workers=3, clock=lambda: now[0], wait=poll)
    )
    try:
        rt = handle.runtime
        rt.update_fleet(fleet(warehouse(schedule="* * * * *")))
        time.sleep(0.05)
        assert pools[0].ran == [], "re-armed from 03:00: nothing is due until 03:01"
        now[0] = QUIET + timedelta(minutes=1)
        eventually(lambda: len(pools[0].ran) == 3)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert sorted(table for table, _ in pools[0].ran) == ["raw.a", "raw.b", "raw.c"]


# -- before the loop starts, and after it leaves ---------------------------------


def test_the_control_surface_works_before_run_and_after_it(tmp_path):
    pools: list[FakePool] = []
    rt = runtime(tmp_path, pools=pools)
    assert rt.state().phase == CREATED
    assert rt.offer("acme", ["raw.b"], reason=SCHEDULE).queued == ("raw.b",)
    assert rt.update_fleet(fleet()) == 2

    handle = start_in_thread(rt)
    eventually(lambda: rt.state().busy == 0 and rt.state().queued == ())
    assert handle.stop(grace=timedelta(seconds=5))
    assert [table for table, _ in pools[0].ran] == ["raw.b"], "offered before the loop existed"

    state = rt.state()
    assert (state.phase, state.generation) == (STOPPED, 2)
    assert rt.offer("acme", reason=SCHEDULE).refused == "the runtime is stopping"
    rt.request_stop(force=True)  # and stopping a stopped runtime is harmless


def test_a_runtime_runs_once(tmp_path):
    rt = runtime(tmp_path)
    start_in_thread(rt).stop(grace=timedelta(seconds=5))
    with pytest.raises(RuntimeError, match="runs once"):
        asyncio.run(rt.run())


# -- what an offer did -----------------------------------------------------------


def test_an_offer_of_every_table_names_each_one(tmp_path):
    rt = runtime(tmp_path)
    rt.offer("acme", ["raw.b"], reason=SCHEDULE)
    receipt = rt.offer("acme", reason=EVENT)
    assert receipt == OfferReceipt("acme", EVENT, queued=("raw.a", "raw.c"), coalesced=("raw.b",))


def test_an_offer_sorts_each_table_and_names_the_unknown(tmp_path):
    rt = runtime(tmp_path)
    receipt = rt.offer("acme", ["raw.c", "raw.nope", "raw.c", "raw.a"], reason=SCHEDULE)
    assert receipt.queued == ("raw.c", "raw.a"), "in the order offered, once each"
    assert receipt.coalesced == ()
    assert receipt.unknown == ("raw.nope",)
    assert receipt.refused is None
    assert rt.state().queued == (("acme", "raw.c"), ("acme", "raw.a"))


def test_an_offer_for_a_warehouse_not_in_the_fleet_is_refused(tmp_path):
    rt = runtime(tmp_path)
    receipt = rt.offer("zenith", ["raw.a"], reason=SCHEDULE)
    assert receipt.refused is not None and "zenith" in receipt.refused
    assert (receipt.queued, receipt.coalesced, receipt.unknown) == ((), (), ())
    assert rt.state().queued == ()


def test_an_unknown_reason_is_refused_in_the_callers_thread(tmp_path):
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools))
    try:
        with pytest.raises(UnknownReasonError):
            handle.runtime.offer("acme", ["raw.a"], reason="because")
        assert handle.runtime.state().queued == ()
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert pools[0].ran == []


@pytest.mark.parametrize("reason", [MANUAL, RECLAIM])
def test_manual_and_reclaim_are_offered_from_another_thread(tmp_path, reason):
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools))
    try:
        assert handle.runtime.offer("acme", ["raw.a"], reason=reason).queued == ("raw.a",)
        eventually(lambda: bool(pools[0].items))
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    (item,) = pools[0].items
    assert item.reasons == {reason}
    assert item.operations == operations_of([reason])


def test_offers_merged_before_a_worker_takes_them_run_every_operation(tmp_path):
    pools: list[FakePool] = []
    rt = runtime(tmp_path, pools=pools)
    rt.offer("acme", ["raw.a"], reason=EVENT)
    assert rt.offer("acme", ["raw.a"], reason=RECLAIM).coalesced == ("raw.a",)
    handle = start_in_thread(rt)
    try:
        eventually(lambda: bool(pools[0].items))
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    (item,) = pools[0].items
    assert (item.reason, item.also) == (RECLAIM, frozenset({EVENT}))
    assert item.operations == operations_of([EVENT, RECLAIM])


def test_a_deadline_offered_from_another_thread_is_honoured(tmp_path):
    """The runtime's clock stands at QUIET: one table's deadline has passed, the
    other's has not."""
    finished: list[ItemResult] = []
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools, hooks=_Hooks(finished=finished.append)))
    rt = handle.runtime
    try:
        rt.offer("acme", ["raw.a"], reason=SCHEDULE, not_after=QUIET - timedelta(minutes=1))
        rt.offer("acme", ["raw.b"], reason=SCHEDULE, not_after=QUIET + timedelta(minutes=1))
        eventually(lambda: len(finished) == 2)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert [table for table, _ in pools[0].ran] == ["raw.b"]
    assert {r.item.table: r.dropped for r in finished} == {"raw.a": "deadline", "raw.b": None}
    assert all(r.exit_code == 0 for r in finished)


def test_a_naive_deadline_is_refused_in_the_callers_thread(tmp_path):
    pools: list[FakePool] = []
    handle = start_in_thread(runtime(tmp_path, pools=pools))
    try:
        with pytest.raises(ValueError, match="time zone"):
            handle.runtime.offer(
                "acme", ["raw.a"], reason=SCHEDULE, not_after=datetime(2026, 10, 2, 4, 0)
            )
        assert handle.runtime.state().queued == ()
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert pools[0].ran == []


# -- stopping a runtime on its own thread ------------------------------------------


def test_stop_within_the_grace_lets_tables_in_flight_finish(tmp_path):
    finished: list[ItemResult] = []
    pools: list[FakePool] = []
    handle = start_in_thread(
        runtime(tmp_path, pools=pools, hold=True, hooks=_Hooks(finished=finished.append))
    )
    handle.runtime.offer("acme", ["raw.a"], reason=SCHEDULE)
    eventually(lambda: bool(pools[0].ran))
    threading.Timer(0.1, pools[0].release.set).start()

    assert handle.stop(grace=timedelta(seconds=5)) is True

    (pool,) = pools
    assert not pool.killed and pool.closed
    (result,) = finished
    assert (result.item.table, result.exit_code, result.died) == ("raw.a", 0, None)
    assert handle.runtime.state().phase == STOPPED
    assert not handle._thread.is_alive()


def test_stop_past_the_grace_kills_tables_in_flight(tmp_path):
    finished: list[ItemResult] = []
    pools: list[FakePool] = []
    handle = start_in_thread(
        runtime(tmp_path, pools=pools, hold=True, hooks=_Hooks(finished=finished.append))
    )
    handle.runtime.offer("acme", ["raw.a", "raw.b"], reason=SCHEDULE)
    eventually(lambda: bool(pools[0].ran))

    began = time.monotonic()
    assert stop_within(handle, timedelta(seconds=0.2)) is False
    assert time.monotonic() - began >= 0.2, "the grace was given before killing"

    (pool,) = pools
    assert pool.killed and pool.closed
    (result,) = finished
    assert result.item.table == "raw.a" and result.died is not None
    assert [table for table, _ in pool.ran] == ["raw.a"], "nothing new was taken after stop"
    assert handle.runtime.state().phase == STOPPED
    assert not handle._thread.is_alive()


def test_start_in_thread_raises_if_the_runtime_fails_while_starting(tmp_path, caplog):
    def broken(size: PoolSize, config: WorkerConfig) -> FakePool:
        raise OSError("no workdir")

    rt = MaintenanceRuntime(
        fleet=StaticFleet(fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: QUIET,
        pool_factory=broken,
    )
    with (
        caplog.at_level(logging.ERROR, logger="zamboni.runtime"),
        pytest.raises(RuntimeError, match="exited while starting"),
    ):
        start_in_thread(rt)
    assert "no workdir" in caplog.text
    assert rt.state().phase == STOPPED
