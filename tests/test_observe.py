# SPDX-License-Identifier: Apache-2.0
"""Observing a runtime, and what `state()` says (#166, docs/maintenance-runtime.md §8).

Observers are called on the runtime's loop. The two properties worth a test are
the two the loop depends on: an observer that raises changes nothing, and one
that is slow delays nothing once it is handed off. The slow cases block on an
event rather than sleeping, so "nothing ran" is a fact about a blocked loop,
not a race against a timer.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from datetime import datetime, timedelta

import pytest

from zamboni.fleet import FleetConfig
from zamboni.observe import CLOSE_TIMEOUT_SECONDS, EVENTS, Observer, Observers, handed_off
from zamboni.pool import PoolSize, WorkerConfig
from zamboni.runtime import (
    RUNNING,
    STOPPED,
    STOPPING,
    MaintenanceRuntime,
    StaticFleet,
    start_in_thread,
)
from zamboni.scheduler import SCHEDULE

from .test_runtime import QUIET, FakePool, eventually, fleet, warehouse


class Recorder(Observer):
    """Every call, with the thread it arrived on."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.threads: set[str] = set()

    def __getattribute__(self, name: str):
        if name in EVENTS:

            def record(*args) -> None:
                self.calls.append((name, args))
                self.threads.add(threading.current_thread().name)

            return record
        return super().__getattribute__(name)

    def events(self) -> list[str]:
        return [name for name, _ in self.calls]


class Raising(Observer):
    def __getattribute__(self, name: str):
        if name in EVENTS:

            def explode(*_) -> None:
                raise RuntimeError(f"observer broke on {name}")

            return explode
        return super().__getattribute__(name)


class Gated(Recorder):
    """Blocks on each call of ``only`` (every event by default) until ``gate`` is set."""

    def __init__(self, only=EVENTS) -> None:
        super().__init__()
        self.gate = threading.Event()
        self.only = frozenset(only)

    def __getattribute__(self, name: str):
        if name in EVENTS:
            record = super().__getattribute__(name)
            gate = super().__getattribute__("gate")
            blocks = name in super().__getattribute__("only")

            def blocked(*args) -> None:
                if blocks:
                    gate.wait(10)
                record(*args)

            return blocked
        return super().__getattribute__(name)


class PacedPool(FakePool):
    """Takes 5 ms a table, waiting as a real worker's pipe read does, so a
    reader on another thread sees the queue part-way through."""

    async def run(self, item):
        await asyncio.sleep(0.005)
        return await super().run(item)


def runtime(
    tmp_path,
    *,
    observers=(),
    pools: list[FakePool] | None = None,
    provider=None,
    workers: int = 1,
    pool_class: type[FakePool] = FakePool,
    clock: Callable[[], datetime] = lambda: QUIET,
    wait=None,
) -> MaintenanceRuntime:
    pools = [] if pools is None else pools

    def factory(size: PoolSize, config: WorkerConfig) -> FakePool:
        pool = pool_class(size, config)
        pools.append(pool)
        return pool

    return MaintenanceRuntime(
        fleet=provider or StaticFleet(fleet()),
        pool=PoolSize(workers, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        observers=observers,
        clock=clock,
        pool_factory=factory,
        wait=wait,
    )


# -- what an observer is told -----------------------------------------------------


def test_an_observer_hears_a_tables_life_in_order_on_the_runtimes_thread(tmp_path):
    seen = Recorder()
    handle = start_in_thread(runtime(tmp_path, observers=[seen]))
    receipt = handle.runtime.offer("acme", ["raw.a"], reason=SCHEDULE)
    eventually(lambda: "finished" in seen.events())
    assert handle.stop(grace=timedelta(seconds=5))

    lifecycle = [e for e in seen.events() if e != "ticked"]
    assert lifecycle == [
        "phase_changed",
        "offered",
        "started",
        "finished",
        "phase_changed",
        "phase_changed",
    ]
    phases = [args[0] for name, args in seen.calls if name == "phase_changed"]
    assert phases == [RUNNING, STOPPING, STOPPED]
    assert dict(seen.calls)["offered"] == (receipt,)
    (result,) = dict(seen.calls)["finished"]
    assert (result.item.table, result.exit_code) == ("raw.a", 0)
    assert "ticked" in seen.events(), "the liveness signal reaches observers"
    assert seen.threads == {"zamboni-runtime"}


def test_an_observer_may_define_only_what_it_wants(tmp_path):
    """The runtime calls whichever methods an object has; no base class needed."""

    class OnlyFinished:
        def __init__(self) -> None:
            self.tables: list[str] = []

        def finished(self, result) -> None:
            self.tables.append(result.item.table)

    seen = OnlyFinished()
    handle = start_in_thread(runtime(tmp_path, observers=[seen]))
    handle.runtime.offer("acme", ["raw.a", "raw.b"], reason=SCHEDULE)
    eventually(lambda: len(seen.tables) == 2)
    assert handle.stop(grace=timedelta(seconds=5))


def test_a_raising_observer_changes_nothing(tmp_path, caplog):
    """Every method raises. The tables still run, the next observer still
    hears every event, and the runtime stops cleanly."""
    seen = Recorder()
    pools: list[FakePool] = []
    with caplog.at_level(logging.ERROR, logger="zamboni.observe"):
        handle = start_in_thread(runtime(tmp_path, observers=[Raising(), seen], pools=pools))
        handle.runtime.offer("acme", ["raw.a", "raw.b"], reason=SCHEDULE)
        eventually(lambda: seen.events().count("finished") == 2)
        assert handle.stop(grace=timedelta(seconds=5))
    assert [table for table, _ in pools[0].ran] == ["raw.a", "raw.b"]
    assert seen.events()[-1] == "phase_changed"
    assert "observer broke on finished" in caplog.text


# -- a slow observer ---------------------------------------------------------------


def firing_runtime(tmp_path, observer, pools):
    """A runtime whose clock stands at 03:00 under an every-minute schedule.

    The scheduler polls the clock rather than sleeping, so moving it to 03:01
    fires the warehouse at the next step.
    """
    now = [QUIET]

    async def poll(stop: asyncio.Event, seconds: float) -> None:
        await asyncio.sleep(0.005)

    rt = runtime(
        tmp_path,
        observers=[observer],
        pools=pools,
        workers=3,
        provider=StaticFleet(fleet(warehouse(schedule="* * * * *"))),
        clock=lambda: now[0],
        wait=poll,
    )
    return rt, now


def test_a_handed_off_observer_does_not_delay_a_firing(tmp_path):
    """The observer is blocked from its first call. The firing still happens
    and every table runs; the observer then hears all of it, in order."""
    slow = Gated()
    pools: list[FakePool] = []
    rt, now = firing_runtime(tmp_path, handed_off(slow), pools)
    handle = start_in_thread(rt)
    now[0] = QUIET + timedelta(minutes=1)
    eventually(lambda: len(pools[0].ran) == 3)
    assert slow.calls == [], "the observer has not been able to hear anything yet"

    slow.gate.set()
    assert handle.stop(grace=timedelta(seconds=5))
    lifecycle = [e for e in slow.events() if e != "ticked"]
    assert lifecycle[0] == "phase_changed" and lifecycle[1] == "fired"
    assert lifecycle.count("finished") == 3
    assert lifecycle[-1] == "phase_changed", "delivered in full before the runtime returned"
    assert slow.threads == {"observer-Gated"}


def test_a_slow_observer_not_handed_off_does_delay_a_firing(tmp_path):
    """The negative case, so the test above can fail: called inline, the same
    observer holds the loop, and no table starts until it returns. It blocks on
    `fired` alone, or it would hold `start_in_thread` too."""
    slow = Gated(only={"fired"})
    pools: list[FakePool] = []
    rt, now = firing_runtime(tmp_path, slow, pools)
    handle = start_in_thread(rt)
    now[0] = QUIET + timedelta(minutes=1)
    threading.Event().wait(0.3)
    assert pools[0].ran == [], "the loop is held by the observer"
    slow.gate.set()
    eventually(lambda: len(pools[0].ran) == 3)
    assert handle.stop(grace=timedelta(seconds=5))


def test_handed_off_delivers_in_order_past_a_failing_call():
    class Flaky:
        def __init__(self) -> None:
            self.got: list[datetime] = []

        def ticked(self, at: datetime) -> None:
            if at == QUIET + timedelta(minutes=1):
                raise RuntimeError("one bad call")
            self.got.append(at)

    flaky = Flaky()
    wrapped = handed_off(flaky)
    for minute in range(4):
        wrapped.ticked(QUIET + timedelta(minutes=minute))
    assert wrapped.close(5)
    assert flaky.got == [QUIET + timedelta(minutes=m) for m in (0, 2, 3)]
    assert not hasattr(wrapped, "finished"), "it offers only what the observer defines"


def test_only_a_handed_off_observer_is_closed_by_the_runtime():
    """An observer's own `close` may mean anything -- a file, a connection the
    application still uses -- so the runtime leaves it alone."""

    class Closable:
        closed = False

        def close(self) -> None:
            self.closed = True

    own = Closable()
    wrapped = handed_off(Recorder())
    Observers([own, wrapped]).close()
    assert not own.closed
    assert not wrapped._thread.is_alive()
    assert CLOSE_TIMEOUT_SECONDS > 0


# -- state() --------------------------------------------------------------------


def test_state_never_shows_a_table_both_queued_and_in_flight(tmp_path):
    """Read from another thread, as fast as it answers, while forty tables pass
    through two workers (~100 ms)."""
    tables = [f"t{i:02}" for i in range(40)]
    current = fleet(warehouse(tables=tables))
    pools: list[FakePool] = []
    seen = Recorder()
    handle = start_in_thread(
        runtime(
            tmp_path,
            observers=[seen],
            pools=pools,
            workers=2,
            provider=StaticFleet(current),
            pool_class=PacedPool,
        )
    )
    rt = handle.runtime
    rt.offer("acme", reason=SCHEDULE)
    snapshots = []
    while seen.events().count("finished") < len(tables):
        snapshots.append(rt.state())
    assert handle.stop(grace=timedelta(seconds=5))

    midway = [s for s in snapshots if s.queued and s.in_flight]
    assert len(midway) >= 5, "enough reads taken while tables were both waiting and running"
    for state in snapshots:
        assert not set(state.queued) & set(state.in_flight)
        assert state.busy <= 2
        assert len(state.queued) + state.busy <= len(tables)


def test_state_carries_each_warehouses_record(tmp_path):
    """What `serve`'s state file shows per warehouse is the runtime's now."""
    pools: list[FakePool] = []
    rt, now = firing_runtime(tmp_path, Observer(), pools)
    handle = start_in_thread(rt)
    now[0] = QUIET + timedelta(minutes=1)
    eventually(lambda: (s := rt.state().warehouses.get("acme")) is not None and s.last_sweep)
    assert handle.stop(grace=timedelta(seconds=5))

    acme = rt.state().warehouses["acme"]
    assert acme.last_fired == QUIET + timedelta(minutes=1)
    assert acme.last_sweep is not None
    assert (acme.last_sweep.tables, acme.last_sweep.failures) == (3, 0)
    assert acme.last_exit_code == 0
    assert rt.state().last_error is None


class Refusing:
    """A provider that refuses one bad candidate, three times, then adopts one."""

    def __init__(self, current: FleetConfig, after: FleetConfig) -> None:
        self.fleet = current
        self.after = after
        self.go = threading.Event()

    async def run(self, adopt, refused, stop) -> None:
        for _ in range(3):
            refused("fleet.yaml: bad cron")
        while not self.go.is_set():
            await asyncio.sleep(0.005)
        adopt(self.after)


def test_a_refused_fleet_is_told_once_and_cleared_by_the_next_adopted(tmp_path):
    seen = Recorder()
    provider = Refusing(fleet(), fleet(warehouse(tables=("a",))))
    handle = start_in_thread(runtime(tmp_path, observers=[seen], provider=provider))
    rt = handle.runtime
    eventually(lambda: rt.state().reload_error is not None)
    assert rt.state().reload_error == "fleet.yaml: bad cron"
    assert [args for name, args in seen.calls if name == "refused"] == [
        ("fleet", "fleet.yaml: bad cron")
    ], "a provider polling a bad file refuses it every poll; observers hear it once"

    provider.go.set()
    eventually(lambda: rt.state().generation == 2)
    assert handle.stop(grace=timedelta(seconds=5))
    assert rt.state().reload_error is None
    assert [args[0] for name, args in seen.calls if name == "fleet_changed"] == [2]


@pytest.mark.parametrize("event", EVENTS)
def test_every_event_is_a_method_of_observer(event):
    """`handed_off` and the runtime both go by EVENTS; the base class must match."""
    assert callable(getattr(Observer, event))
