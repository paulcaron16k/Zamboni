# SPDX-License-Identifier: Apache-2.0
"""A runtime loop that raises stops the runtime (#170).

`MaintenanceRuntime.run` used to gather its loops, so when one raised the rest
kept running: a scheduler firing into a queue nothing took from, with the pool
closed under it. Each test makes one loop raise and asserts that ``run``
re-raises it, says which loop failed, and stops as on any stop.
"""

from __future__ import annotations

import asyncio
import itertools
import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest

from tests.test_runtime import QUIET, FakePool, eventually, fleet, warehouse
from zamboni.fleet import FleetConfig
from zamboni.pool import PoolSize, WorkerConfig
from zamboni.pressure import PressureSettings
from zamboni.runtime import RUNNING, STOPPED, MaintenanceRuntime, StaticFleet
from zamboni.scheduler import MANUAL, PressureCheck


class Boom(Exception):
    pass


class BrokenProvider(StaticFleet):
    async def run(self, adopt, refused, stop) -> None:
        raise Boom("the fleet provider")


def take_fails(fleet, *, high_only=False):
    """The workers loop's own step, outside any one table's handling."""
    raise Boom("the workers")


class PoolThatCannotCheck(FakePool):
    async def run(self, item):
        if isinstance(item, PressureCheck):
            raise Boom("the pressure checks")
        return await super().run(item)


class BrokenFeed:
    source = SimpleNamespace(name="broken")
    counters = SimpleNamespace(as_dict=dict)

    async def run(self, stop) -> None:
        raise Boom("an event feed")


async def scheduler_fails(stop: asyncio.Event, seconds: float) -> None:
    raise Boom("the scheduler")


def build(tmp_path, loop: str) -> tuple[MaintenanceRuntime, list]:
    """A runtime whose ``loop`` raises ``Boom`` as soon as it runs."""
    pools: list = []
    provider = StaticFleet(fleet())
    clock = lambda: QUIET  # noqa: E731
    wait = None
    make_pool = FakePool

    if loop == "scheduler":
        wait = scheduler_fails
    elif loop == "fleet provider":
        provider = BrokenProvider(fleet())
    elif loop == "pressure checks":
        checked = warehouse()
        provider = StaticFleet(
            FleetConfig(
                warehouses=(
                    type(checked)(
                        checked.name,
                        checked.schedule,
                        checked.table_config,
                        pressure=PressureSettings(check_every_minutes=1),
                    ),
                )
            )
        )
        # A minute on with every call, so the first check falls due on the
        # loop's own first pass; the next schedule firing is a day away.
        minutes = itertools.count()
        clock = lambda: QUIET + timedelta(minutes=next(minutes))  # noqa: E731
        make_pool = PoolThatCannotCheck

    def factory(size, config):
        pools.append(make_pool(size, config))
        return pools[-1]

    rt = MaintenanceRuntime(
        fleet=provider,
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=clock,
        pool_factory=factory,
        wait=wait,
    )
    if loop == "event feed broken":
        rt.feeds = [BrokenFeed()]
    if loop == "workers":
        rt._queue.take = take_fails
    return rt, pools


LOOPS = ["scheduler", "workers", "fleet provider", "pressure checks", "event feed broken"]


@pytest.mark.parametrize("loop", LOOPS)
def test_a_loop_that_raises_stops_the_runtime_and_is_raised(tmp_path, loop):
    rt, pools = build(tmp_path, loop)

    with pytest.raises(Boom):
        asyncio.run(asyncio.wait_for(rt.run(), timeout=30))

    state = rt.state()
    assert state.phase == STOPPED, "every other loop ended too"
    assert state.last_error.startswith(f"runtime: the {loop} loop failed: Boom(")
    assert pools[0].closed, "the pool was closed as on any stop"


def test_only_a_failed_workers_loop_kills_the_workers(tmp_path):
    """Nothing waits on that loop's tables any more, so they are not left running."""
    for loop, killed in (("workers", True), ("scheduler", False)):
        rt, pools = build(tmp_path, loop)
        with pytest.raises(Boom):
            asyncio.run(asyncio.wait_for(rt.run(), timeout=30))
        assert pools[0].killed is killed, loop


def test_a_table_in_flight_when_a_loop_fails_is_let_finish(tmp_path):
    """As on a stop: the failure takes no new table and kills none in flight."""
    failing = threading.Event()

    async def scheduler_fails_on_cue(stop: asyncio.Event, seconds: float) -> None:
        while not failing.is_set():
            await asyncio.sleep(0.005)
        raise Boom("the scheduler")

    pools: list[FakePool] = []
    rt = MaintenanceRuntime(
        fleet=StaticFleet(fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: QUIET,
        pool_factory=lambda size, config: (
            pools.append(FakePool(size, config, hold=True)) or pools[-1]
        ),
        wait=scheduler_fails_on_cue,
    )
    raised: list[BaseException] = []

    def main() -> None:
        try:
            asyncio.run(rt.run())
        except BaseException as exc:
            raised.append(exc)

    thread = threading.Thread(target=main, daemon=True)
    thread.start()
    eventually(lambda: rt.state().phase == RUNNING)
    rt.offer("acme", ["raw.a"], reason=MANUAL)
    eventually(lambda: rt.state().in_flight == (("acme", "raw.a"),))

    failing.set()
    eventually(lambda: rt.state().last_error is not None)
    thread.join(0.3)
    assert thread.is_alive(), "still waiting for the table in flight"
    assert rt.offer("acme", ["raw.b"], reason=MANUAL).refused == "the runtime is stopping"
    assert not pools[0].killed

    pools[0].release.set()
    thread.join(10)
    assert not thread.is_alive()
    assert len(raised) == 1 and isinstance(raised[0], Boom)
    assert [table for table, _ in pools[0].ran] == ["raw.a"], "it finished, and nothing after"
    assert rt.state().warehouses["acme"].last_exit_code == 0
