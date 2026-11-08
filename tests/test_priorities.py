# SPDX-License-Identifier: Apache-2.0
"""Priority classes, ageing and oldest-watermark-first (#165, R8; §3.4).

A queue taken first come first served makes "run now" wait behind a nightly
sweep. Every expectation here is derived from ``REASON_CLASS`` and the design
doc's §3.4 table, and every test runs against an injected clock.
"""

from __future__ import annotations

import asyncio
import dataclasses
import itertools
import re
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_runtime import QUIET, eventually
from tests.test_scheduler import fleet, warehouse
from zamboni.fleet import FleetConfig
from zamboni.pool import ItemResult, PoolSize, WorkerConfig, WorkerSlots
from zamboni.pressure import PressureSettings
from zamboni.runtime import (
    MaintenanceRuntime,
    RuntimeSettings,
    StaticFleet,
    run_pressure_checks,
    start_in_thread,
)
from zamboni.scheduler import (
    CLASS_NAMES,
    EVENT,
    HIGH,
    LOW,
    MANUAL,
    MEDIUM,
    PRESSURE,
    REASON_CLASS,
    REASON_OPERATIONS,
    SCHEDULE,
    Candidate,
    CandidateQueue,
    class_of,
)

DESIGN = Path(__file__).parent.parent / "docs" / "maintenance-runtime.md"
START = datetime(2026, 11, 5, 2, 0, tzinfo=UTC)
STEP = timedelta(minutes=30)
TABLES = tuple(f"t{n:03d}" for n in range(200))


class Clock:
    def __init__(self, at: datetime = START) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at

    def advance(self, by: timedelta) -> None:
        self.at += by


def wide_fleet():
    return fleet(warehouse("acme", tables=TABLES))


def key(table: str) -> tuple[str, str]:
    return ("acme", f"raw.{table}")


def offer(queue: CandidateQueue, table: str, reason: str, **kw) -> bool:
    return queue.offer(Candidate("acme", f"raw.{table}", reason, **kw))


def queue(**kw) -> tuple[CandidateQueue, Clock]:
    clock = Clock()
    return CandidateQueue(clock=clock, **kw), clock


# -- the classes ---------------------------------------------------------------------


def design_classes() -> dict[str, int]:
    """§3.4's table, read from the doc: ``| **high** | `manual`, `reclaim` | ...``."""
    section = DESIGN.read_text().split("### 3.4 Priorities", 1)[1].split("### 3.5", 1)[0]
    by_name = {name: cls for cls, name in CLASS_NAMES.items()}
    found = {}
    for name, reasons in re.findall(r"^\| \*\*(\w+)\*\* \| ([^|]+) \|", section, re.MULTILINE):
        for reason in re.findall(r"`([a-z-]+)`", reasons):
            found[reason] = by_name[name]
    return found


def test_every_reason_has_the_class_the_design_gives_it():
    assert set(REASON_CLASS) == set(REASON_OPERATIONS), "a reason without a class, or vice versa"
    assert design_classes() == REASON_CLASS


def test_a_merged_entry_has_its_highest_reasons_class():
    for a in REASON_CLASS:
        for b in REASON_CLASS:
            assert class_of({a, b}) == max(REASON_CLASS[a], REASON_CLASS[b])


# -- taking by class -----------------------------------------------------------------


def test_a_free_worker_takes_the_highest_class_first():
    """Offered lowest class first, so first come first served would invert it."""
    q, _ = queue()
    reasons = sorted(REASON_CLASS, key=REASON_CLASS.__getitem__)
    for n, reason in enumerate(reasons):
        offer(q, TABLES[n], reason)

    taken = [item.reason for item in iter(lambda: q.take(wide_fleet()), None)]

    assert [REASON_CLASS[r] for r in taken] == sorted(
        (REASON_CLASS[r] for r in reasons), reverse=True
    )


@pytest.mark.parametrize("reason", [r for r, c in REASON_CLASS.items() if c > LOW])
def test_within_high_and_medium_the_first_offered_is_taken_first(reason):
    q, clock = queue()
    for table in TABLES[:5]:
        offer(q, table, reason)
        clock.advance(timedelta(seconds=1))
    assert q.queued == tuple(key(t) for t in TABLES[:5])


def test_within_low_the_oldest_watermark_is_taken_first():
    """A table never maintained (or not yet heard of) first, then the one
    maintained longest ago; equal answers in the order offered."""
    marks = {
        key("t000"): START - timedelta(hours=1),
        key("t001"): START - timedelta(days=3),
        key("t002"): None,
        key("t003"): START - timedelta(days=3),
        key("t004"): START - timedelta(hours=5),
    }
    q, _ = queue(watermark=lambda wh, table: marks[(wh, table)])
    for table in ("t000", "t001", "t002", "t003", "t004"):
        offer(q, table, SCHEDULE)

    assert q.queued == (key("t002"), key("t001"), key("t003"), key("t004"), key("t000"))


def test_the_watermark_does_not_reorder_high_or_medium():
    """Only the schedule's sweep is ordered by it; an event is a write that
    already happened, and waits its turn."""
    marks = {key("t000"): START, key("t001"): None}
    q, _ = queue(watermark=lambda wh, table: marks[(wh, table)])
    offer(q, "t000", EVENT)
    offer(q, "t001", EVENT)
    assert q.queued == (key("t000"), key("t001"))


def test_coalescing_keeps_the_higher_class_and_the_first_offer():
    q, clock = queue()
    offer(q, "t000", SCHEDULE)
    clock.advance(timedelta(minutes=1))
    offer(q, "t001", MANUAL)
    offer(q, "t000", MANUAL)  # an operator asks for a table the sweep queued

    assert q.class_now(key("t000")) == HIGH
    assert q.queued == (key("t000"), key("t001")), "offered first, so taken first among high"


# -- ageing --------------------------------------------------------------------------


def test_waiting_raises_an_entry_one_class_per_step_up_to_high():
    q, clock = queue(age_step=STEP)
    offer(q, "t000", SCHEDULE)
    seen = []
    for _ in range(HIGH - LOW + 2):
        seen.append(q.class_now(key("t000")))
        clock.advance(STEP)
    assert seen == [*range(LOW, HIGH + 1), HIGH], "one class per step, and never above high"


def test_without_an_age_step_nothing_ages():
    q, clock = queue(age_step=None)
    offer(q, "t000", SCHEDULE)
    clock.advance(timedelta(days=30))
    assert q.class_now(key("t000")) == LOW


@pytest.mark.parametrize("step", [timedelta(0), -STEP])
def test_an_age_step_that_is_not_positive_is_refused(step):
    with pytest.raises(ValueError, match="priority_age_step must be positive"):
        CandidateQueue(age_step=step)


def run_against_a_constant_event_stream(age_step: timedelta | None, minutes: int):
    """One worker; every minute a new table's event arrives and the worker
    takes one entry. Returns when the scheduled table was taken, or None."""
    q, clock = queue(age_step=age_step)
    current = wide_fleet()
    offer(q, "t000", SCHEDULE)
    offer(q, "t001", EVENT)
    for minute in range(minutes):
        offer(q, TABLES[2 + minute], EVENT)
        item = q.take(current)
        assert item is not None
        q.finish(item)
        if item.reason == SCHEDULE:
            return clock()
        clock.advance(timedelta(minutes=1))
    return None


def test_ageing_prevents_starvation_under_a_constant_stream_of_events():
    """Medium-class work never runs out, so without ageing the sweep never runs."""
    horizon = int(STEP / timedelta(minutes=1)) * 3
    assert run_against_a_constant_event_stream(None, horizon) is None, "the starvation it prevents"

    taken = run_against_a_constant_event_stream(STEP, horizon)
    # One step raises it to medium, where it was offered before every event queued.
    assert taken == START + STEP * (MEDIUM - LOW)


def test_a_dirty_re_offer_has_been_waiting_since_it_went_dirty():
    q, clock = queue(age_step=STEP)
    current = wide_fleet()
    offer(q, "t000", SCHEDULE)
    running = q.take(current)
    offer(q, "t000", EVENT)  # a write during the run: dirty
    clock.advance(STEP)  # a long run
    q.finish(running)

    assert q.class_now(key("t000")) == REASON_CLASS[EVENT] + 1


# -- reserved workers ----------------------------------------------------------------


def test_a_reserved_worker_takes_only_what_is_high_by_its_reasons():
    """Not by ageing: a sweep left waiting would age into high and fill the
    worker kept for an operator's "run now"."""
    q, clock = queue(age_step=STEP)
    current = wide_fleet()
    offer(q, "t000", SCHEDULE)
    offer(q, "t001", PRESSURE)
    clock.advance(STEP * (HIGH - LOW))
    assert q.class_now(key("t000")) == HIGH

    assert q.take(current, high_only=True) is None
    offer(q, "t002", MANUAL)
    assert q.take(current, high_only=True).table == "raw.t002"
    assert q.take(current).table == "raw.t000", "aged to high, and offered first"


# -- the slots ------------------------------------------------------------------------


@pytest.mark.parametrize(("workers", "asked"), [(1, 1), (2, 1), (4, 1), (4, 3), (4, 9), (3, 0)])
def test_the_reservation_is_capped_at_one_worker_short_of_the_pool(workers, asked):
    slots = WorkerSlots(workers, asked)
    assert slots.reserved == min(asked, workers - 1)
    for _ in range(workers - slots.reserved):
        slots.hold(False)
    assert not slots.general_free, "the rest are kept for high-class work"
    assert slots.free is (slots.reserved > 0)


def test_a_reservation_below_zero_is_refused_by_the_runtimes_constructor(tmp_path):
    with pytest.raises(ValueError, match="reserved_high_slots must be at least 0"):
        rt(tmp_path, workers=2, reserved_high_slots=-1)


def test_holding_a_worker_that_is_not_free_is_refused():
    slots = WorkerSlots(2, 1)
    slots.hold(False)
    with pytest.raises(RuntimeError, match="no general worker"):
        slots.hold(False)
    slots.hold(True)
    with pytest.raises(RuntimeError, match="no free worker"):
        slots.hold(True)


def test_a_pressure_check_waits_for_a_general_worker_not_a_reserved_one():
    """Before #165 a check went straight to the pool, and could take the one
    worker a "run now" was waiting for."""
    slots = WorkerSlots(2, 1)
    checked: list[str] = []

    class Pool:
        async def run(self, check):
            checked.append(check.warehouse)
            return SimpleNamespace(record=None, exit_code=0)

    current = FleetConfig(
        warehouses=(
            dataclasses.replace(
                warehouse("acme"), pressure=PressureSettings(check_every_minutes=1)
            ),
        )
    )

    async def scenario():
        stop = asyncio.Event()
        minutes = itertools.count()
        slots.hold(False)  # the one general worker, on a sweep
        loop = asyncio.create_task(
            run_pressure_checks(
                lambda: current,
                Pool(),
                lambda c: True,
                stop,
                clock=lambda: START + timedelta(minutes=next(minutes)),
                wait=lambda stop, seconds: asyncio.sleep(0),
                slots=slots,
            )
        )
        for _ in range(20):
            await asyncio.sleep(0)
        assert checked == [], "the reserved worker was free, and not taken"
        slots.release(False)
        for _ in range(20):
            await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(loop, 5)

    asyncio.run(scenario())
    assert checked and set(checked) == {"acme"}


# -- the runtime ----------------------------------------------------------------------


class GatedPool:
    """The pool's interface without processes; each table runs until the test
    lets it go, so the test can hold a sweep on every worker."""

    def __init__(self, size: PoolSize, config: WorkerConfig) -> None:
        self.size = size
        self.gates: dict[str, threading.Event] = {}
        self.started: list[str] = []
        self.killed = self.closed = False

    def gate(self, table: str) -> threading.Event:
        return self.gates.setdefault(table, threading.Event())

    async def run(self, item):
        self.started.append(item.table)
        gate = self.gate(item.table)
        while not (gate.is_set() or self.killed):
            await asyncio.sleep(0.005)
        return ItemResult(item, 0, {"exit_code": 0}, "", None, 0.0)

    def release_all(self) -> None:
        for table in TABLES:
            self.gate(f"raw.{table}").set()

    def kill(self) -> None:
        self.killed = True

    async def close(self) -> None:
        self.closed = True


def rt(tmp_path, *, workers: int, pools: list | None = None, **settings) -> MaintenanceRuntime:
    pools = [] if pools is None else pools
    return MaintenanceRuntime(
        fleet=StaticFleet(wide_fleet()),
        pool=PoolSize(workers, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        settings=RuntimeSettings(**settings),
        clock=lambda: QUIET,
        pool_factory=lambda size, config: pools.append(GatedPool(size, config)) or pools[-1],
    )


def sweep_then_run_now(tmp_path, workers: int, **settings) -> tuple[list[str], list[str]]:
    """Every table in a sweep, then an operator's "run now" while it holds the
    workers. Returns what had started before and after the request."""
    pools: list[GatedPool] = []
    handle = start_in_thread(rt(tmp_path, workers=workers, pools=pools, **settings))
    runtime = handle.runtime
    sweep = [f"raw.{t}" for t in TABLES[:10]]
    try:
        runtime.offer("acme", sweep, reason=SCHEDULE)
        reserved = WorkerSlots(workers, runtime.settings.reserved_high_slots).reserved
        eventually(lambda: len(pools[0].started) == workers - reserved)
        time.sleep(0.1)  # long enough for a free worker to take more, if it would
        before = list(pools[0].started)
        runtime.offer("acme", [f"raw.{TABLES[-1]}"], reason=MANUAL)
        eventually(lambda: f"raw.{TABLES[-1]}" in pools[0].started, timeout=1.0)
        return before, list(pools[0].started)
    finally:
        pools[0].release_all()
        assert handle.stop(grace=timedelta(seconds=10))


def test_run_now_starts_on_a_reserved_worker_while_every_other_runs_the_sweep(tmp_path):
    workers = 3
    before, after = sweep_then_run_now(tmp_path, workers)

    reserved = RuntimeSettings().reserved_high_slots
    assert len(before) == workers - reserved, "the sweep never took the reserved worker"
    assert after == [*before, f"raw.{TABLES[-1]}"], "run now started without waiting"


def test_without_a_reservation_run_now_waits_behind_the_sweep(tmp_path):
    """The negative case: what the reservation buys."""
    with pytest.raises(AssertionError, match="timed out waiting"):
        sweep_then_run_now(tmp_path, 3, reserved_high_slots=0)


def test_a_single_worker_pool_still_serves_every_class(tmp_path):
    """The reservation is capped at workers - 1: 0 here, so a sweep runs."""
    pools: list[GatedPool] = []
    handle = start_in_thread(rt(tmp_path, workers=1, pools=pools))
    try:
        for n, reason in enumerate(REASON_CLASS):
            handle.runtime.offer("acme", [f"raw.{TABLES[n]}"], reason=reason)
        pools[0].release_all()
        eventually(lambda: len(pools[0].started) == len(REASON_CLASS))
    finally:
        assert handle.stop(grace=timedelta(seconds=10))
    started = [TABLES.index(t.removeprefix("raw.")) for t in pools[0].started]
    reasons = list(REASON_CLASS)
    assert [REASON_CLASS[reasons[n]] for n in started[1:]] == sorted(
        (REASON_CLASS[reasons[n]] for n in started[1:]), reverse=True
    ), "whatever started first, the rest by class"


def test_an_age_step_the_runtime_cannot_use_is_refused_by_its_constructor(tmp_path):
    with pytest.raises(ValueError, match="priority_age_step must be positive"):
        rt(tmp_path, workers=2, priority_age_step=timedelta(0))
