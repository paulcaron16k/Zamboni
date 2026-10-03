# SPDX-License-Identifier: Apache-2.0
"""The scheduler and the candidate queue.

Almost everything here runs against an injected clock, because the properties
worth pinning -- a missed firing fires once, a reload keeps an unchanged
warehouse's firing, a queued table runs under the config current when it is
*taken* -- are about time and ordering, and a test that sleeps to observe them
is a slow test that flakes.
"""

from __future__ import annotations

import asyncio
import dataclasses
import pickle
from datetime import UTC, datetime, timedelta

import pytest

from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse
from zamboni.scheduler import (
    EVENT,
    MAX_SLEEP,
    SCHEDULE,
    Candidate,
    CandidateQueue,
    Scheduler,
    WorkItem,
    run_scheduler,
)
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def warehouse(name: str, schedule: str = "0 2 * * *", tables=("events", "sessions"), uri=None):
    """A warehouse that fires on the exact minute.

    Spreading is on by default; these tests switch it off so they can state the
    minute a firing happens. :func:`spread` builds one with it on.
    """
    return FleetWarehouse(
        name=name,
        schedule=CronSchedule(schedule, random=False),
        table_config=TableConfig(
            warehouse=name,
            namespaces={"raw": NamespaceSettings(tables={t: TableSettings() for t in tables})},
        ),
        uri=uri,
    )


def fleet(*warehouses: FleetWarehouse) -> FleetConfig:
    return FleetConfig(warehouses=warehouses or (warehouse("acme"),))


def drain(queue: CandidateQueue, current: FleetConfig) -> list[WorkItem]:
    items = []
    while (item := queue.take(current)) is not None:
        items.append(item)
    return items


# -- the queue ------------------------------------------------------------------


def test_offering_a_queued_table_coalesces():
    queue = CandidateQueue()
    assert queue.offer(Candidate("acme", "raw.events", EVENT))
    for _ in range(49):
        assert not queue.offer(Candidate("acme", "raw.events", EVENT))
    assert not queue.offer(Candidate("acme", "raw.events", SCHEDULE))
    assert len(queue) == 1


def test_a_table_being_maintained_is_not_re_enqueued():
    queue, current = CandidateQueue(), fleet()
    queue.offer(Candidate("acme", "raw.events"))
    item = queue.take(current)
    assert item is not None and item.key in queue.in_flight

    assert not queue.offer(Candidate("acme", "raw.events", EVENT))
    assert queue.take(current) is None

    queue.finish(item)
    assert queue.offer(Candidate("acme", "raw.events", EVENT))


def test_the_same_table_name_in_two_warehouses_is_two_candidates():
    """A table identifier is not unique across a fleet -- the same trap runlog
    documents -- so the key is (warehouse, table)."""
    queue = CandidateQueue()
    assert queue.offer(Candidate("acme", "raw.events"))
    assert queue.offer(Candidate("globex", "raw.events"))
    assert len(queue) == 2


def test_order_is_first_offered_first_taken():
    queue, current = CandidateQueue(), fleet()
    queue.offer(Candidate("acme", "raw.sessions"))
    queue.offer(Candidate("acme", "raw.events"))
    assert [i.table for i in drain(queue, current)] == ["raw.sessions", "raw.events"]


def test_a_queued_item_takes_the_config_current_when_it_is_taken():
    """A reload changes the next decision: an item queued before the reload
    runs under the reloaded config, because the queue held only its key."""
    queue = CandidateQueue()
    queue.offer(Candidate("acme", "raw.events"))

    reloaded = fleet(warehouse("acme", uri="https://elsewhere/catalog"))
    item = queue.take(reloaded)

    assert item is not None
    assert item.uri == "https://elsewhere/catalog"
    assert item.table_config is reloaded["acme"].table_config


def test_an_item_in_flight_keeps_the_config_it_started_with():
    """...and never one in flight: the worker holds its own frozen WorkItem, and
    a later take against a reloaded fleet builds a new one rather than editing it."""
    queue = CandidateQueue()
    queue.offer(Candidate("acme", "raw.events"))
    queue.offer(Candidate("acme", "raw.sessions"))
    running = queue.take(fleet())
    assert running is not None
    before = running.table_config

    reloaded = fleet(warehouse("acme", uri="https://elsewhere/catalog"))
    after = queue.take(reloaded)

    assert after is not None and after.uri == "https://elsewhere/catalog"
    assert running.table_config is before and running.uri is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        running.uri = "x"  # type: ignore[misc]


def test_candidates_a_reload_removed_are_dropped():
    queue = CandidateQueue()
    queue.offer(Candidate("gone", "raw.events"))
    queue.offer(Candidate("acme", "raw.removed"))
    queue.offer(Candidate("acme", "raw.events"))

    assert [i.key for i in drain(queue, fleet())] == [("acme", "raw.events")]
    assert queue.in_flight == {("acme", "raw.events")}


def test_a_work_item_is_config_that_pickles():
    """#121 sends these to another process. A CatalogSession cannot go; this must."""
    queue = CandidateQueue()
    queue.offer(Candidate("acme", "raw.events"))
    item = queue.take(fleet(warehouse("acme", uri="https://c/catalog")))
    assert pickle.loads(pickle.dumps(item)) == item
    assert {f.name for f in dataclasses.fields(WorkItem)} == {
        "warehouse",
        "table",
        "reason",
        "table_config",
        "uri",
    }, "a new WorkItem field must be serialisable config, and must not bypass the due-check"


def test_get_waits_for_an_offer():
    async def scenario():
        queue, current = CandidateQueue(), fleet()
        waiter = asyncio.create_task(queue.get(lambda: current))
        await asyncio.sleep(0)
        assert not waiter.done()
        queue.offer(Candidate("acme", "raw.events"))
        return await asyncio.wait_for(waiter, timeout=1)

    assert asyncio.run(scenario()).table == "raw.events"


# -- the scheduler -------------------------------------------------------------


def test_a_tick_offers_every_table_of_a_due_warehouse_and_nothing_else():
    current = fleet(warehouse("acme", "0 2 * * *"), warehouse("globex", "0 3 * * *"))
    scheduler, queue = Scheduler(current, at("2026-10-01T00:00")), CandidateQueue()

    assert scheduler.tick(at("2026-10-01T01:59"), queue) == []
    assert scheduler.tick(at("2026-10-01T02:00"), queue) == ["acme"]

    assert sorted(c.table for c in drain(queue, current)) == ["raw.events", "raw.sessions"]
    assert scheduler.next_firing("acme") == at("2026-10-02T02:00")
    assert scheduler.next_firing("globex") == at("2026-10-01T03:00")


def test_a_tick_is_only_ever_an_offer():
    """The due-check lives in maintain(); a tick can only put keys in the queue.
    Everything it produced is a Candidate with the schedule reason."""
    current = fleet()
    scheduler, queue = Scheduler(current, at("2026-10-01T00:00")), CandidateQueue()
    scheduler.tick(at("2026-10-01T02:00"), queue)
    items = drain(queue, current)
    assert items and all(i.reason == SCHEDULE for i in items)


def test_missed_firings_fire_once():
    scheduler = Scheduler(fleet(warehouse("acme", "*/5 * * * *")), at("2026-10-01T00:00"))
    queue = CandidateQueue()

    assert scheduler.tick(at("2026-10-01T00:31"), queue) == ["acme"]  # six firings missed
    assert len(queue) == 2
    assert scheduler.next_firing("acme") == at("2026-10-01T00:35")
    assert scheduler.tick(at("2026-10-01T00:32"), queue) == []


def test_a_fresh_start_waits_for_the_next_firing():
    """Started at 02:01, the 02:00 run is not replayed: a restart is not a sweep."""
    scheduler = Scheduler(fleet(), at("2026-10-01T02:01"))
    assert scheduler.tick(at("2026-10-01T02:01"), CandidateQueue()) == []
    assert scheduler.next_firing("acme") == at("2026-10-02T02:00")


def test_a_tick_while_a_table_is_in_flight_does_not_queue_it_again():
    current = fleet(warehouse("acme", "*/5 * * * *", tables=("events",)))
    scheduler, queue = Scheduler(current, at("2026-10-01T00:00")), CandidateQueue()
    scheduler.tick(at("2026-10-01T00:05"), queue)
    running = queue.take(current)
    assert running is not None

    scheduler.tick(at("2026-10-01T00:10"), queue)
    assert len(queue) == 0


def test_rearm_keeps_an_unchanged_warehouses_firing():
    """Re-arming everything from now would push acme's 02:00 run to tomorrow
    whenever globex was provisioned at 01:59."""
    scheduler = Scheduler(fleet(warehouse("acme")), at("2026-10-01T00:00"))
    before = scheduler.next_firing("acme")

    scheduler.rearm(
        fleet(warehouse("acme"), warehouse("globex", "30 1 * * *")), at("2026-10-01T01:59")
    )

    assert scheduler.next_firing("acme") == before == at("2026-10-01T02:00")
    assert scheduler.next_firing("globex") == at("2026-10-02T01:30")


def test_rearm_rearms_a_changed_schedule_and_forgets_a_removed_warehouse():
    scheduler = Scheduler(fleet(warehouse("acme"), warehouse("globex")), at("2026-10-01T00:00"))
    scheduler.rearm(fleet(warehouse("acme", "0 4 * * *")), at("2026-10-01T01:00"))

    assert scheduler.next_firing("acme") == at("2026-10-01T04:00")
    assert scheduler.fleet.names == ("acme",)
    assert scheduler.tick(at("2026-10-01T02:00"), queue := CandidateQueue()) == []
    assert len(queue) == 0


def test_rearm_keeps_a_firing_when_only_the_tables_changed():
    scheduler = Scheduler(fleet(warehouse("acme", tables=("events",))), at("2026-10-01T00:00"))
    scheduler.rearm(fleet(warehouse("acme", tables=("events", "new"))), at("2026-10-01T01:00"))
    queue = CandidateQueue()
    scheduler.tick(at("2026-10-01T02:00"), queue)
    assert sorted(c.table for c in drain(queue, scheduler.fleet)) == ["raw.events", "raw.new"]


# -- the loop -------------------------------------------------------------------


def test_the_loop_ticks_offers_and_stops():
    """The real loop, with a clock that runs a minute per step so it reaches
    the firing without sleeping for it."""
    now = [at("2026-10-01T01:58")]

    def clock() -> datetime:
        return now[0]

    async def scenario():
        scheduler = Scheduler(fleet(warehouse("acme", "0 2 * * *")), clock())
        queue, stop, ticks = CandidateQueue(), asyncio.Event(), []

        def on_tick(moment: datetime) -> None:
            ticks.append(moment)
            now[0] += timedelta(minutes=1)
            if len(queue):
                stop.set()

        async def no_sleep(stop: asyncio.Event, seconds: float) -> None:
            waits.append(seconds)
            await asyncio.sleep(0)

        waits: list[float] = []
        loop = run_scheduler(scheduler, queue, stop, clock=clock, on_tick=on_tick, wait=no_sleep)
        await asyncio.wait_for(loop, 5)
        # 01:59 -> 02:00 is 60 s; 02:00 is due now; after firing, tomorrow's
        # 02:00 is ~24 h away and the sleep is capped at MAX_SLEEP.
        assert waits == [60.0, 0.0, MAX_SLEEP.total_seconds()]
        return ticks, drain(queue, scheduler.fleet)

    ticks, items = asyncio.run(scenario())
    assert ticks == [at("2026-10-01T01:58"), at("2026-10-01T01:59"), at("2026-10-01T02:00")]
    assert {i.table for i in items} == {"raw.events", "raw.sessions"}


# -- spread firings (ZMBNI-144) ---------------------------------------------------


def spread(name: str, schedule: str = "0 2 * * *", tables=("events",)) -> FleetWarehouse:
    w = warehouse(name, schedule, tables)
    return dataclasses.replace(w, schedule=CronSchedule(schedule, random=True))


def named(sign: int) -> str:
    """A warehouse name whose daily offset has the given sign."""
    nominal = at("2026-10-01T02:00")
    for i in range(100):
        name = f"w{i}"
        offset = CronSchedule("0 2 * * *", random=True).offset(nominal, name)
        if offset * sign > timedelta(minutes=5):
            return name
    raise AssertionError("no seed found")


def test_a_fleet_on_one_expression_no_longer_fires_in_one_minute():
    names = [f"tenant-{i}" for i in range(100)]
    together = Scheduler(fleet(*(warehouse(n) for n in names)), at("2026-10-01T00:00"))
    apart = Scheduler(fleet(*(spread(n) for n in names)), at("2026-10-01T00:00"))

    assert len(together.tick(at("2026-10-01T02:00"), CandidateQueue())) == 100

    due = [apart.next_firing(n) for n in names]
    assert all(at("2026-10-01T01:30") <= d <= at("2026-10-01T02:30") for d in due)
    assert len({d.replace(second=0) for d in due}) > 30, "spread over many distinct minutes"
    assert len(apart.tick(at("2026-10-01T02:00"), CandidateQueue())) < 100


def test_a_firing_moved_early_does_not_fire_twice():
    """Offered at e.g. 01:40 for a nominal 02:00, the next firing is tomorrow's --
    not the 02:00 that is still ahead of it."""
    name = named(-1)
    scheduler = Scheduler(fleet(spread(name)), at("2026-10-01T00:00"))
    due = scheduler.next_firing(name)
    assert due < at("2026-10-01T02:00")

    assert scheduler.tick(due, CandidateQueue()) == [name]
    assert scheduler.tick(at("2026-10-01T02:00"), CandidateQueue()) == []
    tomorrow = scheduler.next_firing(name)
    assert at("2026-10-02T01:30") <= tomorrow <= at("2026-10-02T02:30")


def test_a_firing_moved_late_waits_for_its_moment():
    name = named(+1)
    scheduler = Scheduler(fleet(spread(name)), at("2026-10-01T00:00"))
    due = scheduler.next_firing(name)
    assert due > at("2026-10-01T02:00")

    assert scheduler.tick(at("2026-10-01T02:00"), CandidateQueue()) == []
    assert scheduler.tick(due, CandidateQueue()) == [name]
    tomorrow = scheduler.next_firing(name)
    assert at("2026-10-02T01:30") <= tomorrow <= at("2026-10-02T02:30")


def test_a_missed_spread_firing_still_fires_once():
    name = named(-1)
    scheduler = Scheduler(fleet(spread(name)), at("2026-10-01T00:00"))
    assert scheduler.tick(at("2026-10-03T12:00"), CandidateQueue()) == [name]
    assert scheduler.next_firing(name).date().isoformat() == "2026-10-04"


def test_turning_random_on_rearms_only_that_warehouse():
    early = named(-1)
    scheduler = Scheduler(fleet(warehouse(early), warehouse("acme")), at("2026-10-01T00:00"))
    before = scheduler.next_firing("acme")

    scheduler.rearm(fleet(spread(early), warehouse("acme")), at("2026-10-01T00:10"))

    assert scheduler.next_firing("acme") == before
    assert scheduler.next_firing(early) < at("2026-10-01T02:00")


def test_spreading_is_the_default():
    """Worst-case-safe by default: a warehouse built with no opinion is spread."""
    plain = FleetWarehouse(
        name="acme",
        schedule=CronSchedule("0 2 * * *"),
        table_config=warehouse("acme").table_config,
    )
    assert plain.schedule.random is True


def test_a_restart_does_not_re_roll_tonights_firing():
    """The offset is derived from (warehouse, firing), so a service restarted
    at 01:45 arms the same moment it was already waiting for."""
    current = fleet(spread("acme"))
    first = Scheduler(current, at("2026-10-01T00:00")).next_firing("acme")
    again = Scheduler(current, at("2026-10-01T01:15")).next_firing("acme")
    assert first == again
