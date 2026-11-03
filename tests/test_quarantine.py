# SPDX-License-Identifier: Apache-2.0
"""A table that fails every time backs off (docs/maintenance-runtime.md §7.1, #170).

The first half drives :class:`Quarantine` with results and times; the second
makes a table fail three times in a row through a running runtime and asserts
what it refuses and what it reports.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import timedelta

import pytest

from tests.test_runtime import QUIET, FakePool, eventually, fleet, warehouse
from zamboni.pool import SOUND_EXITS, ItemResult, PoolSize, WorkerConfig
from zamboni.quarantine import Quarantine
from zamboni.runtime import MaintenanceRuntime, RuntimeSettings, StaticFleet, start_in_thread
from zamboni.scheduler import EVENT, MANUAL, REASON_OPERATIONS, SCHEDULE, Candidate, WorkItem

SETTINGS = RuntimeSettings()
AFTER = SETTINGS.quarantine_after
assert AFTER is not None
BACKOFF, MOST = SETTINGS.quarantine_backoff, SETTINGS.quarantine_backoff_max
WAREHOUSE = warehouse()
TABLE = "raw.a"
KEY = f"acme/{TABLE}"


def quarantine() -> Quarantine:
    return Quarantine(after=AFTER, backoff=BACKOFF, most=MOST)


def work(reason: str = SCHEDULE) -> WorkItem:
    return WorkItem("acme", TABLE, reason, WAREHOUSE.table_config)


def failed(why: str = "stalled: no progress report for 1:00:00") -> ItemResult:
    return ItemResult(work(), 1, None, "", why, 1.0)


def ended(code: int) -> ItemResult:
    return ItemResult(work(), code, {"exit_code": code}, "", None, 1.0)


def offer(reason: str) -> Candidate:
    return Candidate("acme", TABLE, reason)


def test_the_failures_before_the_last_do_not_quarantine():
    q = quarantine()
    for _ in range(AFTER - 1):
        assert q.finished(failed(), QUIET) == ("", None)
    assert q.quarantined == {}
    assert q.admits(offer(SCHEDULE), QUIET)


def test_the_last_failure_in_a_row_quarantines_until_the_backoff_passes():
    q = quarantine()
    for _ in range(AFTER - 1):
        q.finished(failed(), QUIET)

    change, held = q.finished(failed("timed out after 6:00:00 (schedule)"), QUIET)

    assert change == "quarantined"
    assert held == q.quarantined[KEY]
    assert (held.failures, held.until, held.backoff) == (AFTER, QUIET + BACKOFF, BACKOFF)
    assert held.last_failure == "timed out after 6:00:00 (schedule)"
    for reason in sorted(set(REASON_OPERATIONS) - {MANUAL}):
        assert not q.admits(offer(reason), QUIET + BACKOFF / 2), reason
    assert q.admits(offer(MANUAL), QUIET), "an operator asking is the override"
    assert q.admits(offer(SCHEDULE), QUIET + BACKOFF), "admitted again once it has passed"


def test_each_failure_after_doubles_the_backoff_up_to_its_ceiling():
    q = quarantine()
    backoffs = []
    for n in range(AFTER + 8):
        _, held = q.finished(failed(), QUIET)
        if n + 1 >= AFTER:
            backoffs.append(held.backoff)

    expected = [min(BACKOFF * 2**n, MOST) for n in range(len(backoffs))]
    assert backoffs == expected
    assert backoffs[-1] == MOST, "long enough to reach the ceiling"


@pytest.mark.parametrize("code", sorted(SOUND_EXITS))
def test_a_sound_run_clears_a_quarantine_and_says_so(code):
    q = quarantine()
    for _ in range(AFTER):
        q.finished(failed(), QUIET)

    assert q.finished(ended(code), QUIET) == ("released", None)
    assert q.quarantined == {}
    for _ in range(AFTER - 1):
        q.finished(failed(), QUIET)
    assert q.quarantined == {}, "the count started again from nothing"


def test_a_sound_run_resets_failures_that_had_not_yet_quarantined():
    q = quarantine()
    for _ in range(AFTER - 1):
        q.finished(failed(), QUIET)
    assert q.finished(ended(0), QUIET) == ("", None)
    for _ in range(AFTER - 1):
        q.finished(failed(), QUIET)
    assert q.quarantined == {}, "not in a row"


def test_a_table_that_never_ran_neither_counts_nor_clears():
    q = quarantine()
    dropped = dataclasses.replace(ended(0), dropped="deadline")
    for _ in range(AFTER - 1):
        q.finished(failed(), QUIET)
        assert q.finished(dropped, QUIET) == ("", None)
    q.finished(failed(), QUIET)
    assert KEY in q.quarantined, "the drops between did not break the row"
    q.finished(dropped, QUIET)
    assert KEY in q.quarantined, "and a drop did not clear it"


def test_a_table_the_fleet_no_longer_has_is_forgotten():
    q = quarantine()
    for _ in range(AFTER):
        q.finished(failed(), QUIET)
    q.adopted(fleet(warehouse(tables=("b",))))
    assert q.quarantined == {}


def test_none_never_quarantines():
    q = Quarantine(after=None, backoff=BACKOFF, most=MOST)
    for _ in range(AFTER * 3):
        q.finished(failed(), QUIET)
    assert q.quarantined == {}


@pytest.mark.parametrize(
    "settings",
    [
        {"quarantine_after": 0},
        {"quarantine_backoff": timedelta(0)},
        {"quarantine_backoff": timedelta(hours=2), "quarantine_backoff_max": timedelta(hours=1)},
    ],
    ids=["no-failures", "no-backoff", "ceiling-below-backoff"],
)
def test_a_quarantine_it_cannot_use_is_refused_by_the_runtimes_constructor(tmp_path, settings):
    with pytest.raises(ValueError, match="quarantine"):
        MaintenanceRuntime(
            fleet=StaticFleet(fleet()),
            pool=PoolSize(1, 1, "test"),
            worker=WorkerConfig(workdir=tmp_path),
            settings=RuntimeSettings(**settings),
        )


# -- through a running runtime --------------------------------------------------------


class FailingPool(FakePool):
    """Every run of ``raw.a`` fails as a stalled worker would; others succeed."""

    async def run(self, item: WorkItem) -> ItemResult:
        if item.table != TABLE:
            return await super().run(item)
        self.ran.append((item.table, "pool"))
        self.items.append(item)
        await asyncio.sleep(0)
        return ItemResult(item, 1, None, "", "stalled: no progress report for 1:00:00", 0.0)


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def quarantined(self, warehouse, table, state) -> None:
        self.calls.append(("quarantined", warehouse, table, state.failures))

    def released(self, warehouse, table) -> None:
        self.calls.append(("released", warehouse, table))


def test_a_table_failing_in_a_row_is_quarantined_and_reported(tmp_path):
    pools: list[FailingPool] = []
    recorder = Recorder()
    now = [QUIET]
    rt = MaintenanceRuntime(
        fleet=StaticFleet(fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: now[0],
        pool_factory=lambda size, config: pools.append(FailingPool(size, config)) or pools[-1],
        observers=[recorder],
    )
    handle = start_in_thread(rt)

    def ran(n: int) -> bool:
        """``raw.a`` has run ``n`` times, and nothing is in flight."""
        runs = sum(item.table == TABLE for item in pools[0].items)
        return runs == n and not rt.state().in_flight

    try:
        for n in range(1, AFTER + 1):
            assert rt.offer("acme", [TABLE], reason=EVENT).queued == (TABLE,)
            eventually(lambda n=n: ran(n))

        refused = rt.offer("acme", [TABLE], reason=EVENT)
        assert (refused.queued, refused.quarantined) == ((), (TABLE,))
        held = rt.state().quarantined[KEY]
        assert (held.failures, held.until) == (AFTER, QUIET + BACKOFF)
        assert held.last_failure.startswith("stalled")
        assert recorder.calls == [("quarantined", "acme", TABLE, AFTER)]
        assert rt.offer("acme", ["raw.b"], reason=EVENT).queued == ("raw.b",), (
            "only the failing table backs off"
        )

        assert rt.offer("acme", [TABLE], reason=MANUAL).queued == (TABLE,), "the override"
        eventually(lambda: ran(AFTER + 1))
        assert rt.state().quarantined[KEY].backoff == 2 * BACKOFF, "it failed again: doubled"

        now[0] = QUIET + 3 * BACKOFF  # past the doubled backoff
        assert rt.offer("acme", [TABLE], reason=EVENT).queued == (TABLE,)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))


def test_a_schedule_firing_skips_a_quarantined_table(tmp_path):
    """A firing offers straight to the queue, not through `offer()`, so this is
    the queue's own gate: the warehouse's other tables run, the failing one
    does not. The backoff is longer than a day so it still holds at the
    next 02:00."""
    pools: list[FailingPool] = []
    now = [QUIET]

    async def step(stop: asyncio.Event, seconds: float) -> None:
        await asyncio.sleep(0.01)

    rt = MaintenanceRuntime(
        fleet=StaticFleet(fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        clock=lambda: now[0],
        pool_factory=lambda size, config: pools.append(FailingPool(size, config)) or pools[-1],
        settings=RuntimeSettings(
            quarantine_backoff=timedelta(days=2), quarantine_backoff_max=timedelta(days=4)
        ),
        wait=step,
    )
    handle = start_in_thread(rt)
    try:
        for n in range(1, AFTER + 1):
            rt.offer("acme", [TABLE], reason=EVENT)
            eventually(lambda n=n: len(pools[0].items) == n and not rt.state().in_flight)
        assert KEY in rt.state().quarantined

        now[0] = QUIET + timedelta(days=1)  # past the next 02:00 firing
        eventually(lambda: rt.state().warehouses["acme"].last_fired is not None)
        eventually(lambda: not rt.state().in_flight and not rt.state().queued)

        tables = [item.table for item in pools[0].items]
        assert tables.count(TABLE) == AFTER, "the firing did not run the quarantined table"
        assert {"raw.b", "raw.c"} <= set(tables[AFTER:]), "it ran the others"
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
