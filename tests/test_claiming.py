# SPDX-License-Identifier: Apache-2.0
"""The runtime holding claims while tables run (#167, docs/maintenance-runtime.md §6).

The kill is tested on real worker processes: "the table's worker is killed
before its lease expires" is a claim about a process and a clock, which a fake
pool can only restate. The rest -- claimed elsewhere, a failing store, release,
what observers see -- is the runtime's own logic, on the fake pool.
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from zamboni.claims import Claim, LocalClaimStore
from zamboni.pool import ItemResult, PoolSize, WorkerConfig, WorkerPool
from zamboni.runtime import MaintenanceRuntime, RuntimeSettings, StaticFleet, start_in_thread
from zamboni.scheduler import SCHEDULE, WorkItem

from .test_runtime import QUIET, FakePool, eventually, fleet, warehouse

#: Where `hang` says it started: a test proves a table did *not* run by its absence.
RAN = "ZAMBONI_TEST_RAN_DIR"


def hang(argv: list[str]) -> int:
    """A worker entry: say it started, then outlast any lease a test uses.

    ``raw.warm`` returns at once. Run first, it spawns the worker, so the
    spawn (about 2 s) is not spent out of the lease of the table under test.
    """
    Path(os.environ[RAN], argv[1]).touch()
    if argv[1] != "raw.warm":
        time.sleep(60)
    return 0


# -- stores that misbehave ----------------------------------------------------------


class StopsRenewing(LocalClaimStore):
    """Acquires and releases; every renewal fails, as an unreachable database's would."""

    def __init__(self) -> None:
        super().__init__()
        self.renewals = 0

    def renew(self, claim, *, lease):
        self.renewals += 1
        raise ConnectionError("the claim store is unreachable")


class RenewalHangs(LocalClaimStore):
    def renew(self, claim, *, lease):
        time.sleep(30)
        return super().renew(claim, lease=lease)


class ClaimGone(LocalClaimStore):
    """Answers that the claim is gone: it expired, and may be another replica's now."""

    def renew(self, claim, *, lease):
        return None


class AcquireFails(LocalClaimStore):
    def acquire(self, warehouse, table, *, holder, lease, reason):
        raise ConnectionError("the claim store is unreachable")


# -- real workers -------------------------------------------------------------------


class Timeline:
    """Each table's claim, and its result with this process's clock when it came."""

    def __init__(self) -> None:
        self.claims: dict[str, Claim] = {}
        self.results: dict[str, tuple[ItemResult, datetime]] = {}

    def started(self, item: WorkItem, claim: Claim) -> None:
        self.claims[item.table] = claim

    def finished(self, result: ItemResult) -> None:
        self.results[result.item.table] = (result, datetime.now(UTC))


def worker_config(tmp_path: Path) -> WorkerConfig:
    ran = tmp_path / "ran"
    ran.mkdir(exist_ok=True)
    return WorkerConfig(
        entry="tests.test_claiming:hang",
        workdir=tmp_path,
        base_environ={"PATH": "/usr/bin:/bin", RAN: str(ran)},
    )


def run_warm(tmp_path, store, settings) -> Timeline:
    """Warm the only worker, then run ``raw.a`` under ``store`` until it ends."""
    timeline = Timeline()
    rt = MaintenanceRuntime(
        fleet=StaticFleet(fleet(warehouse(tables=("warm", "a")))),
        pool=PoolSize(1, 1, "test"),
        worker=worker_config(tmp_path),
        claims=store,
        settings=settings,
        observers=[timeline],
        clock=lambda: QUIET,
    )
    handle = start_in_thread(rt)
    try:
        rt.offer("acme", ["raw.warm"], reason=SCHEDULE)
        eventually(lambda: "raw.warm" in timeline.results, timeout=60)
        rt.offer("acme", ["raw.a"], reason=SCHEDULE)
        eventually(lambda: "raw.a" in timeline.results, timeout=60)
    finally:
        assert handle.stop(grace=timedelta(seconds=10))
    return timeline


#: Long enough that a warm worker has started the table before the kill at
#: two-thirds (3 s): with four copies of this file running at once on a
#: 4-CPU machine, 1.5 s was not (the kill landed before the table began).
LEASE = timedelta(seconds=4.5)


@pytest.mark.parametrize("store_class", [StopsRenewing, RenewalHangs], ids=["fails", "hangs"])
def test_a_claim_not_renewed_kills_its_worker_before_the_lease_expires(tmp_path, store_class):
    """The issue's acceptance case. Renewal stops succeeding, and the table's
    worker is SIGKILLed once two-thirds of the lease has passed -- and has
    reported dead before the claim it started under expires, so no other
    replica could have acquired the table while it ran. A renewal that hangs is
    not waited for."""
    store = store_class()
    timeline = run_warm(tmp_path, store, RuntimeSettings(claim_lease=LEASE))
    assert (tmp_path / "ran" / "raw.a").exists(), "the worker was running the table"

    claim = timeline.claims["raw.a"]
    result, ended = timeline.results["raw.a"]
    assert result.exit_code == 1 and result.record is None
    assert result.died is not None
    assert result.died.startswith("claim lost (not renewed in time); worker killed")
    assert "signal 9" in result.died
    # LocalClaimStore's clock is this process's, so the two compare directly.
    assert ended < claim.expires_at, "the worker outlived its claim"
    # The deadline runs from just *before* the acquire call -- the lease cannot
    # have started earlier -- so it may fall a call's latency before this.
    acquired = claim.expires_at - LEASE
    assert ended >= acquired + LEASE * 2 / 3 - timedelta(seconds=0.25), "killed early"
    if isinstance(store, StopsRenewing):
        assert store.renewals >= 2, "a failed renewal is retried before the deadline"


def test_a_claim_reported_gone_kills_its_worker_at_once(tmp_path):
    """Nothing to wait for: the claim may already be another replica's."""
    lease = timedelta(seconds=30)
    settings = RuntimeSettings(claim_lease=lease, claim_renew_every=timedelta(seconds=1))
    timeline = run_warm(tmp_path, ClaimGone(), settings)

    claim = timeline.claims["raw.a"]
    result, ended = timeline.results["raw.a"]
    assert result.died is not None and result.died.startswith("claim lost (the claim is gone)")
    acquired = claim.expires_at - lease
    assert ended - acquired < timedelta(seconds=10), (
        "killed at the first renewal, not at two-thirds of the lease (20 s)"
    )


# -- acquiring ----------------------------------------------------------------------


def fake_runtime(tmp_path, *, store=None, pools=None, observers=(), hold=False):
    pools = [] if pools is None else pools

    def factory(size, config):
        pools.append(FakePool(size, config, hold=hold))
        return pools[-1]

    return MaintenanceRuntime(
        fleet=StaticFleet(fleet()),
        pool=PoolSize(1, 1, "test"),
        worker=WorkerConfig(workdir=tmp_path),
        claims=store,
        observers=observers,
        clock=lambda: QUIET,
        pool_factory=factory,
    )


def test_a_table_claimed_elsewhere_is_dropped_without_running(tmp_path):
    store = LocalClaimStore()
    store.acquire("acme", "raw.a", holder="another replica", lease=LEASE * 100, reason=SCHEDULE)
    finished: list[ItemResult] = []
    started: list[WorkItem] = []
    pools: list[FakePool] = []
    observer = SimpleNamespace(finished=finished.append, started=lambda i, c: started.append(i))
    handle = start_in_thread(fake_runtime(tmp_path, store=store, pools=pools, observers=[observer]))
    try:
        handle.runtime.offer("acme", ["raw.a", "raw.b"], reason=SCHEDULE)
        eventually(lambda: len(finished) == 2)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    by_table = {r.item.table: r for r in finished}
    assert (by_table["raw.a"].exit_code, by_table["raw.a"].dropped) == (0, "claimed elsewhere")
    assert by_table["raw.b"].dropped is None
    assert [t for t, _ in pools[0].ran] == ["raw.b"]
    assert [i.table for i in started] == ["raw.b"], "a dropped table is never started"


def test_a_claim_store_that_cannot_answer_fails_the_table_unstarted(tmp_path):
    """Exit 1, not a quiet drop: an outage of the store must be seen, and a
    table it could not claim must not run."""
    finished: list[ItemResult] = []
    pools: list[FakePool] = []
    observer = SimpleNamespace(finished=finished.append)
    handle = start_in_thread(
        fake_runtime(tmp_path, store=AcquireFails(), pools=pools, observers=[observer])
    )
    try:
        handle.runtime.offer("acme", ["raw.a"], reason=SCHEDULE)
        eventually(lambda: bool(finished))
        assert handle.runtime.state().last_error == (
            "acme/raw.a: claim store error: ConnectionError('the claim store is unreachable')"
        )
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    (result,) = finished
    assert result.exit_code == 1 and result.dropped is None
    assert pools[0].ran == []


def test_two_runtimes_sharing_a_store_never_run_one_table_at_once(tmp_path):
    store = LocalClaimStore()
    pools: list[FakePool] = []
    finished: list[ItemResult] = []
    observer = SimpleNamespace(finished=finished.append)
    handles = [
        start_in_thread(
            fake_runtime(tmp_path, store=store, pools=pools, observers=[observer], hold=True),
            name=f"replica-{n}",
        )
        for n in range(2)
    ]
    try:
        for handle in handles:
            handle.runtime.offer("acme", ["raw.a"], reason=SCHEDULE)
        eventually(lambda: len(finished) == 1 and sum(len(p.ran) for p in pools) == 1)
        (dropped,) = finished
        assert dropped.dropped == "claimed elsewhere"
        for pool in pools:
            pool.release.set()
    finally:
        for handle in handles:
            assert handle.stop(grace=timedelta(seconds=5))


# -- holding and releasing ----------------------------------------------------------


def test_a_started_table_is_told_with_its_claim(tmp_path):
    started: list[tuple[WorkItem, Claim]] = []
    observer = SimpleNamespace(started=lambda item, claim: started.append((item, claim)))
    handle = start_in_thread(fake_runtime(tmp_path, observers=[observer]))
    rt = handle.runtime
    try:
        rt.offer("acme", ["raw.a"], reason=SCHEDULE)
        eventually(lambda: bool(started))
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    ((item, claim),) = started
    assert (item.warehouse, item.table) == (claim.warehouse, claim.table) == ("acme", "raw.a")
    assert claim.holder == rt.holder


@pytest.mark.parametrize("dies", [False, True], ids=["finishes", "worker-dies"])
def test_a_claim_is_released_however_the_table_ends(tmp_path, dies):
    store = LocalClaimStore()
    finished: list[ItemResult] = []
    pools: list[FakePool] = []
    observer = SimpleNamespace(finished=finished.append)
    handle = start_in_thread(
        fake_runtime(tmp_path, store=store, pools=pools, hold=True, observers=[observer])
    )

    def other() -> Claim | None:
        return store.acquire("acme", "raw.a", holder="other", lease=LEASE, reason=SCHEDULE)

    try:
        handle.runtime.offer("acme", ["raw.a"], reason=SCHEDULE)
        eventually(lambda: bool(pools[0].ran))
        assert other() is None, "claimed while it runs"
        if dies:
            pools[0].kill()
        else:
            pools[0].release.set()
        eventually(lambda: bool(finished))
        eventually(lambda: other() is not None)
    finally:
        assert handle.stop(grace=timedelta(seconds=5))
    assert (finished[0].died is not None) is dies


def test_a_renewal_cadence_the_lease_cannot_honour_is_refused_at_construction(tmp_path):
    with pytest.raises(ValueError, match="more often than 67%"):
        MaintenanceRuntime(
            fleet=StaticFleet(fleet()),
            pool=PoolSize(1, 1, "test"),
            worker=WorkerConfig(workdir=tmp_path),
            settings=RuntimeSettings(claim_lease=LEASE, claim_renew_every=LEASE),
        )


# -- WorkerPool.kill_table ------------------------------------------------------------


def item(table: str = "raw.a") -> WorkItem:
    return WorkItem(
        warehouse="acme",
        table=table,
        reason=SCHEDULE,
        table_config=fleet().warehouses[0].table_config,
    )


def test_a_table_killed_before_its_worker_took_it_never_runs(tmp_path):
    """The kill can land before the job is sent -- on no process yet, or on the
    previous one, which the slot would replace and then run the table on. Each
    round kills as soon as `run` has the slot; the table must never start."""
    config = worker_config(tmp_path)

    async def scenario() -> list[ItemResult]:
        pool = WorkerPool(PoolSize(1, 1, "test"), config)
        results = []
        try:
            for _ in range(3):
                running = asyncio.create_task(pool.run(item()))
                await asyncio.sleep(0)  # `run` has the slot, and hands it to a thread
                assert pool.kill_table(running_item(pool))
                results.append(await asyncio.wait_for(running, 60))
        finally:
            await pool.close()
        return results

    results = asyncio.run(scenario())
    assert all(r.exit_code == 1 and r.died for r in results)
    assert not (tmp_path / "ran" / "raw.a").exists(), "a killed table started anyway"


def running_item(pool: WorkerPool) -> WorkItem:
    (job,) = [slot.job for slot in pool._slots if slot.job is not None]
    assert isinstance(job, WorkItem)
    return job


def test_kill_table_of_a_table_not_running_is_a_no_op(tmp_path):
    pool = WorkerPool(PoolSize(1, 1, "test"), WorkerConfig(workdir=tmp_path))
    assert pool.kill_table(item()) is False
