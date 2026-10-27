# SPDX-License-Identifier: Apache-2.0
"""The event feed: received messages become offers (#124).

Mostly the pure half -- `handle` and `flush_due` against an injected clock --
because the properties are about time and identity: a burst settles into one
offer, a table written constantly is still maintained by the cap, a duplicate is
recognised, and every message is acknowledged exactly once. The async half and
the service wiring get one end-to-end test each.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

from zamboni.events.feed import CAP, QUIET, EventFeed
from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse
from zamboni.scheduler import EVENT, Candidate
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

T0 = datetime(2026, 10, 11, 2, 0, tzinfo=UTC)


def fleet() -> FleetConfig:
    config = TableConfig(
        warehouse="acme",
        namespaces={
            "raw": NamespaceSettings(tables={"events": TableSettings(), "logs": TableSettings()})
        },
    )
    return FleetConfig(warehouses=(FleetWarehouse("acme", CronSchedule("0 2 * * *"), config),))


def payload(table: str = "events", snapshot_id: int = 1, warehouse: str = "acme") -> bytes:
    return json.dumps(
        {
            "specversion": "1.0",
            "type": "writeCompleted",
            "source": "uri:test:host",
            "id": f"{warehouse}/raw/{table}@{snapshot_id}",
            "data": {
                "warehouse": warehouse,
                "namespace": ["raw"],
                "table": table,
                "snapshot_id": snapshot_id,
                "completed_at": "2026-10-11T02:00:00Z",
            },
        }
    ).encode()


class Clock:
    def __init__(self, now: datetime = T0) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class Message:
    def __init__(self, data: bytes) -> None:
        self.payload = data
        self.acked = 0
        self.naked = 0

    async def ack(self) -> None:
        self.acked += 1

    async def nak(self) -> None:
        self.naked += 1


class FakeSource:
    def __init__(self, payloads: list[bytes], *, name: str = "fake", connected: bool = True):
        self.name = name
        self.connected = connected
        self.messages = [Message(p) for p in payloads]

    async def _iterate(self):
        for message in self.messages:
            yield message
            await asyncio.sleep(0)

    def __aiter__(self):
        return self._iterate()


def feed(clock: Clock | None = None, offered: list | None = None, **kw) -> EventFeed:
    offered = offered if offered is not None else []
    return EventFeed(
        FakeSource([]),
        fleet=fleet,
        offer=lambda c: offered.append(c) or True,
        clock=clock or Clock(),
        **kw,
    )


# -- one message in, one decision out --------------------------------------------------


def test_each_kind_of_message_is_counted_once():
    f = feed()
    outcomes = [
        f.handle(payload("events", 1)),
        f.handle(payload("events", 1)),  # the same write again
        f.handle(payload("events", 2)),  # a new write, same table, still quiet
        f.handle(payload("orders", 1)),  # a table the fleet does not manage
        f.handle(payload(warehouse="globex")),  # a warehouse it does not manage
        f.handle(b"{ not json"),
    ]
    assert outcomes == ["pending", "duplicate", "coalesced", "unmapped", "unmapped", "rejected"]
    c = f.counters
    assert (c.received, c.duplicates, c.coalesced, c.unmapped, c.rejected) == (6, 1, 1, 2, 1)


def test_dedup_is_bounded_and_forgets_the_oldest():
    f = feed(dedup_size=2)
    for snapshot in (1, 2, 3):
        f.handle(payload("events", snapshot))
    f.flush_due(T0 + CAP)
    assert f.handle(payload("events", 3)) == "duplicate"
    assert f.handle(payload("events", 1)) == "pending", "evicted: falls to the due-check"


# -- debounce -------------------------------------------------------------------------


def test_a_table_is_offered_after_its_quiet_period():
    clock, offered = Clock(), []
    f = feed(clock, offered)
    f.handle(payload())
    assert f.flush_due(T0 + QUIET - timedelta(seconds=1)) == []
    assert f.flush_due(T0 + QUIET) == [("acme", "raw.events")]
    assert offered == [Candidate("acme", "raw.events", EVENT)]
    assert f.pending == frozenset()


def test_a_burst_settles_into_one_offer():
    clock, offered = Clock(), []
    f = feed(clock, offered)
    for minute in range(4):
        clock.now = T0 + timedelta(minutes=minute)
        f.handle(payload("events", minute + 1))
    assert f.flush_due(T0 + timedelta(minutes=3) + QUIET - timedelta(seconds=1)) == []
    f.flush_due(T0 + timedelta(minutes=3) + QUIET)
    assert len(offered) == 1


def test_a_table_written_constantly_is_still_offered_at_the_cap():
    """Every event restarts the quiet period; the cap is what stops a table
    written every minute from never being maintained."""
    clock, offered = Clock(), []
    f = feed(clock, offered)
    minute = 0
    while not offered:
        clock.now = T0 + timedelta(minutes=minute)
        f.handle(payload("events", minute + 1))
        f.flush_due()
        minute += 1
    assert clock.now == T0 + CAP


def test_two_tables_are_debounced_independently():
    clock, offered = Clock(), []
    f = feed(clock, offered)
    f.handle(payload("events"))
    clock.now = T0 + timedelta(minutes=2)
    f.handle(payload("logs"))
    f.flush_due(T0 + QUIET)
    assert [c.table for c in offered] == ["raw.events"]
    assert f.next_due() == T0 + timedelta(minutes=2) + QUIET


# -- the async half ---------------------------------------------------------------------------


def run_feed(source: FakeSource, **kw) -> tuple[list[Candidate], EventFeed]:
    offered: list[Candidate] = []

    async def scenario():
        stop = asyncio.Event()
        f = EventFeed(
            source,
            fleet=fleet,
            offer=lambda c: offered.append(c) or True,
            quiet=timedelta(0),
            cap=timedelta(0),
            **kw,
        )
        task = asyncio.create_task(f.run(stop))
        while not all(m.acked or m.naked for m in source.messages):
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.2)  # let the flusher see the due tables
        stop.set()
        await asyncio.wait_for(task, 5)
        return f

    return offered, asyncio.run(scenario())


def test_every_handled_message_is_acknowledged_exactly_once():
    """Offered, coalesced, duplicate, unmapped and rejected all ack: a bad
    message is not redelivered forever."""
    source = FakeSource([payload(), payload(), payload("orders"), b"junk"])
    offered, _ = run_feed(source)
    assert [m.acked for m in source.messages] == [1, 1, 1, 1]
    assert [m.naked for m in source.messages] == [0, 0, 0, 0]
    assert offered == [Candidate("acme", "raw.events", EVENT)]


def test_an_internal_error_naks_and_the_feed_keeps_going(monkeypatch):
    from zamboni.events import feed as feed_module

    calls = {"n": 0}
    real = feed_module.map_event

    def flaky(event, fleet):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("bug")
        return real(event, fleet)

    monkeypatch.setattr(feed_module, "map_event", flaky)
    source = FakeSource([payload("events", 1), payload("logs", 1)])
    offered, f = run_feed(source)
    assert (source.messages[0].naked, source.messages[1].acked) == (1, 1)
    assert f.counters.errors == 1
    assert [c.table for c in offered] == ["raw.logs"]


def test_tables_pending_when_the_source_ends_are_still_offered():
    source = FakeSource([payload()])
    offered, _ = run_feed(source)
    assert offered, "the source ended; its pending table was flushed anyway"


# -- telemetry (#181) ----------------------------------------------------------------------


class Recorder:
    """A FeedObserver that records -- or, with ``fail``, raises every time."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.heard: list[tuple[str, float, BaseException | None]] = []
        self.offers: list[Candidate] = []

    def consumed(self, source, outcome, seconds, error):
        if self.fail:
            raise RuntimeError("exporter down")
        self.heard.append((outcome, seconds, error))

    def offered(self, source, candidate):
        if self.fail:
            raise RuntimeError("exporter down")
        self.offers.append(candidate)


def one_of_each() -> list[bytes]:
    """A message for every outcome but `error`, which needs a broken feed."""
    return [
        payload("events", 1),  # pending
        payload("events", 1),  # duplicate
        payload("events", 2),  # coalesced: same table, later snapshot
        payload("events", 3, warehouse="initech"),  # unmapped
        b"{not json",  # rejected
        b"boom",  # error -- see broken()
    ]


def broken(f: EventFeed) -> EventFeed:
    """Make handling `boom` raise, as an internal error would."""
    handle = f.handle

    def raising(data: bytes) -> str:
        if data == b"boom":
            raise KeyError("internal")
        return handle(data)

    f.handle = raising  # type: ignore[method-assign]
    return f


def test_the_observer_hears_every_outcome_once_and_every_offer():
    from zamboni.events.feed import OUTCOMES

    clock, recorder, offered = Clock(), Recorder(), []
    source = FakeSource(one_of_each())
    f = broken(
        EventFeed(
            source,
            fleet=fleet,
            offer=lambda c: offered.append(c) or True,
            clock=clock,
            observer=recorder,
        )
    )

    asyncio.run(f._read())
    clock.now += QUIET
    f.flush_due()

    assert [outcome for outcome, _, _ in recorder.heard] == [
        "pending",
        "duplicate",
        "coalesced",
        "unmapped",
        "rejected",
        "error",
    ]
    assert {outcome for outcome, _, _ in recorder.heard} == set(OUTCOMES)
    assert all(seconds >= 0 for _, seconds, _ in recorder.heard)
    errors = [error for outcome, _, error in recorder.heard if error is not None]
    assert len(errors) == 1 and isinstance(errors[0], KeyError), "only `error` carries one"
    assert recorder.offers == offered == [Candidate("acme", "raw.events", EVENT)]


def test_an_observer_that_raises_changes_nothing_the_feed_does():
    """A reporter can never fail a run; an observer can never fail a feed. Every
    message is still settled exactly once and the table is still offered."""
    clock, offered = Clock(), []
    source = FakeSource(one_of_each())
    f = broken(
        EventFeed(
            source,
            fleet=fleet,
            offer=lambda c: offered.append(c) or True,
            clock=clock,
            observer=Recorder(fail=True),
        )
    )

    asyncio.run(f._read())
    clock.now += QUIET
    f.flush_due()

    assert [(m.acked, m.naked) for m in source.messages] == [(1, 0)] * 5 + [(0, 1)]
    assert f.counters.as_dict() == {
        "received": 5,
        "offered": 1,
        "coalesced": 1,
        "duplicates": 1,
        "unmapped": 1,
        "rejected": 1,
        "errors": 1,
    }
    assert offered == [Candidate("acme", "raw.events", EVENT)]


# -- reuse -----------------------------------------------------------------------------------


def test_the_feed_imports_no_engine_and_no_transport():
    """Shared by `zamboni serve` and the maintenance runtime, and imported by a
    service process that must stay light (#154)."""
    heavy = ("pyiceberg", "duckdb", "pyarrow", "nats", "opentelemetry")
    code = (
        "import sys, json; import zamboni.events.feed; "
        f"print(json.dumps(sorted(m for m in {heavy!r} if m in sys.modules)))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert json.loads(out.stdout) == []


# -- in the service ---------------------------------------------------------------------------


def test_an_event_reaches_a_worker_as_a_write_driven_run(tmp_path):
    """End to end through `Service`: a write.completed message becomes a table
    run with reason=event, the state file counts it, and readiness reflects the
    source's connection."""
    import yaml

    from tests.test_service import FakePool, no_sleep
    from zamboni.pool import PoolSize, WorkerConfig
    from zamboni.service import Service, read_state

    fleet_file = tmp_path / "fleet.yaml"
    fleet_file.write_text(
        yaml.safe_dump(
            {
                "warehouses": [
                    {
                        "name": "acme",
                        "schedule": "0 2 1 1 *",  # never fires during the test
                        "table_config": {"namespaces": {"raw": {"tables": {"events": {}}}}},
                    }
                ]
            }
        )
    )
    pools: list = []
    source = FakeSource([payload()])

    async def scenario():
        svc = Service(
            fleet_file,
            size=PoolSize(1, 1, "test", None),
            worker_config=WorkerConfig(workdir=tmp_path),
            state_path=tmp_path / "state.json",
            run_log=tmp_path / "runs.jsonl",
            pool_factory=lambda size, config: pools.append(FakePool(size, config)) or pools[-1],
            scheduler_wait=no_sleep,
            capability_check=lambda: None,
            events=[source],
        )
        task = asyncio.create_task(svc.run())
        while not svc.feeds:
            await asyncio.sleep(0.01)
        for f in svc.feeds:
            f.quiet = f.cap = timedelta(0)
        while not (pools and pools[0].ran):
            await asyncio.sleep(0.01)
        svc.request_stop()
        return await asyncio.wait_for(task, 5)

    assert asyncio.run(scenario()) == 0
    assert pools[0].ran == ["raw.events"]
    state = read_state(tmp_path / "state.json")
    assert state["nats_connected"] is True
    assert state["events"]["fake"]["offered"] == 1


@pytest.mark.parametrize(("connected", "ready"), [(True, True), (False, False), (None, True)])
def test_readiness_waits_for_a_configured_source_only(connected, ready):
    from zamboni.service import probe

    def alive(pid: int) -> bool:
        return pid == 4242

    state = {"pid": 4242, "phase": "running", "generation": 1, "nats_connected": connected}
    assert probe(state, "readiness", alive=alive)[0] is ready
    assert probe(state | {"last_tick": datetime.now(UTC).isoformat()}, "liveness", alive=alive)[0]


def test_a_failing_source_is_reported_when_it_fails_not_at_the_next_event(caplog):
    class Broken:
        name, connected = "broken", False

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise ConnectionError("broker gone")

    async def scenario():
        stop = asyncio.Event()
        f = EventFeed(Broken(), fleet=fleet, offer=lambda c: True)
        task = asyncio.create_task(f.run(stop))
        for _ in range(100):
            if "broker gone" in caplog.text:
                break
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 5)

    with caplog.at_level("ERROR", logger="zamboni.events.feed"):
        asyncio.run(scenario())
    assert "event source broken failed" in caplog.text
