# SPDX-License-Identifier: Apache-2.0
"""The NATS event source (#124): settings, broker names, mode, ack, outage.

Most of it against a fake client, because the properties are about decisions --
which NATS, which stream, what an ack does when the connection has gone. The
`live` tests at the bottom run the same source against a real nats-server and
skip when none answers on ``ZAMBONI_TEST_NATS_URL``; the dev stack runs one.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import ClassVar
from urllib.parse import urlparse

import pytest
from nats.js.errors import NotFoundError, ServiceUnavailableError

from zamboni.events import nats as nats_source
from zamboni.events.nats import (
    DEFAULT_MAX_DELIVERIES,
    EXTRA,
    NAK_DELAY_S,
    NatsConfigError,
    NatsEventSource,
    NatsSettings,
    connect_options,
    durable_name,
    stream_name,
)

SUBJECT = "dev.iceberg.write.completed"


def settings(**kw) -> NatsSettings:
    return NatsSettings(servers=("nats://broker:4222",), subject=SUBJECT, **kw)


# -- settings ---------------------------------------------------------------------------------


def test_nats_is_off_unless_both_servers_and_subject_are_set():
    assert NatsSettings.from_mapping({}) is None
    assert NatsSettings.from_mapping({"servers": "", "subject": ""}) is None
    for half in ({"servers": "nats://a:4222"}, {"subject": SUBJECT}):
        with pytest.raises(NatsConfigError, match="required when the other is set"):
            NatsSettings.from_mapping(half)


def test_values_arrive_as_strings_from_the_environment():
    s = NatsSettings.from_mapping(
        {
            "servers": "nats://a:4222, nats://b:4222",
            "subject": SUBJECT,
            "jetstream": "false",
            "tls": "true",
            "max_deliveries": "3",
        }
    )
    assert s is not None
    assert (s.servers, s.jetstream, s.tls, s.max_deliveries) == (
        ("nats://a:4222", "nats://b:4222"),
        False,
        True,
        3,
    )


@pytest.mark.parametrize(
    ("value", "mode"),
    [
        *[(None, None), ("", None), ("auto", None), ("AUTO", None)],
        *[(True, True), ("yes", True), (False, False), ("false", False), ("0", False)],
    ],
)
def test_jetstream_is_auto_true_or_false(value, mode):
    raw = {"servers": "nats://a:4222", "subject": SUBJECT, "jetstream": value}
    assert NatsSettings.from_mapping(raw).jetstream is mode


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"jetsream": "true"}, "unknown setting"),
        ({"jetstream": "maybe"}, "auto, true or false"),
        ({"tls": "sometimes"}, "true or false"),
        ({"max_deliveries": "many"}, "whole number"),
        ({"max_deliveries": True}, "whole number"),
        ({"max_deliveries": -1}, "0 \\(unlimited\\) or more"),
        ({"token": "t", "username": "u"}, "not both"),
        ({"subject": "has space"}, "not a NATS subject"),
    ],
)
def test_a_block_that_cannot_mean_what_it_says_is_refused(raw, message):
    with pytest.raises(NatsConfigError, match=message):
        NatsSettings.from_mapping({"servers": "nats://a:4222", "subject": SUBJECT} | raw)


def test_defaults_are_the_shared_ones():
    s = NatsSettings.from_mapping({"servers": "nats://a:4222", "subject": SUBJECT})
    assert (s.jetstream, s.tls, s.max_deliveries, s.stream) == (
        None,
        False,
        DEFAULT_MAX_DELIVERIES,
        "",
    )


def test_secrets_stay_out_of_the_repr():
    s = settings(username="svc", password="hunter2-pw", token="")
    t = settings(token="tok-123456")
    assert "hunter2-pw" not in repr(s) and "tok-123456" not in repr(t)
    assert "svc" in repr(s), "a username is not a secret, and helps debugging"


# -- names on the broker ----------------------------------------------------------------------


@pytest.mark.parametrize("subject", [SUBJECT, "prod.iceberg.>", "a.*.b"])
def test_stream_and_durable_names_are_legal_and_follow_the_subject(subject):
    s = NatsSettings(servers=("nats://a",), subject=subject)
    for name in (stream_name(s), durable_name(s)):
        assert not set(name) & set(".*> \t"), name
    other = NatsSettings(servers=("nats://a",), subject=subject + "x")
    assert durable_name(s) != durable_name(other), "one durable per subject"


def test_a_named_stream_is_used_as_named():
    assert stream_name(settings(stream="EVENTS")) == "EVENTS"


def test_connect_options_carry_one_auth_method_and_a_tls_context():
    assert connect_options(settings(token="t"))["token"] == "t"
    assert "user" not in connect_options(settings(token="t"))
    o = connect_options(settings(username="u", password="p", tls=True, creds_file="/x.creds"))
    assert (o["user"], o["password"], o["user_credentials"]) == ("u", "p", "/x.creds")
    assert isinstance(o["tls"], ssl.SSLContext)
    assert "tls" not in connect_options(settings())
    assert connect_options(settings())["max_reconnect_attempts"] == -1


# -- light import -----------------------------------------------------------------------------


def test_importing_the_module_does_not_import_the_client():
    """settings.py builds NatsSettings from every profile, so this module must
    load without the extra."""
    code = "import sys, zamboni.events.nats; print('nats' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


def test_a_missing_client_names_the_extra(monkeypatch):
    monkeypatch.setattr(nats_source.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(ImportError, match=EXTRA.replace("[", r"\[").replace("]", r"\]")):
        NatsEventSource(settings())


# -- the source against a fake client ---------------------------------------------------------


class FakeMsg:
    def __init__(self, data: bytes, *, fail: bool = False) -> None:
        self.data = data
        self.acks: list[str] = []
        self.delay: float | None = None
        self.fail = fail

    async def ack(self) -> None:
        if self.fail:
            raise ConnectionError("gone")
        self.acks.append("ack")

    async def nak(self, delay: float | None = None) -> None:
        self.acks.append("nak")
        self.delay = delay


class FakePull:
    def __init__(self, batches: list) -> None:
        self.batches = batches

    async def fetch(self, batch: int, timeout: float):
        if not self.batches:
            await asyncio.sleep(3600)
        item = self.batches.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class FakeJS:
    def __init__(self, *, jetstream: bool, streams: dict[str, str] | None = None, batches=()):
        self.jetstream = jetstream
        self.streams = dict(streams or {})  # subject -> stream
        self.asked = 0
        self.added: list[dict] = []
        self.subscribed: dict = {}
        self.pull = FakePull(list(batches))

    async def account_info(self):
        self.asked += 1
        if not self.jetstream:
            raise ServiceUnavailableError()
        return object()

    async def find_stream_name_by_subject(self, subject):
        if subject not in self.streams:
            raise NotFoundError()
        return self.streams[subject]

    async def stream_info(self, name):
        if name not in self.streams.values():
            raise NotFoundError()

    async def add_stream(self, **kw):
        self.added.append(kw)
        self.streams[kw["subjects"][0]] = kw["name"]

    async def pull_subscribe(self, subject, *, durable, stream, config):
        self.subscribed = {"subject": subject, "durable": durable, "stream": stream}
        self.config = config
        return self.pull


class FakeSub:
    def __init__(self, messages) -> None:
        self._messages = messages

    @property
    async def messages(self):
        for m in self._messages:
            yield m


class FakeClient:
    def __init__(self, js: FakeJS, core_messages=()) -> None:
        self.js = js
        self.is_connected = True
        self.connected_url = urlparse("nats://broker:4222")
        self.closed = False
        self.core_messages = list(core_messages)
        self.subscribed: str | None = None

    def jetstream(self):
        return self.js

    async def subscribe(self, subject):
        self.subscribed = subject
        return FakeSub(self.core_messages)

    async def close(self):
        self.closed = True
        self.is_connected = False


def source(client: FakeClient, failures: int = 0, **kw):
    attempts: list[dict] = []
    sleeps: list[float] = []

    async def connect(**options):
        attempts.append(options)
        if len(attempts) <= failures:
            raise OSError("refused")
        return client

    async def sleep(seconds):
        sleeps.append(seconds)

    src = NatsEventSource(settings(**kw), connect=connect, sleep=sleep)
    return src, attempts, sleeps


async def take(src, n: int):
    out = []
    async for message in src:
        out.append(message)
        if len(out) == n:
            break
    return out


def test_auto_on_a_core_server_subscribes_plainly_and_ack_is_a_no_op():
    client = FakeClient(FakeJS(jetstream=False), core_messages=[FakeMsg(b"one")])
    src, _, _ = source(client)

    async def scenario():
        (message,) = await take(src, 1)
        assert src.connected, "subscribed, connected"
        await message.ack()
        await message.nak()
        return message

    message = asyncio.run(scenario())
    assert (src.mode, client.subscribed, message.payload) == ("core", SUBJECT, b"one")
    assert client.closed and not src.connected, "closed when the iterator ends"


def test_auto_on_jetstream_pulls_from_a_durable_and_acks_for_real():
    from nats.js.api import AckPolicy, DeliverPolicy

    msg = FakeMsg(b"one")
    js = FakeJS(jetstream=True, streams={SUBJECT: "ICEBERG"}, batches=[TimeoutError(), [msg]])
    src, _, _ = source(FakeClient(js))

    async def scenario():
        (message,) = await take(src, 1)
        await message.ack()

    asyncio.run(scenario())
    assert src.mode == "jetstream"
    assert js.subscribed == {"subject": SUBJECT, "durable": durable_name(src.settings),
                             "stream": "ICEBERG"}  # fmt: skip
    assert js.added == [], "an existing stream capturing the subject is used, not duplicated"
    assert (js.config.ack_policy, js.config.deliver_policy) == (
        AckPolicy.EXPLICIT,
        DeliverPolicy.NEW,
    )
    assert js.config.max_deliver == DEFAULT_MAX_DELIVERIES
    assert msg.acks == ["ack"]


def test_with_no_stream_capturing_the_subject_one_is_created_and_bounded():
    js = FakeJS(jetstream=True, batches=[[FakeMsg(b"x")]])
    src, _, _ = source(FakeClient(js))
    asyncio.run(take(src, 1))
    (added,) = js.added
    assert (added["name"], added["subjects"]) == (stream_name(src.settings), [SUBJECT])
    assert added["max_age"] > 0, "a stream Zamboni creates is bounded"


def test_unlimited_deliveries_is_spelt_minus_one_to_the_server():
    js = FakeJS(jetstream=True, batches=[[FakeMsg(b"x")]])
    src, _, _ = source(FakeClient(js), max_deliveries=0)
    asyncio.run(take(src, 1))
    assert js.config.max_deliver == -1


def test_jetstream_false_never_asks_the_account():
    js = FakeJS(jetstream=True)
    client = FakeClient(js, core_messages=[FakeMsg(b"x")])
    src, _, _ = source(client, jetstream=False)
    asyncio.run(take(src, 1))
    assert (src.mode, js.asked) == ("core", 0)


def test_jetstream_true_on_a_core_account_is_a_configuration_error():
    client = FakeClient(FakeJS(jetstream=False))
    src, _, _ = source(client, jetstream=True)
    with pytest.raises(NatsConfigError, match="no JetStream"):
        asyncio.run(take(src, 1))
    assert client.closed and not src.connected


def test_a_nak_waits_before_redelivery_and_a_failed_ack_does_not_stop_the_reader():
    naked, broken = FakeMsg(b"a"), FakeMsg(b"b", fail=True)
    js = FakeJS(jetstream=True, streams={SUBJECT: "S"}, batches=[[naked, broken]])
    src, _, _ = source(FakeClient(js))

    async def scenario():
        first, second = await take(src, 2)
        await first.nak()
        await second.ack()  # raises inside; must not escape

    asyncio.run(scenario())
    assert (naked.acks, naked.delay) == (["nak"], NAK_DELAY_S)


def test_a_failing_fetch_is_retried_not_fatal():
    js = FakeJS(
        jetstream=True,
        streams={SUBJECT: "S"},
        batches=[ConnectionError("reconnecting"), ConnectionError("still"), [FakeMsg(b"x")]],
    )
    src, _, sleeps = source(FakeClient(js))
    (message,) = asyncio.run(take(src, 1))
    assert message.payload == b"x"
    assert sleeps == [1.0, 1.0]


def test_a_broker_down_at_start_is_retried_with_backoff_and_reads_as_disconnected(caplog):
    client = FakeClient(FakeJS(jetstream=False), core_messages=[FakeMsg(b"x")])
    src, attempts, sleeps = source(client, failures=3)
    assert not src.connected, "nothing connected yet"
    with caplog.at_level("DEBUG", logger="zamboni.events.nats"):
        asyncio.run(take(src, 1))
    assert len(attempts) == 4
    assert sleeps == [1.0, 2.0, 4.0]
    unreachable = [r.levelname for r in caplog.records if "unreachable" in r.message]
    assert unreachable == ["WARNING", "DEBUG", "DEBUG"], "one warning per outage"


def test_a_dropped_connection_reads_as_disconnected():
    client = FakeClient(FakeJS(jetstream=False), core_messages=[FakeMsg(b"x")])
    src, _, _ = source(client)

    async def scenario():
        iterator = src.__aiter__()
        await iterator.__anext__()
        assert src.connected
        client.is_connected = False
        assert not src.connected
        await iterator.aclose()

    asyncio.run(scenario())


# -- live: a real nats-server -----------------------------------------------------------------

#: The dev stack's NATS (dev-stack/.env.sample, NATS_PORT).
LIVE_URL = os.environ.get("ZAMBONI_TEST_NATS_URL", "nats://127.0.0.1:4232")
DEV_STACK = Path(__file__).resolve().parent.parent / "dev-stack"


@pytest.fixture
def live_url() -> str:
    parsed = urlparse(LIVE_URL)
    try:
        socket.create_connection((parsed.hostname, parsed.port or 4222), timeout=0.5).close()
    except OSError:
        if os.environ.get("ZAMBONI_REQUIRE_DEV_STACK"):
            pytest.fail(f"no NATS at {LIVE_URL} and ZAMBONI_REQUIRE_DEV_STACK is set")
        pytest.skip(f"no NATS at {LIVE_URL}")
    return LIVE_URL


async def live_round_trip(url: str, *, jetstream: bool | None, duplicates: bool):
    """Publish a write.completed-shaped message and read it back through the source.
    A fresh subject per test, so durables and streams never collide."""
    import nats

    subject = f"zamboni.test.{uuid.uuid4().hex}"
    src = NatsEventSource(NatsSettings(servers=(url,), subject=subject, jetstream=jetstream))
    received: list[bytes] = []

    async def consume():
        async for message in src:
            received.append(message.payload)
            await message.ack()

    task = asyncio.create_task(consume())
    for _ in range(100):
        if src.connected or task.done():
            break
        await asyncio.sleep(0.05)
    task.done() and task.result()
    pub = await nats.connect(url)
    payload = json.dumps({"n": 1}).encode()
    msg_id = {"Nats-Msg-Id": str(uuid.uuid4())}
    if src.mode == "jetstream":
        js = pub.jetstream()
        for _ in range(2 if duplicates else 1):
            await js.publish(subject, payload, headers=msg_id)
    else:
        await pub.publish(subject, payload)
    await pub.flush()
    for _ in range(100):
        if received:
            break
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.3)  # long enough for a duplicate to arrive, if one would
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    if src.mode == "jetstream":
        jsm = pub.jetstream()
        await jsm.delete_stream(stream_name(src.settings))
    await pub.close()
    return src, received


def test_live_auto_finds_jetstream_and_the_server_dedups_on_msg_id(live_url):
    src, received = asyncio.run(live_round_trip(live_url, jetstream=None, duplicates=True))
    assert src.mode == "jetstream", "the dev stack's NATS runs with -js"
    assert received == [b'{"n": 1}'], "published twice with one Nats-Msg-Id, delivered once"
    assert not src.connected


def test_live_core_when_told_to(live_url):
    src, received = asyncio.run(live_round_trip(live_url, jetstream=False, duplicates=False))
    assert (src.mode, received) == ("core", [b'{"n": 1}'])


# -- in `zamboni serve` ------------------------------------------------------------------------


def test_a_broker_that_never_answers_leaves_the_schedule_running_and_readiness_down(tmp_path):
    """The service-level half of "killing NATS leaves maintenance working, only
    slower": the schedule maintains the fleet, liveness holds, readiness says
    the configured source is not there."""
    from tests.test_service import service
    from zamboni.service import probe, read_state

    async def refused(**options):
        raise OSError("connection refused")

    async def forever(seconds):
        await asyncio.sleep(0)

    src = NatsEventSource(settings(), connect=refused, sleep=forever)
    pools: list = []

    async def scenario():
        svc = service(tmp_path, "a", pools=pools, events=[src])
        task = asyncio.create_task(svc.run())
        while not (pools and pools[0].ran):
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)  # one more state write
        state = read_state(tmp_path / "serve.state.json")
        svc.request_stop()
        assert await asyncio.wait_for(task, 5) == 0
        return state

    state = asyncio.run(scenario())
    assert set(pools[0].ran) == {"raw.a"}, "the schedule ran the table with no broker"
    assert state["nats_connected"] is False
    assert probe(state, "readiness")[0] is False


def test_serve_without_the_extra_refuses_to_start_naming_it(tmp_path, monkeypatch, capsys):
    from tests.test_service import fleet_file
    from zamboni.cli import main

    monkeypatch.chdir(tmp_path)  # no stray zamboni.yml or .env
    monkeypatch.setenv("ZAMBONI_NATS_SERVERS", "nats://broker:4222")
    monkeypatch.setenv("ZAMBONI_NATS_SUBJECT", SUBJECT)
    monkeypatch.setattr(nats_source.importlib.util, "find_spec", lambda name: None)
    code = main(
        [
            "serve",
            "--fleet",
            str(fleet_file(tmp_path, "a", schedule="0 2 1 1 *")),
            "--state-file",
            str(tmp_path / "state.json"),
            "--pid-file",
            str(tmp_path / "serve.pid"),
        ]
    )
    assert code == 2
    assert EXTRA in capsys.readouterr().err
    assert not (tmp_path / "state.json").exists(), "refused before the service started"


def test_readiness_follows_the_connection_without_waiting_for_a_tick(tmp_path):
    """The scheduler writes state when it ticks, up to a minute apart; a source
    connecting or dropping is written within a second of it happening."""
    from tests.test_service import FakePool, fleet_file
    from zamboni.pool import PoolSize, WorkerConfig
    from zamboni.service import Service, read_state

    class Flipping:
        name, connected = "nats", False

        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(3600)

    source = Flipping()
    state_path = tmp_path / "state.json"

    async def scenario():
        svc = Service(
            fleet_file(tmp_path, "a", schedule="0 2 1 1 *"),  # one tick, then a 60 s sleep
            size=PoolSize(1, 1, "test", None),
            worker_config=WorkerConfig(workdir=tmp_path),
            state_path=state_path,
            pool_factory=lambda size, config: FakePool(size, config),
            capability_check=lambda: None,
            events=[source],
        )
        task = asyncio.create_task(svc.run())
        seen = []
        for connected in (True, False):
            await asyncio.sleep(0.2)
            source.connected = connected
            for _ in range(30):
                await asyncio.sleep(0.1)
                if read_state(state_path).get("nats_connected") is connected:
                    seen.append(connected)
                    break
        svc.request_stop()
        await asyncio.wait_for(task, 5)
        return seen

    assert asyncio.run(scenario()) == [True, False]


# -- live: kill NATS under a real `zamboni serve` ----------------------------------------------


def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args], cwd=DEV_STACK, capture_output=True, text=True, timeout=120
    )


@pytest.fixture
def dev_stack_nats(live_url) -> str:
    """The dev stack's own NATS container, which this test stops and starts. A
    NATS that is not the compose service is skipped: stopping it is not ours."""
    try:
        running = compose("ps", "-q", "nats").stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        running = ""
    if not running:
        if os.environ.get("ZAMBONI_REQUIRE_DEV_STACK"):
            pytest.fail("the dev stack's nats service is not running")
        pytest.skip("the dev stack's nats service is not running")
    return live_url


def test_killing_nats_under_zamboni_serve_costs_readiness_and_nothing_else(
    tmp_path, dev_stack_nats
):
    """docs/event-driven-maintenance.md's test for the whole design: stop the
    broker under a running service and it stays alive, says it is not ready,
    and picks the events up again when the broker returns."""
    import nats

    from tests.test_feed import payload
    from tests.test_service import fleet_file
    from zamboni.cli import main
    from zamboni.service import read_state

    subject = f"zamboni.test.{uuid.uuid4().hex}"
    run = tmp_path / "run"
    state, pid = run / "serve.state.json", run / "serve.pid"
    workdir = tmp_path / "cwd"
    workdir.mkdir()  # no stray zamboni.yml or .env
    env = {
        **os.environ,
        "ZAMBONI_LOCAL_WAREHOUSE": str(tmp_path / "wh"),
        "ZAMBONI_NATS_SERVERS": dev_stack_nats,
        "ZAMBONI_NATS_SUBJECT": subject,
    }
    command = [
        sys.executable,
        "-c",
        "from zamboni.cli import main; raise SystemExit(main())",
        "serve",
        "--fleet",
        str(fleet_file(tmp_path, "events", schedule="0 2 1 1 *")),
        "--state-file",
        str(state),
        "--pid-file",
        str(pid),
    ]
    status = ["service-status", "--state-file", str(state), "--probe"]

    def wait_for(predicate, what: str, timeout: float = 60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.2)
        raise AssertionError(f"timed out waiting for {what}")

    def received() -> int:
        return read_state(state).get("events", {}).get("nats", {}).get("received", 0)

    async def publish(snapshot_id: int) -> None:
        nc = await nats.connect(dev_stack_nats)
        await nc.jetstream().publish(
            subject, payload("events", snapshot_id), headers={"Nats-Msg-Id": uuid.uuid4().hex}
        )
        await nc.close()

    proc = subprocess.Popen(command, cwd=workdir, env=env, stderr=subprocess.PIPE, text=True)
    try:
        wait_for(lambda: state.exists() and main([*status, "readiness"]) == 0, "readiness")
        asyncio.run(publish(1))
        wait_for(lambda: received() == 1, "the first event")

        assert compose("stop", "nats").returncode == 0
        wait_for(lambda: main([*status, "readiness"]) == 1, "readiness to drop", 15)
        assert main([*status, "liveness"]) == 0, "a broker outage is not a reason to restart"
        assert proc.poll() is None

        assert compose("start", "nats").returncode == 0
        wait_for(lambda: main([*status, "readiness"]) == 0, "readiness to return")
        asyncio.run(publish(2))
        wait_for(lambda: received() == 2, "an event after the outage")

        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        compose("up", "-d", "--wait", "nats")

        async def drop_stream() -> None:
            nc = await nats.connect(dev_stack_nats)
            with contextlib.suppress(Exception):
                await nc.jetstream().delete_stream(
                    stream_name(NatsSettings(servers=(dev_stack_nats,), subject=subject))
                )
            await nc.close()

        asyncio.run(drop_stream())


# -- serve --metrics otel (#181) ----------------------------------------------------


class StubService:
    """Stands in for `Service`: records what `serve` built it with, then stops."""

    built: ClassVar[dict] = {}

    def __init__(self, *args, **kwargs):
        StubService.built = kwargs
        self.hangup = asyncio.Event()

    def request_stop(self): ...

    async def run(self) -> int:
        return 0


def serve_with(tmp_path, monkeypatch, *extra: str, nats: bool = True) -> dict:
    """Run `zamboni serve` with the Service stubbed out; what it was built with,
    and the exit code under ``exit``."""
    from tests.test_service import fleet_file
    from zamboni import service
    from zamboni.cli import main

    monkeypatch.chdir(tmp_path)
    if nats:
        monkeypatch.setenv("ZAMBONI_NATS_SERVERS", "nats://broker:4222")
        monkeypatch.setenv("ZAMBONI_NATS_SUBJECT", SUBJECT)
    monkeypatch.setattr(service, "Service", StubService)
    code = main(
        [
            "serve",
            "--fleet",
            str(fleet_file(tmp_path, "a", schedule="0 2 1 1 *")),
            "--state-file",
            str(tmp_path / "state.json"),
            "--pid-file",
            str(tmp_path / "serve.pid"),
            *extra,
        ]
    )
    return {**StubService.built, "exit": code}


def test_serve_reports_the_consumer_only_when_asked(tmp_path, monkeypatch, collector):
    from zamboni.otel import OTelFeedObserver

    assert serve_with(tmp_path, monkeypatch)["feed_observer"] is None
    built = serve_with(tmp_path, monkeypatch, "--metrics", "otel")
    assert built["exit"] == 0
    assert isinstance(built["feed_observer"], OTelFeedObserver)


def test_serve_without_metrics_never_imports_opentelemetry(tmp_path):
    """The service process stays as light as #154 requires: a deployment that
    did not ask for telemetry does not import it."""
    code = f"""
import sys
from pathlib import Path
from typing import ClassVar
from tests.test_nats import serve_with
class MP:
    def chdir(self, p): import os; os.chdir(p)
    def setenv(self, k, v): import os; os.environ[k] = v
    def setattr(self, o, n, v): setattr(o, n, v)
built = serve_with(Path({str(tmp_path)!r}), MP())
assert built["exit"] == 0 and built["feed_observer"] is None
print(sorted(m for m in sys.modules if m.startswith("opentelemetry")))
"""
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
    assert out.stdout.strip() == "[]"


def test_serve_metrics_without_a_nats_block_says_there_is_nothing_to_report(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.delenv("ZAMBONI_NATS_SERVERS", raising=False)
    monkeypatch.delenv("ZAMBONI_NATS_SUBJECT", raising=False)

    built = serve_with(tmp_path, monkeypatch, "--metrics", "otel", nats=False)

    assert built["exit"] == 0 and built["feed_observer"] is None
    assert "no `nats:` block" in capsys.readouterr().err


def attributes_of(point) -> dict[str, str]:
    return {kv.key: kv.value.string_value for kv in point.attributes}


def test_a_live_serve_exports_the_consumer_metrics_to_a_collector(tmp_path, live_url, collector):
    """End to end: a real `zamboni serve --metrics otel` consumes from the dev
    stack's NATS, and on SIGTERM what it measured reaches the collector named
    by OTEL_EXPORTER_OTLP_ENDPOINT -- the convention's names and attributes."""
    import nats

    from tests.test_feed import payload
    from tests.test_service import fleet_file
    from zamboni.service import read_state

    subject = f"zamboni.test.{uuid.uuid4().hex}"
    state = tmp_path / "run" / "serve.state.json"
    workdir = tmp_path / "cwd"
    workdir.mkdir()
    env = {
        **os.environ,  # carries the collector's OTEL_EXPORTER_OTLP_ENDPOINT
        "ZAMBONI_LOCAL_WAREHOUSE": str(tmp_path / "wh"),
        "ZAMBONI_NATS_SERVERS": live_url,
        "ZAMBONI_NATS_SUBJECT": subject,
    }
    command = [
        sys.executable,
        "-c",
        "from zamboni.cli import main; raise SystemExit(main())",
        "serve",
        "--fleet",
        str(fleet_file(tmp_path, "events", schedule="0 2 1 1 *")),
        "--state-file",
        str(state),
        "--pid-file",
        str(tmp_path / "run" / "serve.pid"),
        "--metrics",
        "otel",
    ]

    def received() -> int:
        if not state.exists():
            return -1
        return read_state(state).get("events", {}).get("nats", {}).get("received", 0)

    async def publish() -> None:
        nc = await nats.connect(live_url)
        js = nc.jetstream()
        for data in (payload("events", 1), b"{not json"):
            await js.publish(subject, data, headers={"Nats-Msg-Id": uuid.uuid4().hex})
        await nc.close()

    proc = subprocess.Popen(command, cwd=workdir, env=env, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 60
        while not (state.exists() and read_state(state).get("nats_connected")):
            assert time.monotonic() < deadline, "the service never connected"
            time.sleep(0.2)
        asyncio.run(publish())
        while received() < 2:
            assert time.monotonic() < deadline, "the events never arrived"
            time.sleep(0.2)
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

        async def drop_stream() -> None:
            nc = await nats.connect(live_url)
            with contextlib.suppress(Exception):
                await nc.jetstream().delete_stream(
                    stream_name(NatsSettings(servers=(live_url,), subject=subject))
                )
            await nc.close()

        asyncio.run(drop_stream())

    metrics = {metric.name: metric for _, metric in collector.metrics()}
    assert collector.services() == {"zamboni"}
    consumed = metrics["messaging.client.consumed.messages"].sum.data_points
    by_outcome = {attributes_of(p)["zamboni.event.outcome"]: p.as_int for p in consumed}
    assert by_outcome == {"pending": 1, "rejected": 1}
    for point in consumed:
        attributes = attributes_of(point)
        assert attributes["messaging.system"] == "nats"
        assert attributes["messaging.destination.name"] == subject
        assert attributes["messaging.operation.name"] == "fetch"
        assert attributes["messaging.consumer.group.name"] == durable_name(
            NatsSettings(servers=(live_url,), subject=subject)
        )
    assert metrics["messaging.process.duration"].unit == "s"
