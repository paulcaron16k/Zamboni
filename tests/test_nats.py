# SPDX-License-Identifier: Apache-2.0
"""The NATS event source (#124): settings, broker names, mode, ack, outage.

Most of it against a fake client, because the properties are about decisions --
which NATS, which stream, what an ack does when the connection has gone. The
`live` tests at the bottom run the same source against a real nats-server and
skip when none answers on ``ZAMBONI_TEST_NATS_URL``; the dev stack runs one.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import ssl
import subprocess
import sys
import uuid
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

LIVE_URL = os.environ.get("ZAMBONI_TEST_NATS_URL", "nats://127.0.0.1:4222")


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
