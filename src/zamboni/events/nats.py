# SPDX-License-Identifier: Apache-2.0
"""A NATS :class:`~zamboni.events.feed.EventSource`, core or JetStream.

docs/maintenance-runtime.md §5.3. The only module in Zamboni that talks to a
NATS client, and it imports one only when a source *connects*: importing this
module, or building a :class:`NatsSettings` from a profile, costs nothing and
needs no extra. The client is the ``zamboni[nats]`` extra (nats-py).

**Which NATS.** ``jetstream: auto`` asks the account (``$JS.API.INFO``,
``JetStreamContext.account_info``): an answer means JetStream, a 503 means core.
It asks the account rather than reading the server's connect ``INFO`` because
JetStream can be enabled for the server and not the account -- and because
nats-py keeps ``INFO`` private. Verified against nats-server 2.15 with nats-py
2.16: a ``-js`` server answers, a plain one raises ``ServiceUnavailableError``.

- **JetStream:** a durable pull consumer, at-least-once. Acknowledged once the
  feed has handled a message; naked, with a delay, only on an internal error,
  and redelivered at most ``max_deliveries`` times. There is no dead-letter
  subject: an event is a hint, and one that is never handled costs a table the
  wait for its schedule -- not data.
- **Core:** a plain subscription, at-most-once, ``ack``/``nak`` are no-ops, and
  the schedule is the backstop for anything missed while disconnected.

**What it never does is stop the service.** A broker that is down at start is
retried with backoff, and one that goes away later is reconnected by the client;
meanwhile :attr:`NatsEventSource.connected` is false, readiness fails, liveness
does not, and the schedule keeps maintaining the fleet.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import logging
import re
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any

from ..urls import carries_userinfo

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.aio.msg import Msg
    from nats.js.client import JetStreamContext

logger = logging.getLogger(__name__)

#: The extra that installs the client, named in every error that needs it.
EXTRA = "iceberg-zamboni[nats]"

#: Redelivery bound on JetStream -- the same default as the other services that
#: share these setting names (``SYS_NATS_MAX_DELIVERIES=5``). Applied when the
#: durable consumer is created; an existing consumer keeps its own.
DEFAULT_MAX_DELIVERIES = 5
#: How long a naked message waits before redelivery, so an internal error does
#: not spin on one message. Chosen, not measured.
NAK_DELAY_S = 30.0
#: How many messages one pull asks for, and how long it waits for them. The wait
#: is also how quickly a stop is noticed between batches. Chosen, not measured.
FETCH_BATCH = 32
FETCH_TIMEOUT_S = 5.0
#: Backoff while the broker is unreachable at start: doubling, capped.
CONNECT_BACKOFF_S = (1.0, 30.0)
#: How long a stream Zamboni creates keeps a message. A write.completed older
#: than a week describes a table its schedule has maintained since. Chosen.
STREAM_MAX_AGE_S = 7 * 24 * 3600.0


class NatsConfigError(ValueError):
    """A ``nats:`` block that cannot be used as written."""


def _secret() -> Any:
    return field(default="", repr=False)


@dataclass(frozen=True)
class NatsSettings:
    """Where to subscribe and how to authenticate.

    The names are the ones shared with the other services that use NATS beside
    Zamboni, without their environment prefix, plus ``subject``. NATS is enabled
    only when ``servers`` and ``subject`` are both set (:meth:`from_mapping`).
    """

    servers: tuple[str, ...]
    subject: str
    #: ``None`` is ``auto``: ask the account.
    jetstream: bool | None = None
    username: str = ""
    password: str = _secret()
    token: str = _secret()
    tls: bool = False
    #: A NATS credentials file (JWT + nkey seed). A path, not a secret.
    creds_file: str = ""
    max_deliveries: int = DEFAULT_MAX_DELIVERIES
    #: The JetStream stream capturing ``subject``. Empty: the stream that already
    #: captures it, or one Zamboni creates (:func:`stream_name`).
    stream: str = ""

    def __post_init__(self) -> None:
        if not self.servers or not all(self.servers):
            raise NatsConfigError("nats.servers: at least one server URL is required")
        # Refused here rather than in `from_mapping`, so a caller constructing
        # the settings directly is held to it too. The URL is not repeated in
        # the message: it is the secret.
        for position, server in enumerate(self.servers, start=1):
            if carries_userinfo(server):
                raise NatsConfigError(
                    f"nats.servers: server {position} has credentials written into its "
                    "URL, where they would be logged and shown. Put them in "
                    "nats.username and nats.password, or nats.token, instead"
                )
        if not self.subject or re.search(r"\s", self.subject):
            raise NatsConfigError(f"nats.subject: {self.subject!r} is not a NATS subject")
        if self.token and (self.username or self.password):
            raise NatsConfigError("nats: set token or username/password, not both")
        if self.max_deliveries < 0:
            raise NatsConfigError("nats.max_deliveries: must be 0 (unlimited) or more")

    @classmethod
    def keys(cls) -> frozenset[str]:
        return frozenset(f.name for f in fields(cls))

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> NatsSettings | None:
        """Build from a profile's ``nats:`` block or environment values,
        which arrive as strings. ``None`` when NATS is not configured.

        An unknown key is an error, as everywhere in the profile: a misspelt
        ``jetsream`` silently meaning ``auto`` is the failure an allow-list is for.
        """
        if unknown := sorted(set(raw) - cls.keys()):
            raise NatsConfigError(
                f"nats: unknown setting(s) {', '.join(unknown)}. "
                f"Known: {', '.join(sorted(cls.keys()))}"
            )
        servers = _servers(raw.get("servers"))
        subject = str(raw.get("subject") or "").strip()
        if not servers or not subject:
            if servers or subject:
                missing = "subject" if servers else "servers"
                raise NatsConfigError(f"nats: {missing} is required when the other is set")
            return None
        return cls(
            servers=servers,
            subject=subject,
            jetstream=_jetstream(raw.get("jetstream")),
            username=str(raw.get("username") or ""),
            password=str(raw.get("password") or ""),
            token=str(raw.get("token") or ""),
            tls=_flag("tls", raw.get("tls"), default=False),
            creds_file=str(raw.get("creds_file") or ""),
            max_deliveries=_count("max_deliveries", raw.get("max_deliveries")),
            stream=str(raw.get("stream") or ""),
        )


def _servers(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    items = value.split(",") if isinstance(value, str) else list(value)
    return tuple(s for s in (str(item).strip() for item in items) if s)


_TRUE = {"true", "yes", "on", "1"}
_FALSE = {"false", "no", "off", "0"}


def _flag(key: str, value: Any, *, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE | _FALSE:
        return text in _TRUE
    raise NatsConfigError(f"nats.{key}: expected true or false, got {value!r}")


def _jetstream(value: Any) -> bool | None:
    if value is None or (isinstance(value, str) and value.strip().lower() in ("", "auto")):
        return None
    try:
        return _flag("jetstream", value, default=False)
    except NatsConfigError:
        raise NatsConfigError(
            f"nats.jetstream: expected auto, true or false, got {value!r}"
        ) from None


def _count(key: str, value: Any) -> int:
    if value in (None, ""):
        return DEFAULT_MAX_DELIVERIES
    if isinstance(value, bool):
        raise NatsConfigError(f"nats.{key}: expected a whole number, got {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise NatsConfigError(f"nats.{key}: expected a whole number, got {value!r}") from None


# -- names on the broker -------------------------------------------------------------------


def _sanitize(subject: str) -> str:
    """A subject as a stream or consumer name, which may not contain ``.``,
    ``*``, ``>`` or whitespace."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", subject)


def stream_name(settings: NatsSettings) -> str:
    """The stream Zamboni creates when none captures the subject."""
    return settings.stream or f"zamboni_{_sanitize(settings.subject)}"


def durable_name(settings: NatsSettings) -> str:
    """One durable per subject: a durable has one filter subject, and replicas of
    one deployment share it, so the server hands each message to one of them."""
    return f"zamboni_{_sanitize(settings.subject)}"


def connect_options(settings: NatsSettings) -> dict[str, Any]:
    """``nats.connect`` keyword arguments, without the callbacks."""
    options: dict[str, Any] = {
        "servers": list(settings.servers),
        "name": "zamboni",
        # Retry forever, including the first connection: nats-py's connect()
        # loops on refused connections at ``reconnect_time_wait`` (2 s) when
        # attempts are -1 (``Client.connect``; seen with the broker stopped at
        # start). What it raises instead -- a bad creds file, a bad URL -- is
        # :meth:`NatsEventSource._connect_until_up`'s to retry.
        "allow_reconnect": True,
        "max_reconnect_attempts": -1,
    }
    if settings.token:
        options["token"] = settings.token
    elif settings.username:
        options["user"] = settings.username
        options["password"] = settings.password
    if settings.creds_file:
        options["user_credentials"] = settings.creds_file
    if settings.tls:
        # nats-py wants a context, not a flag; a ``tls://`` server URL also works.
        options["tls"] = ssl.create_default_context()
    return options


def require_client() -> None:
    """Raise with the extra's name when nats-py is not installed."""
    if importlib.util.find_spec("nats") is None:
        raise ImportError(f"a nats: block needs the NATS client: install {EXTRA}")


# -- the source -------------------------------------------------------------------------------


@dataclass
class _Received:
    payload: bytes
    _msg: Msg | None

    async def ack(self) -> None:
        if self._msg is not None:
            await _settle(self._msg.ack(), "ack")

    async def nak(self) -> None:
        if self._msg is not None:
            await _settle(self._msg.nak(delay=NAK_DELAY_S), "nak")


async def _settle(call: Awaitable[None], what: str) -> None:
    """An ack that fails -- the connection dropped between handling and acking --
    means a redelivery, which the feed's dedup absorbs. Never a stopped reader."""
    try:
        await call
    except Exception as exc:
        logger.warning("could not %s a NATS message, it will be redelivered: %r", what, exc)


Connect = Callable[..., Awaitable["Client"]]


async def _nats_connect(**options: Any) -> Client:
    import nats

    return await nats.connect(**options)


class NatsEventSource:
    """Messages from one NATS subject, for an :class:`~zamboni.events.feed.EventFeed`."""

    def __init__(
        self,
        settings: NatsSettings,
        *,
        name: str = "nats",
        connect: Connect | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if connect is None:
            require_client()
        self.settings = settings
        self.name = name
        #: ``"jetstream"`` or ``"core"`` once resolved; for the state file and logs.
        self.mode: str | None = None
        self._connect = connect or _nats_connect
        self._sleep = sleep
        self._nc: Client | None = None
        self._consuming = False
        #: Set while we close the connection ourselves, which is not an outage.
        self._closing = False
        #: Whether this outage has been reported; the client calls back on every
        #: failed attempt, every 2 s, and one warning says it.
        self._reported = False

    @property
    def connected(self) -> bool:
        """Connected *and* subscribed: a connection with no subscription
        delivers nothing, and readiness should say so."""
        return self._consuming and self._nc is not None and self._nc.is_connected

    @property
    def telemetry_attributes(self) -> dict[str, str]:
        """OpenTelemetry messaging attributes describing this source (#181).

        Keys from the messaging semantic conventions, whose attributes are still
        *Development*. ``nats`` is not one of the convention's well-known
        ``messaging.system`` values; it allows a custom one where none applies.
        The subject is low-cardinality -- one per deployment -- which is the
        condition for ``messaging.destination.name``. On JetStream the durable
        consumer is the consumer group; core NATS has none.
        """
        attributes = {
            "messaging.system": "nats",
            "messaging.destination.name": self.settings.subject,
        }
        if self.mode == "jetstream":
            attributes["messaging.consumer.group.name"] = durable_name(self.settings)
        return attributes

    @property
    def receive_operation(self) -> str:
        """The system-specific name of how a message arrived: a JetStream pull
        consumer *fetches* (nats-py's ``fetch``); a core subscription receives."""
        return "fetch" if self.mode == "jetstream" else "receive"

    def __aiter__(self) -> AsyncIterator[_Received]:
        return self._messages()

    async def _messages(self) -> AsyncIterator[_Received]:
        nc = await self._connect_until_up()
        self._nc = nc
        try:
            js = await self._jetstream(nc)
            self.mode = "core" if js is None else "jetstream"
            server = getattr(nc.connected_url, "netloc", None) or ",".join(self.settings.servers)
            logger.info("consuming %s on %s (%s)", self.settings.subject, server, self.mode)
            messages = self._core(nc) if js is None else self._pull(js)
            async for message in messages:
                yield message
        finally:
            self._consuming = False
            self._closing = True
            with contextlib.suppress(Exception):
                await nc.close()

    async def _connect_until_up(self) -> Client:
        delay, ceiling = CONNECT_BACKOFF_S
        options = connect_options(self.settings) | {
            "error_cb": self._on_error,
            "disconnected_cb": self._on_disconnected,
            "reconnected_cb": self._on_reconnected,
        }
        while True:
            try:
                nc = await self._connect(**options)
                self._reported = False
                return nc
            except Exception as exc:
                logger.log(
                    logging.DEBUG if self._reported else logging.WARNING,
                    "NATS at %s unreachable, retrying every %.0fs at most: %r",
                    ",".join(self.settings.servers),
                    ceiling,
                    exc,
                )
                self._reported = True
            await self._sleep(delay)
            delay = min(delay * 2, ceiling)

    async def _jetstream(self, nc: Client) -> JetStreamContext | None:
        """The JetStream context, or ``None`` for core -- per ``jetstream``."""
        if self.settings.jetstream is False:
            return None
        from nats.js.errors import ServiceUnavailableError

        js = nc.jetstream()
        try:
            await js.account_info()
        except ServiceUnavailableError:
            if self.settings.jetstream:
                raise NatsConfigError(
                    "nats.jetstream is true but the account has no JetStream "
                    "($JS.API.INFO answered 503); set it to auto or false"
                ) from None
            return None
        return js

    async def _core(self, nc: Client) -> AsyncIterator[_Received]:
        sub = await nc.subscribe(self.settings.subject)
        self._consuming = True
        async for msg in sub.messages:
            yield _Received(msg.data, None)

    async def _pull(self, js: JetStreamContext) -> AsyncIterator[_Received]:
        from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

        stream = await self._ensure_stream(js)
        config = ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT,
            # A new durable starts at new messages: anything older is a write
            # the schedule covers, and a backlog replayed at start is a storm
            # of offers for no gain. An existing durable resumes where it was.
            deliver_policy=DeliverPolicy.NEW,
            # 0 means unlimited to the operator; to the server that is -1.
            max_deliver=self.settings.max_deliveries or -1,
        )
        sub = await js.pull_subscribe(
            self.settings.subject,
            durable=durable_name(self.settings),
            stream=stream,
            config=config,
        )
        self._consuming = True
        failing = False
        while True:
            try:
                batch = await sub.fetch(FETCH_BATCH, timeout=FETCH_TIMEOUT_S)
            except TimeoutError:  # nothing to fetch; nats-py's subclasses it
                continue
            except Exception as exc:
                # Reconnecting, or a server restarting: the client recovers the
                # connection, this loop only waits for it. Said once per outage.
                if not failing:
                    logger.warning("NATS fetch failing, will retry: %r", exc)
                failing = True
                await self._sleep(1.0)
                continue
            if failing:
                logger.info("NATS fetch recovered")
                failing = False
            for msg in batch:
                yield _Received(msg.data, msg)

    async def _ensure_stream(self, js: JetStreamContext) -> str:
        """The stream to consume from: the named one, the one already capturing
        the subject, or one created for it -- bounded by :data:`STREAM_MAX_AGE_S`."""
        from nats.js.errors import NotFoundError

        if not self.settings.stream:
            with contextlib.suppress(NotFoundError):
                return await js.find_stream_name_by_subject(self.settings.subject)
        name = stream_name(self.settings)
        try:
            await js.stream_info(name)
        except NotFoundError:
            await js.add_stream(
                name=name, subjects=[self.settings.subject], max_age=STREAM_MAX_AGE_S
            )
            logger.info("created JetStream stream %s for %s", name, self.settings.subject)
        return name

    async def _on_error(self, exc: Exception) -> None:
        if self._reported:
            logger.debug("NATS error: %r", exc)
        else:
            self._reported = True
            logger.warning("NATS error, retrying until it answers: %r", exc)

    async def _on_disconnected(self) -> None:
        if not self._closing:
            logger.warning("NATS disconnected; reconnecting")

    async def _on_reconnected(self) -> None:
        self._reported = False
        logger.info("NATS reconnected")
