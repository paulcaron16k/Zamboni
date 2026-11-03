# SPDX-License-Identifier: Apache-2.0
"""Sign S3 requests once per signer, where PyIceberg 0.12's ``_s3()`` cannot (ZMBNI-51).

Against a catalog that remote-signs, every table goes through ``FsspecFileIO``,
and PyIceberg 0.12's ``_s3()`` attaches the request signer to an S3 client that
fsspec caches and shares across threads and tables. It replaces the signer on
every call by unregistering it and registering the new one, so:

- a request signed between the two goes out with no ``Authorization`` header and
  the store answers ``403 AccessDenied`` -- what ZMBNI-51 measured, on the
  manifest ``PUT`` of a commit;
- the last table loaded signs for every table sharing the client, which breaks
  per-table sign endpoints such as Lakekeeper's;
- a client s3fs recreates, on refresh or when its connection pool has closed,
  has no signer.

The ExperienceFlow fork fixes this in ``_s3()`` itself
(``bug/s3v4restsigner_unregisters_signer_mid_flight``, merged into
``feature/maintenance``). Zamboni's users install the release from PyPI, so the
same behaviour is carried here: the signer is registered once, on the
``AioSession`` s3fs creates every client from, and the filesystem is shared per
signer for as long as a table uses it.

**Signing runs off the event loop (#250).** ``S3V4RestSigner`` signs with a
blocking ``requests`` call, and aiobotocore runs a synchronous ``before-sign``
handler inline on the event loop (``AioHierarchicalEmitter._emit``). s3fs
drives every filesystem from fsspec's one loop thread, so each sign round trip
stalled all object-store I/O in the process: with a 50 ms sign endpoint, GETs
ran at about 8 a second whether 1, 8 or 20 threads issued them. The signer is
registered behind :class:`_SignOffTheLoop`, which runs each call on a daemon
thread of its own and awaits the result.

**Connections are shared across tables (#250).** A signing catalog vends each
table its own sign endpoint -- Lakekeeper 0.13.1's differ only in
``s3.signer.endpoint``, which names the table -- so each table has its own
signer and so its own client. Each client kept its own object-store connection
pool and each signer its own ``requests.Session``: 21 sockets for 10 tables
held, 101 for 50, against a local stand-in. Clients and signers stay per table,
so each request is still signed through its own table's endpoint, but every
client draws its connections from one shared aiohttp connector
(:func:`_shared_pool_session`) and every signer posts through one session.

The fork carries both in ``_s3()`` itself (``bug/s3_signer_blocks_event_loop``
and ``bug/signed_s3_filesystems_share_no_connections``).

**Why this replaces a library global rather than subclassing.** A catalog names
``FsspecFileIO`` by class path, so a subclass is never used; the only seam is the
``pyiceberg.io.fsspec.SCHEME_TO_FS`` registry. :func:`install` replaces its S3
entries and refuses, naming the entry, unless every one is still the stock
``_s3`` -- the same fail-closed shape as
:func:`zamboni.committer.assert_supported_pyiceberg`. It does nothing against a
PyIceberg that already signs once per signer. Listed in
``docs/pyiceberg-private-api.md`` §2.6; delete this module in the change that
adopts a release carrying the fix.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import functools
import hashlib
import inspect
import logging
import os
import threading
import weakref
from typing import TYPE_CHECKING, Any

from zamboni.committer import UnsupportedPyIceberg

if TYPE_CHECKING:
    from collections.abc import Callable

    import aiohttp
    import requests
    from fsspec import AbstractFileSystem
    from pyiceberg.typedef import Properties

logger = logging.getLogger(__name__)

__all__ = ["install", "library_signs_once"]

#: The schemes PyIceberg 0.12 routes to ``_s3``.
_S3_SCHEMES = ("s3", "s3a", "s3n")
#: The event and unique id PyIceberg registers its signer under.
_SIGNER_EVENT = "before-sign.s3"
_SIGNER_UNIQUE_ID = 1925

#: The signed filesystems some FileIO still holds, one per signer. Weak, so a
#: filesystem -- with its S3 client, its signer, and through the signer the
#: catalog's auth manager and HTTP session -- goes when the last table using it
#: does. fsspec's instance cache held them all until exit: three sockets each,
#: one per catalog and table, which a ``serve`` worker builds for every table
#: it runs (#246).
_in_use: weakref.WeakValueDictionary[tuple[int, str], AbstractFileSystem] = (
    weakref.WeakValueDictionary()
)
_in_use_lock = threading.Lock()

#: How many signing calls may run at once, process-wide. PyIceberg's sign POST
#: has no timeout, so a sign endpoint that never answers holds a slot for good;
#: ten of them stall all signing, as one stalled the event loop before #250.
#: Ten is also ``requests``' default pool size, so the shared signing session
#: never opens a connection only to discard it.
_SIGN_THREADS = 10
#: Taken by each signing thread. Replaced in a forked child, where the parent's
#: in-flight calls never release theirs.
_sign_slots = threading.BoundedSemaphore(_SIGN_THREADS)
#: Taken on the loop before a signing thread is started, and given back when it
#: finishes, so a batch of requests waits on the loop rather than as one blocked
#: thread each: fsspec fans a batch out up to ``RLIMIT_NOFILE // 8`` requests at
#: once, and 500 pending signs started 500 threads (found by review).
_sign_gates: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = (
    weakref.WeakKeyDictionary()
)

#: The one ``requests.Session`` every ``S3V4RestSigner`` posts through. Created
#: on first use under :data:`_in_use_lock`.
_signing_session_: requests.Session | None = None

#: The shared object-store connectors, per event loop and per connection
#: settings. Each entry is the task building the connector. Read and written
#: only on the loop it belongs to, so unlocked; in practice there is one loop,
#: fsspec's.
_connectors: dict[asyncio.AbstractEventLoop, dict[tuple[Any, ...], asyncio.Task[Any]]] = {}


def _reset_after_fork() -> None:
    """Give a forked child its own signing slots, signing session and connectors.

    ``serve``'s workers are spawned, not forked, but a library caller may fork.
    The parent's connectors are abandoned, never closed: collected in the child,
    aiohttp's ``BaseConnector.__del__`` closes their sockets, and on Linux that
    unregisters each from the epoll instance the child shares with its parent,
    whose next request on that connection then waits for its read timeout
    (reproduced by target-iceberg's ELT-1281 review in a python:3.12-slim
    container). Marked closed, they are collected with nothing to do.
    """
    global _sign_slots, _signing_session_
    _sign_slots = threading.BoundedSemaphore(_SIGN_THREADS)
    _sign_gates.clear()
    _signing_session_ = None
    for connector in _built(_connectors):
        connector._closed = True
    _connectors.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def _built(
    connectors: dict[asyncio.AbstractEventLoop, dict[tuple[Any, ...], asyncio.Task[Any]]],
) -> list[Any]:
    """The connectors built so far, skipping builds still running or failed."""
    return [
        build.result()
        for builds in connectors.values()
        for build in builds.values()
        if build.done() and not build.cancelled() and build.exception() is None
    ]


@atexit.register
def _close_connectors() -> None:
    """Close the shared connectors, which no client owns and so none closes.

    aiohttp reports one left open as "Unclosed connector" when it is collected.
    While their loop still runs -- fsspec's loop thread is a daemon, alive while
    atexit handlers run -- they are collected and closed on it, since only that
    thread touches its entries, and waited for briefly, so a stuck loop cannot
    hold the exit up.
    """
    for loop, builds in list(_connectors.items()):
        if loop.is_running():

            async def close_all(
                loop: asyncio.AbstractEventLoop = loop, builds: dict[Any, Any] = builds
            ) -> None:
                connectors = _built({loop: builds})
                builds.clear()
                for connector in connectors:
                    with contextlib.suppress(Exception):
                        await connector.close()

            with contextlib.suppress(Exception):
                asyncio.run_coroutine_threadsafe(close_all(), loop).result(timeout=1)
            continue
        connectors = _built({loop: builds})
        builds.clear()
        if not loop.is_closed():
            # ``close`` is a coroutine and there is no loop to run it on;
            # ``_close`` does the work synchronously, as s3fs's own
            # ``close_session`` falls back to.
            for connector in connectors:
                with contextlib.suppress(Exception):
                    connector._close()


def library_signs_once() -> bool:
    """Whether the installed PyIceberg already signs S3 requests as this module does.

    That is, registers each signer once per signer and signs off the event loop.
    True on the fork from #250, false on any released 0.12 and on fork tags
    before #250, which sign inline. Asked of the library's behaviour by
    :func:`zamboni.capabilities.detect`, and cached with the other probes; it
    was a check for the fork's function name until #245.
    """
    from .capabilities import detect

    capabilities = detect()
    return capabilities.s3_signer_registered_once and capabilities.s3_signing_overlaps


def install() -> None:
    """Route PyIceberg's S3 filesystems through :func:`_s3_signed_once`.

    ``FsspecFileIO`` copies ``SCHEME_TO_FS`` when it is constructed, so this must
    run before a table is loaded. Calling it again is harmless.

    Raises:
        UnsupportedPyIceberg: If an S3 entry is neither the stock ``_s3`` nor this
            module's, so whatever replaced it would be silently overridden.
    """
    if library_signs_once():
        return

    from pyiceberg.io import fsspec

    # The stock factory, or this module's own from an earlier call.
    expected = {fsspec._s3, _s3_signed_once}
    foreign = [
        f"{scheme}={getattr(factory, '__qualname__', factory)!r}"
        for scheme in _S3_SCHEMES
        if (factory := fsspec.SCHEME_TO_FS.get(scheme)) not in expected
    ]
    if foreign:
        import pyiceberg

        raise UnsupportedPyIceberg(
            f"pyiceberg {pyiceberg.__version__}: cannot install the S3 signer fix "
            f"(ZMBNI-51), because pyiceberg.io.fsspec.SCHEME_TO_FS no longer maps "
            f"{', '.join(foreign)} to the stock _s3 and replacing it would discard "
            f"whatever put it there"
        )

    for scheme in _S3_SCHEMES:
        fsspec.SCHEME_TO_FS[scheme] = _s3_signed_once


def _s3_signed_once(properties: Properties) -> AbstractFileSystem:
    """Build an S3 filesystem as PyIceberg's ``_s3`` does, with its signer registered once.

    Without a signer this is the stock ``_s3``. With one, the stock ``_s3`` builds
    the filesystem's options from the properties minus the signer, so they track
    the installed release rather than a copy of it, and the signer goes on the
    session the clients are created from.
    """
    from pyiceberg.io import S3_SIGNER, fsspec

    signer_class = fsspec.SIGNERS.get(properties.get(S3_SIGNER, ""))
    if signer_class is None:
        # No signer, or one the stock function refuses by name: either way, its behaviour.
        return fsspec._s3(properties)

    from aiobotocore.session import AioSession
    from botocore import UNSIGNED

    without_signer = {key: value for key, value in properties.items() if key != S3_SIGNER}
    options = dict(fsspec._s3(without_signer).storage_options)
    # The signer replaces AWS signing, exactly as the stock ``_s3`` arranges when it has one.
    options["config_kwargs"] = {**options.get("config_kwargs", {}), "signature_version": UNSIGNED}

    # The pid, as fsspec keys its own cache: a forked child must not reuse a
    # client bound to its parent's event-loop thread. The digest covers the
    # auth manager by its repr, which carries its address; that cannot be
    # reused while the filesystem lives, because the filesystem's signer holds it.
    key = (os.getpid(), _signer_cache_key(properties))
    with _in_use_lock:
        if (fs := _in_use.get(key)) is not None:
            return fs
        if (shared_pool := _shared_pool_session()) is not None:
            options["config_kwargs"] = {**options["config_kwargs"], "http_session_cls": shared_pool}
        request_signer = signer_class(properties)
        if isinstance(request_signer, fsspec.S3V4RestSigner):
            request_signer._session = _signing_session()
        session = AioSession()
        session.get_component("event_emitter").register_last(
            _SIGNER_EVENT, _SignOffTheLoop(request_signer), unique_id=_SIGNER_UNIQUE_ID
        )
        fs = _signed_s3_filesystem()(**options, session=session)
        _in_use[key] = fs
        return fs


class _SignOffTheLoop:
    """A ``before-sign`` handler that runs a synchronous signer on a thread of its own.

    aiobotocore awaits whatever a handler returns, so the loop serves other
    requests while the signer waits on its endpoint. An exception the signer
    raises is re-raised to the request, which is then not sent.

    **A daemon thread per call, not a ``ThreadPoolExecutor``.** An executor's
    workers are joined at exit (``concurrent.futures.thread._python_exit``) and
    the sign POST has no timeout, so a sign endpoint that never answered would
    keep the process from exiting; fsspec's loop thread, where signing ran
    before, is a daemon. target-iceberg's ELT-1283 review measured exactly that:
    3 s to exit with the signer inline, still running at 15 s on an executor.
    """

    def __init__(self, signer: Callable[..., None]) -> None:
        self.signer = signer

    async def __call__(self, **kwargs: Any) -> None:
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()
        slots = _sign_slots
        if (gate := _sign_gates.get(loop)) is None:
            gate = _sign_gates[loop] = asyncio.Semaphore(_SIGN_THREADS)
        await gate.acquire()

        def sign() -> None:
            outcome: BaseException | None = None
            try:
                with slots:
                    self.signer(**kwargs)
            except Exception as exc:
                outcome = exc
            except BaseException as exc:
                # Re-raised on fsspec's loop, an interrupt would stop the loop
                # every filesystem shares; dropped, the request would wait
                # forever. So it fails just this request.
                outcome = RuntimeError(f"S3 request signer raised {exc!r}")
                outcome.__cause__ = exc
            if isinstance(outcome, StopIteration):  # A future refuses one as its exception.
                outcome = RuntimeError(f"S3 request signer raised {outcome!r}")
            with contextlib.suppress(RuntimeError):  # The loop closed: nobody is waiting.
                loop.call_soon_threadsafe(_settle, done, outcome, gate)

        try:
            threading.Thread(target=sign, name="s3-signer", daemon=True).start()
        except BaseException:
            gate.release()
            raise
        await done


def _settle(
    done: asyncio.Future[None], outcome: BaseException | None, gate: asyncio.Semaphore
) -> None:
    """Hand a signing thread's outcome to the request awaiting it, unless that was cancelled."""
    gate.release()  # Here, not when the request ends: a cancelled request's signer still runs.
    if done.done():
        return
    if outcome is None:
        done.set_result(None)
    else:
        done.set_exception(outcome)


def _signing_session() -> requests.Session:
    """The session every signer posts through. Call under :data:`_in_use_lock`.

    A signer adds its own ``Authorization`` header to each request, so the
    session holds no credential of its own. It keeps cookies a sign endpoint
    sets, as each signer's own session did, and replays them only to the domain
    that set them.
    """
    global _signing_session_
    if _signing_session_ is None:
        import requests
        from requests.adapters import HTTPAdapter

        session = requests.Session()
        adapter = HTTPAdapter(pool_maxsize=_SIGN_THREADS)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _signing_session_ = session
    return _signing_session_


def _pool_size() -> int:
    """How many object-store connections the shared pool keeps.

    As many as one client kept before -- botocore's ``MAX_POOL_CONNECTIONS``,
    which PyIceberg's ``_s3`` never overrides -- for the caller's own threads,
    which covers the scan's read-ahead (``max_read_ahead_files``, 8 by default),
    plus one per worker of PyIceberg's executor, which reads manifests and
    writes data files in parallel. A request beyond it waits for a connection
    rather than failing. The same rule as the fork's ``_shared_pool_size``.
    """
    from botocore.endpoint import MAX_POOL_CONNECTIONS
    from pyiceberg.utils.concurrent import ExecutorFactory

    # Typed as an ``Executor``, built as a ``ThreadPoolExecutor``.
    return MAX_POOL_CONNECTIONS + getattr(ExecutorFactory.get_or_create(), "_max_workers", 0)


#: What :func:`_shared_pool_session`'s subclass overrides in, or reads from,
#: aiobotocore's ``AIOHTTPSession``. Private, and aiobotocore floats with s3fs,
#: so they are checked before the subclass is used.
_AIOHTTP_SESSION_METHODS = ("_get_session", "_create_connector")
_AIOHTTP_SESSION_ATTRIBUTES = (
    "_sessions",
    "_proxy_config",
    "_exit_stack",
    "_timeout",
    "_verify",
    "_cert_file",
    "_key_file",
    "_connector_args",
    "_max_pool_connections",
)


def _missing_internals() -> list[str]:
    """The aiobotocore names the shared pool needs that the installed version lacks."""
    from aiobotocore.config import AioConfig
    from aiobotocore.httpsession import AIOHTTPSession

    accepts_session_cls = "http_session_cls" in inspect.signature(AioConfig.__init__).parameters
    missing = [] if accepts_session_cls else ["AioConfig(http_session_cls)"]
    missing += [name for name in _AIOHTTP_SESSION_METHODS if not hasattr(AIOHTTPSession, name)]
    probe = AIOHTTPSession()
    return missing + [name for name in _AIOHTTP_SESSION_ATTRIBUTES if not hasattr(probe, name)]


@functools.cache
def _shared_pool_session() -> type | None:
    """aiobotocore's HTTP session class, drawing on a connector every client shares.

    aiobotocore builds one aiohttp connector per client
    (``AIOHTTPSession._get_session``). This keeps a client's own
    ``aiohttp.ClientSession``, which holds no sockets, but hands it the shared
    connector without ownership, so closing the client -- as releasing a table's
    filesystem does (#246) -- leaves the pool open for every other table.
    Connectors are shared per event loop and per connection settings (TLS
    verification, client certificate, proxy, connector arguments), so a client
    never borrows a connection made under different ones.

    None, with a warning, when the installed aiobotocore lacks a name the
    subclass relies on: each client then keeps its own pool, as before #250 --
    more sockets, but correct, where a subclass built on a renamed internal
    could fail every request. Listed in ``docs/pyiceberg-private-api.md`` §2.6.
    """
    import aiohttp
    from aiobotocore.httpsession import AIOHTTPSession

    if missing := _missing_internals():
        logger.warning(
            "aiobotocore lacks %s, so each signed S3 filesystem keeps its own connection pool",
            ", ".join(missing),
        )
        return None

    class SharedPoolAIOHTTPSession(AIOHTTPSession):
        async def _get_session(self, proxy_url: str | None) -> aiohttp.ClientSession:
            # Set by ``__aenter__`` and None once closed, as aiobotocore's own
            # ``_get_session`` assumes.
            sessions: dict[str | None, aiohttp.ClientSession] = self._sessions
            if session := sessions.get(proxy_url):
                return session
            # Held across creation where aiobotocore has it (3.9.1 on), as its
            # own ``_get_session`` holds it, so closing the client waits for a
            # session being created instead of leaving it unclosed.
            lock: contextlib.AbstractAsyncContextManager[Any] = (
                getattr(self, "_sessions_lock", None) or contextlib.nullcontext()
            )
            async with lock:
                # Read again under the lock: a ``close`` that held it emptied
                # the old dict and set ``_sessions`` to None, so a request
                # queued behind it built a session on the closed client that
                # nothing would close (#260). As in aiobotocore's own, such a
                # request now fails instead.
                sessions = self._sessions
                if not (session := sessions.get(proxy_url)):
                    session = await self._exit_stack.enter_async_context(
                        aiohttp.ClientSession(
                            connector=await _shared_connector(self, proxy_url),
                            connector_owner=False,
                            timeout=self._timeout,
                            skip_auto_headers={"CONTENT-TYPE"},
                            auto_decompress=False,
                        )
                    )
                    sessions[proxy_url] = session
            return session

    return SharedPoolAIOHTTPSession


async def _shared_connector(session: Any, proxy_url: str | None) -> Any:
    """The connector shared under this session's settings on the running loop, built once.

    The build is a task of its own, which concurrent first requests all await:
    shielded, so a cancelled request never cancels it for the others, and
    forgotten if it fails, so a later request tries again.
    """
    loop = asyncio.get_running_loop()
    builds = _connectors.setdefault(loop, {})
    key = (
        session._verify,
        session._cert_file,
        session._key_file,
        proxy_url,
        repr(sorted(session._proxy_config.settings.items())),
        repr(sorted((session._connector_args or {}).items())),
    )
    if (build := builds.get(key)) is None:
        # The shared pool's limit replaces the client's own ``max_pool_connections``.
        session._max_pool_connections = _pool_size()
        build = builds[key] = loop.create_task(_build_connector(session, proxy_url))
        build.add_done_callback(functools.partial(_forget_failed_build, builds, key))
    return await asyncio.shield(build)


async def _build_connector(session: Any, proxy_url: str | None) -> Any:
    # A coroutine from aiobotocore 3.8, which builds TLS contexts on a thread; a
    # plain method before, back to 2.19, where awaiting it unconditionally
    # failed every signed request (found by review).
    connector = session._create_connector(proxy_url)
    return await connector if inspect.isawaitable(connector) else connector


def _forget_failed_build(
    builds: dict[tuple[Any, ...], asyncio.Task[Any]], key: tuple[Any, ...], build: asyncio.Task[Any]
) -> None:
    # ``exception()`` also marks a failure retrieved: every waiter is handed it.
    if (build.cancelled() or build.exception() is not None) and builds.get(key) is build:
        del builds[key]


def _signer_cache_key(properties: Properties) -> str:
    """A digest identifying the signer built from these properties, without keeping the secrets."""
    items = sorted((key, repr(value)) for key, value in properties.items())
    return hashlib.sha256(repr(items).encode()).hexdigest()


@functools.cache
def _signed_s3_filesystem() -> type[AbstractFileSystem]:
    from s3fs import S3FileSystem

    class SignedS3FileSystem(S3FileSystem):
        """An S3FileSystem whose clients are created from a session carrying a request signer.

        Kept out of fsspec's instance cache, which holds every filesystem for
        the life of the process; :data:`_in_use` shares them instead (#246).
        """

        cachable = False

    return SignedS3FileSystem
