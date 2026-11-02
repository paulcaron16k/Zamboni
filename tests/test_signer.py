# SPDX-License-Identifier: Apache-2.0
"""The S3 signer is registered once per signer, never replaced on a shared client (ZMBNI-51).

These mirror the fork's own tests for the same fix: the signer is never absent
from a shared client, each signer gets its own client, identical ones share one,
a recreated client keeps its signer, and no secret is retained. No credentials
and no network -- nothing above the #250 section sends a request.

#250's tests mirror the fork's too: signing runs off the event loop, and
connections are shared across tables. The connection tests send requests, to a
loopback stand-in for the sign endpoint and the object store.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import inspect
import json
import logging
import os
import subprocess
import sys
import textwrap
import threading
import time
import uuid
import weakref
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, ClassVar
from unittest import mock

import pytest
from pyiceberg.io import fsspec
from pyiceberg.io.fsspec import S3V4RestSigner

from zamboni import CatalogSession, signer
from zamboni.committer import UnsupportedPyIceberg

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fsspec import AbstractFileSystem
    from pyiceberg.typedef import Properties

pytestmark = pytest.mark.skipif(
    signer.library_signs_once(), reason="the installed PyIceberg signs once per signer itself"
)


@pytest.fixture
def installed() -> Iterator[None]:
    """Install the fix for one test, and put the registry back afterwards."""
    saved = dict(fsspec.SCHEME_TO_FS)
    signer.install()
    yield
    fsspec.SCHEME_TO_FS.clear()
    fsspec.SCHEME_TO_FS.update(saved)


def _s3(properties: Properties) -> AbstractFileSystem:
    """The S3 filesystem PyIceberg's FileIO would build, through the registry."""
    return fsspec.SCHEME_TO_FS["s3"](properties)


def _signer_properties(**extra: str) -> Properties:
    return {
        "s3.signer": "S3V4RestSigner",
        "uri": "http://catalog.invalid",
        "token": "abc",
        "s3.region": "us-east-1",
        **extra,
    }


def _signers(fs: AbstractFileSystem) -> list[S3V4RestSigner]:
    handlers = fs.s3.meta.events._emitter._handlers.prefix_search("before-sign.s3")
    # Unwrapped from the handler that runs each signer off the event loop (#250).
    signers = [getattr(handler, "signer", handler) for handler in handlers]
    return [s for s in signers if isinstance(s, S3V4RestSigner)]


@pytest.mark.usefixtures("installed")
def test_each_table_signs_through_its_own_endpoint():
    """The stock ``_s3`` lets the last table loaded sign for every table sharing its client."""
    first = _s3(_signer_properties(**{"s3.signer.endpoint": "v1/first/sign"}))
    second = _s3(_signer_properties(**{"s3.signer.endpoint": "v1/second/sign"}))

    assert first is not second
    assert [s.properties["s3.signer.endpoint"] for s in _signers(first)] == ["v1/first/sign"]
    assert [s.properties["s3.signer.endpoint"] for s in _signers(second)] == ["v1/second/sign"]


@pytest.mark.usefixtures("installed")
def test_identical_signer_properties_share_one_client():
    first = _s3(_signer_properties())
    second = _s3(_signer_properties())

    assert first is second
    assert len(_signers(first)) == 1


@pytest.mark.usefixtures("installed")
def test_the_signer_stays_installed_while_another_thread_builds_filesystems():
    """The stock ``_s3`` leaves a window with no signer; a request signed in it is sent unsigned.

    The window is two adjacent statements, so it is held open by delaying only
    the re-registration, as the fork's test does: the stock ``_s3`` unregisters,
    then sleeps here, while another thread watches. With the fix nothing touches
    a client's signer after it is created, so the delay is never reached.
    """
    fs = _s3(_signer_properties())
    emitter = fs.s3.meta.events
    real_register_last = emitter.register_last

    def slow_register_last(*args: Any, **kwargs: Any) -> None:
        time.sleep(0.05)
        real_register_last(*args, **kwargs)

    observations: list[bool] = []
    stop = threading.Event()

    def observe() -> None:
        while not stop.is_set():
            observations.append(bool(_signers(fs)))
            time.sleep(0.001)

    watcher = threading.Thread(target=observe, daemon=True)
    watcher.start()
    try:
        with mock.patch.object(emitter, "register_last", slow_register_last):
            for _ in range(5):
                _s3(_signer_properties())
    finally:
        stop.set()
        watcher.join(timeout=5)

    assert observations
    assert all(observations), f"{observations.count(False)} observations found no signer"


@pytest.mark.usefixtures("installed")
def test_a_filesystem_no_table_uses_is_released_with_its_signer():
    """fsspec's instance cache kept every signed filesystem until exit (#246).

    With it went the S3 client, the signer and, through the signer's
    properties, the catalog's auth manager and HTTP session: three sockets for
    every catalog and table, and a `serve` worker builds a catalog per table.
    The client is closed on fsspec's loop thread after the filesystem goes, so
    the signer is waited for rather than expected at once.
    """
    fs = _s3(_signer_properties())
    filesystem = weakref.ref(fs)
    (signer_ref,) = [weakref.ref(s) for s in _signers(fs)]
    del fs

    deadline = time.monotonic() + 5
    while (filesystem() is not None or signer_ref() is not None) and time.monotonic() < deadline:
        gc.collect()
        time.sleep(0.05)

    assert filesystem() is None, "nothing uses the filesystem, but something still holds it"
    assert signer_ref() is None, "the filesystem went, but its signer and the catalog's auth stayed"


@pytest.mark.usefixtures("installed")
def test_a_recreated_client_keeps_its_signer():
    """s3fs recreates its client on refresh and when its connection pool has closed."""
    fs = _s3(_signer_properties())
    fs.connect(refresh=True)

    assert len(_signers(fs)) == 1


@pytest.mark.usefixtures("installed")
def test_no_signer_secret_is_retained():
    fs = _s3(_signer_properties(token="not-a-hex-secret"))

    assert "not-a-hex-secret" not in repr(fs.storage_options)


@pytest.mark.usefixtures("installed")
def test_an_unsigned_filesystem_is_the_stock_one():
    properties: Properties = {"s3.region": "us-east-1"}

    assert _s3(properties) is fsspec._s3(properties)


@pytest.mark.usefixtures("installed")
def test_install_routes_every_s3_scheme_and_is_idempotent():
    signer.install()

    schemes = {fsspec.SCHEME_TO_FS[scheme] for scheme in ("s3", "s3a", "s3n")}
    assert schemes == {signer._s3_signed_once}


def test_install_refuses_an_entry_it_did_not_expect(monkeypatch: pytest.MonkeyPatch):
    """Replacing an unexpected entry would silently discard whatever put it there."""

    def someone_elses_s3(properties: Properties) -> AbstractFileSystem:
        raise NotImplementedError

    # The stock entry, whatever an earlier test's session installed process-wide.
    monkeypatch.setitem(fsspec.SCHEME_TO_FS, "s3", fsspec._s3)
    monkeypatch.setitem(fsspec.SCHEME_TO_FS, "s3a", someone_elses_s3)

    with pytest.raises(UnsupportedPyIceberg, match="s3a='test_install_refuses"):
        signer.install()
    assert fsspec.SCHEME_TO_FS["s3"] is fsspec._s3, "nothing is replaced unless all can be"


def test_every_session_installs_the_fix(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Through the constructors, and a session an integrator builds directly."""
    for build in (
        lambda: CatalogSession.for_local(warehouse_path=str(tmp_path)),
        lambda: CatalogSession(catalog=mock.Mock(), con=mock.Mock()),
    ):
        monkeypatch.setitem(fsspec.SCHEME_TO_FS, "s3", fsspec._s3)

        build()

        assert fsspec.SCHEME_TO_FS["s3"] is signer._s3_signed_once


# --- #250: signing off the event loop ------------------------------------------


class _ScriptedSigner(fsspec.S3RequestSigner):
    """A signer that runs ``script``, set by each test, instead of calling an endpoint."""

    script: Any = None

    def __call__(self, request: object, **_: object) -> None:
        type(self).script(self.properties["s3.signer.endpoint"])


@pytest.fixture
def scripted(installed: None) -> Iterator[type[_ScriptedSigner]]:
    with mock.patch.dict(fsspec.SIGNERS, {"scripted": _ScriptedSigner}):
        yield _ScriptedSigner
    _ScriptedSigner.script = None


def _scripted_filesystem(endpoint: str) -> AbstractFileSystem:
    # Explicit credentials keep botocore from walking its credential chain;
    # the signer replaces AWS signing, so they are never used.
    return _s3(
        {
            "s3.signer": "scripted",
            "s3.signer.endpoint": f"{endpoint}-{uuid.uuid4().hex}",
            "uri": "http://catalog.invalid",
            "s3.endpoint": "http://store.invalid",
            "s3.region": "us-east-1",
            "s3.access-key-id": "unused",
            "s3.secret-access-key": "unused",
        }
    )


def _presign(fs: Any) -> str:
    """Sign a request as botocore signs one it sends, sending nothing.

    Bounded, so a request whose signing outcome is lost fails instead of hanging.
    """
    from fsspec.asyn import sync

    return sync(
        fs.loop,
        fs.s3.generate_presigned_url,
        "put_object",
        Params={"Bucket": "b", "Key": "k"},
        timeout=10,
    )


def test_concurrent_requests_are_signed_concurrently(scripted):
    """Two signers meet at a barrier, so they finish only if they run at once.

    Signed inline on fsspec's loop, as aiobotocore runs a synchronous handler,
    the first waits alone and the barrier breaks.
    """
    barrier = threading.Barrier(2, timeout=5)
    scripted.script = lambda _: barrier.wait()
    first, second = _scripted_filesystem("first"), _scripted_filesystem("second")

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        assert len(list(pool.map(_presign, [first, second]))) == 2


def test_a_signing_error_still_fails_its_request(scripted):
    from pyiceberg.exceptions import SignError

    def refuse(_: str) -> None:
        raise SignError("refused")

    scripted.script = refuse

    with pytest.raises(SignError, match="refused"):
        _presign(_scripted_filesystem("refused"))


def test_pending_signs_wait_on_the_loop_not_as_threads(scripted):
    """A batch starts at most ``_SIGN_THREADS`` signing threads; the rest wait on the loop.

    fsspec fans a batch out up to ``RLIMIT_NOFILE // 8`` requests at once, and a
    thread per pending sign started 500 threads for 500 requests.
    """
    from fsspec.asyn import sync

    peak: list[int] = []

    def count(_: str) -> None:
        peak.append(sum(thread.name == "s3-signer" for thread in threading.enumerate()))
        time.sleep(0.02)

    scripted.script = count
    fs: Any = _scripted_filesystem("batch")
    fs.s3  # noqa: B018 - the client, built before the batch

    async def batch() -> None:
        await asyncio.gather(
            *(
                fs.s3.generate_presigned_url("put_object", Params={"Bucket": "b", "Key": f"k{i}"})
                for i in range(60)
            )
        )

    sync(fs.loop, batch, timeout=30)

    assert len(peak) == 60
    # A finished thread hands its slot on just before it exits, so one or two
    # may briefly overlap the next; without the gate there were one per request.
    assert max(peak) < 2 * signer._SIGN_THREADS, f"{max(peak)} signing threads at once"


def test_an_interrupted_signer_fails_only_its_request(scripted):
    """Re-raised on fsspec's loop, an interrupt would stop the loop every filesystem shares."""

    def interrupt(endpoint: str) -> None:
        if endpoint.startswith("interrupted"):
            raise KeyboardInterrupt

    scripted.script = interrupt

    with pytest.raises(RuntimeError, match="KeyboardInterrupt"):
        _presign(_scripted_filesystem("interrupted"))
    assert _presign(_scripted_filesystem("after")).startswith("http://store.invalid/")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_forked_child_signs_while_its_parent_holds_every_slot(scripted):
    """Slots held by the parent's in-flight signers are never released in a child."""
    scripted.script = lambda _: None
    slots = signer._sign_slots
    for _ in range(signer._SIGN_THREADS):
        slots.acquire()
    try:
        pid = os.fork()
        if pid == 0:  # pragma: no cover - runs in the child
            code = 1
            try:
                worker = threading.Thread(
                    target=lambda: _presign(_scripted_filesystem("child")), daemon=True
                )
                worker.start()
                worker.join(timeout=10)
                code = 1 if worker.is_alive() else 0
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
    finally:
        for _ in range(signer._SIGN_THREADS):
            slots.release()

    assert os.waitstatus_to_exitcode(status) == 0, "the child inherited its parent's held slots"


def test_a_sign_endpoint_that_never_answers_does_not_keep_the_process_alive():
    """The sign POST has no timeout; a joined executor thread would wait on it forever."""
    script = textwrap.dedent(
        """
        import threading, time
        from pyiceberg.io import fsspec
        from fsspec.asyn import sync
        from zamboni import signer

        class Hang(fsspec.S3RequestSigner):
            def __call__(self, request, **_):
                threading.Event().wait()

        signer.install()
        fsspec.SIGNERS["hang"] = Hang
        fs = fsspec.SCHEME_TO_FS["s3"]({"s3.signer": "hang", "uri": "http://catalog.invalid",
            "s3.region": "us-east-1", "s3.endpoint": "http://store.invalid",
            "s3.access-key-id": "unused", "s3.secret-access-key": "unused"})
        threading.Thread(target=lambda: sync(fs.loop, fs.s3.generate_presigned_url,
            "put_object", Params={"Bucket": "b", "Key": "k"}), daemon=True).start()
        time.sleep(0.5)
        print("exiting", flush=True)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30, check=False
    )

    assert result.returncode == 0, result.stderr
    assert "exiting" in result.stdout


# --- #250: connections shared across tables --------------------------------------


class _StandIn(BaseHTTPRequestHandler):
    """A sign endpoint and an object store on one loopback port, recording each client socket."""

    protocol_version = "HTTP/1.1"  # Keep-alive, so a pooled connection is reused.
    sign_sockets: ClassVar[set[int]]
    object_sockets: ClassVar[set[int]]
    closed_sockets: ClassVar[set[int]]
    signed_by: ClassVar[list[str]]

    def finish(self) -> None:
        type(self).closed_sockets.add(self.client_address[1])
        super().finish()

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        """Sign: echo the URI back, as a sign endpoint that adds no headers would."""
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).sign_sockets.add(self.client_address[1])
        type(self).signed_by.append(self.path)
        self._send(json.dumps({"uri": body["uri"], "headers": {}}).encode())

    def do_GET(self) -> None:
        type(self).object_sockets.add(self.client_address[1])
        self._send(b"abc")

    def do_HEAD(self) -> None:
        """s3fs asks for an object's size before it reads it."""
        type(self).object_sockets.add(self.client_address[1])
        self.send_response(200)
        self.send_header("Content-Length", "3")
        self.end_headers()


@pytest.fixture
def stand_in(installed: None) -> Iterator[tuple[str, type[_StandIn]]]:
    """A fresh stand-in, with no pooled connection left from an earlier test.

    The shared pools outlive a test, and a keep-alive connection to an earlier
    stand-in on a reused port would reach that server instead of this one.
    """
    signer._close_connectors()
    signer._signing_session_ = None

    class Handler(_StandIn):
        sign_sockets: ClassVar[set[int]] = set()
        object_sockets: ClassVar[set[int]] = set()
        closed_sockets: ClassVar[set[int]] = set()
        signed_by: ClassVar[list[str]] = []

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", Handler
    finally:
        signer._close_connectors()
        signer._signing_session_ = None
        server.shutdown()
        server.server_close()


def _table_properties(address: str, table: int) -> Properties:
    """What Lakekeeper 0.13.1 vends each table on a signing warehouse: alike but the endpoint."""
    return _signer_properties(
        uri=address,
        **{
            "s3.endpoint": address,
            "s3.signer.endpoint": f"v1/signer/tabular-id/{table}-{uuid.uuid4().hex}/v1/aws/s3/sign",
            "s3.access-key-id": "unused",
            "s3.secret-access-key": "unused",
        },
    )


def _signed_tables(handler: type[_StandIn]) -> list[str]:
    return [path.split("/")[4].split("-")[0] for path in handler.signed_by]


def test_tables_share_connections_but_each_signs_through_its_own_endpoint(stand_in):
    """Each table's client and signer kept their own pools: two sockets per table.

    Five tables read one object each, in turn: a HEAD and a GET, each signed.
    Shared, the sign endpoint and the object store each see one connection.
    """
    address, handler = stand_in
    tables = [_s3(_table_properties(address, i)) for i in range(5)]
    for i, fs in enumerate(tables):
        assert fs.cat_file(f"s3://b/k{i}") == b"abc"

    assert len({id(fs) for fs in tables}) == len(tables), "still a client and signer per table"
    assert _signed_tables(handler) == [str(i) for i in range(5) for _ in range(2)]
    assert (len(handler.sign_sockets), len(handler.object_sockets)) == (1, 1)


def test_releasing_a_table_leaves_the_shared_pool_to_the_others(stand_in):
    """A released table's client closes its session (#246); the shared pool must stay open."""
    address, handler = stand_in
    released = _s3(_table_properties(address, 0))
    kept = _s3(_table_properties(address, 1))
    assert released.cat_file("s3://b/first") == b"abc"
    client_session = next(iter(released.s3._endpoint.http_session._sessions.values()))
    gone = weakref.ref(released)
    del released
    gc.collect()
    assert gone() is None
    # Otherwise this passes if the release closed nothing; s3fs's finalizer
    # schedules the close on its loop.
    deadline = time.monotonic() + 5
    while not client_session.closed and time.monotonic() < deadline:
        time.sleep(0.05)
    assert client_session.closed

    assert kept.cat_file("s3://b/second") == b"abc"
    assert len(handler.object_sockets) == 1


def test_the_shared_pool_covers_one_client_and_the_executor(stand_in):
    """Derived from the declarations it is built from, not a number."""
    from botocore.endpoint import MAX_POOL_CONNECTIONS
    from pyiceberg.utils.concurrent import ExecutorFactory

    address, _ = stand_in
    fs = _s3(_table_properties(address, 0))
    assert fs.cat_file("s3://b/k") == b"abc"
    connector = next(iter(fs.s3._endpoint.http_session._sessions.values())).connector

    assert connector.limit == MAX_POOL_CONNECTIONS + ExecutorFactory.get_or_create()._max_workers


def test_many_threads_across_tables_queue_for_a_small_shared_pool(stand_in, monkeypatch):
    """Sixteen threads over four tables share a pool of two without exceeding it, and all succeed.

    Two, not the real size, which per-table pools would also stay within.
    Queueing for a connection must not deadlock.
    """
    monkeypatch.setattr(signer, "_pool_size", lambda: 2)
    address, handler = stand_in
    tables = [_s3(_table_properties(address, i)) for i in range(4)]
    failures: list[BaseException] = []

    def read(thread: int) -> None:
        try:
            for i in range(5):
                assert tables[thread % 4].cat_file(f"s3://b/{thread}-{i}") == b"abc"
        except BaseException as exc:
            failures.append(exc)

    threads = [threading.Thread(target=read, args=(n,)) for n in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert not failures, failures
    assert len(handler.signed_by) == 16 * 5 * 2
    assert 1 <= len(handler.object_sockets) <= 2
    assert 1 <= len(handler.sign_sockets) <= signer._SIGN_THREADS


def test_an_aiobotocore_without_the_internals_falls_back_to_a_pool_per_table(
    stand_in, monkeypatch, caplog
):
    """With a private name gone, each table keeps its own pool, as before #250, and says why.

    The guard is handed a name this aiobotocore lacks, as if a release renamed
    one; deleting a real one would break aiobotocore's own session too.
    """
    monkeypatch.setattr(
        signer, "_AIOHTTP_SESSION_METHODS", (*signer._AIOHTTP_SESSION_METHODS, "_renamed")
    )
    signer._shared_pool_session.cache_clear()
    try:
        address, handler = stand_in
        with caplog.at_level(logging.WARNING, logger=signer.__name__):
            tables = [_s3(_table_properties(address, i)) for i in range(3)]
        for i, fs in enumerate(tables):
            assert fs.cat_file(f"s3://b/fallback{i}") == b"abc"
    finally:
        signer._shared_pool_session.cache_clear()

    assert "_renamed" in caplog.text
    assert len(handler.object_sockets) == len(tables)


def test_the_shared_pool_is_closed_at_exit():
    """No client owns the shared connector, so nothing closed it and aiohttp reported it."""
    script = textwrap.dedent(
        """
        import json, threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from pyiceberg.io import fsspec
        from zamboni import signer

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *a): pass
            def _send(self, body):
                self.send_response(200); self.send_header("Content-Length", str(len(body)))
                self.end_headers(); self.wfile.write(body)
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                self._send(json.dumps({"uri": body["uri"], "headers": {}}).encode())
            def do_GET(self):
                self._send(b"abc")
            def do_HEAD(self):
                self.send_response(200); self.send_header("Content-Length", "3"); self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        address = f"http://127.0.0.1:{server.server_port}"
        signer.install()
        fs = fsspec.SCHEME_TO_FS["s3"]({"s3.signer": "S3V4RestSigner", "uri": address,
            "token": "abc", "s3.region": "us-east-1", "s3.endpoint": address,
            "s3.access-key-id": "unused", "s3.secret-access-key": "unused"})
        assert fs.cat_file("s3://b/k") == b"abc"
        print("read")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, check=False
    )

    assert "read" in result.stdout, result.stderr
    assert "Unclosed connector" not in result.stderr, result.stderr


@pytest.mark.filterwarnings("ignore::ResourceWarning")  # The abandoned sockets, collected later.
def test_a_fork_abandons_the_parents_connections_without_closing_them(stand_in):
    """Closed in a forked child, the parent's pooled sockets left the epoll set the two share.

    On Linux the parent's next request on one then waited for its read timeout.
    The fork handler runs here in-process, as the child runs it, after every
    table is released, so only the shared pools reference the connector.
    Collecting it -- aiohttp's ``__del__``, called directly, because here the
    loop's keep-alive timer still holds it -- must leave its connection open.
    """
    address, handler = stand_in
    fs = _s3(_table_properties(address, 0))
    assert fs.cat_file("s3://b/k") == b"abc"
    del fs
    gc.collect()
    assert len(handler.object_sockets) == 1
    inherited = signer._built(signer._connectors)
    assert inherited

    # The pooled connections' transports, read before anything can close them.
    transports = [
        proto.transport for c in inherited for conns in c._conns.values() for proto, _ in conns
    ]
    assert transports

    signer._reset_after_fork()
    for connector in inherited:
        connector.__del__()

    # Checked on the client, synchronously: the stand-in sees a close only
    # once its thread reads the EOF.
    assert not any(transport.is_closing() for transport in transports)
    for connector in inherited:  # Abandoned on purpose; this process closes them after all.
        connector._closed = False
        connector._close()


def _run_on_a_new_loop(scenario: Any) -> Any:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(scenario())
    finally:
        signer._connectors.pop(loop, None)
        loop.close()


def test_a_cancelled_request_does_not_cancel_the_connector_others_await(monkeypatch):
    """Cancelled while the shared connector builds, a request leaves the build to the rest."""
    from aiobotocore.httpsession import AIOHTTPSession

    real_create = AIOHTTPSession._create_connector
    builds: list[int] = []

    async def slow_create(self: AIOHTTPSession, proxy_url: str | None) -> Any:
        builds.append(1)
        await asyncio.sleep(0.2)
        connector = real_create(self, proxy_url)  # A coroutine from aiobotocore 3.8 only.
        return await connector if inspect.isawaitable(connector) else connector

    monkeypatch.setattr(AIOHTTPSession, "_create_connector", slow_create)
    session_class: Any = signer._shared_pool_session()

    async def scenario() -> tuple[bool, Any]:
        async with session_class() as first, session_class() as second:
            requester = asyncio.create_task(signer._shared_connector(first, None))
            await asyncio.sleep(0.05)
            waiter = asyncio.create_task(signer._shared_connector(second, None))
            await asyncio.sleep(0.05)
            requester.cancel()
            connector = await waiter
            await connector.close()
            return requester.cancelled(), connector

    requester_cancelled, connector = _run_on_a_new_loop(scenario)

    assert requester_cancelled
    assert connector is not None
    assert len(builds) == 1, "the waiter must share the one build, not start another"


def test_a_failed_connector_build_is_tried_again(monkeypatch):
    """A build that failed, say on a bad CA bundle, fails its waiters and is not kept."""
    from aiobotocore.httpsession import AIOHTTPSession

    real_create = AIOHTTPSession._create_connector
    attempts: list[int] = []

    async def fail_once(self: AIOHTTPSession, proxy_url: str | None) -> Any:
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError("no CA bundle")
        connector = real_create(self, proxy_url)  # A coroutine from aiobotocore 3.8 only.
        return await connector if inspect.isawaitable(connector) else connector

    monkeypatch.setattr(AIOHTTPSession, "_create_connector", fail_once)
    session_class: Any = signer._shared_pool_session()

    async def scenario() -> Any:
        async with session_class() as session:
            with pytest.raises(OSError, match="no CA bundle"):
                await signer._shared_connector(session, None)
            connector = await signer._shared_connector(session, None)
            await connector.close()
            return connector

    assert _run_on_a_new_loop(scenario) is not None
    assert len(attempts) == 2


def test_a_pool_is_shared_with_an_aiobotocore_that_builds_its_connector_synchronously(stand_in):
    """``_create_connector`` became a coroutine in aiobotocore 3.8; 2.19 to 3.7 have a plain method.

    Awaited unconditionally, it failed every signed request on those versions,
    with every name the guard checks present.
    """
    import aiohttp
    from aiobotocore.httpsession import AIOHTTPSession

    def create_synchronously(self: AIOHTTPSession, proxy_url: str | None) -> aiohttp.TCPConnector:
        return aiohttp.TCPConnector(limit=self._max_pool_connections)

    address, handler = stand_in
    with mock.patch.object(AIOHTTPSession, "_create_connector", create_synchronously):
        tables = [_s3(_table_properties(address, i)) for i in range(2)]
        for i, fs in enumerate(tables):
            assert fs.cat_file(f"s3://b/sync{i}") == b"abc"

    assert len(handler.object_sockets) == 1
