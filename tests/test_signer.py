# SPDX-License-Identifier: Apache-2.0
"""The S3 signer is registered once per signer, never replaced on a shared client (ZMBNI-51).

These mirror the fork's own tests for the same fix: the signer is never absent
from a shared client, each signer gets its own client, identical ones share one,
a recreated client keeps its signer, and no secret is retained. No credentials
and no network -- nothing here sends a request.
"""

from __future__ import annotations

import gc
import threading
import time
import weakref
from typing import TYPE_CHECKING, Any
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
    return [handler for handler in handlers if isinstance(handler, S3V4RestSigner)]


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
