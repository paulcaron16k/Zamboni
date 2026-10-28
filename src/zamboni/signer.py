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
``AioSession`` s3fs creates every client from, and the filesystem is cached per
signer.

**Why this replaces a library global rather than subclassing.** A catalog names
``FsspecFileIO`` by class path, so a subclass is never used; the only seam is the
``pyiceberg.io.fsspec.SCHEME_TO_FS`` registry. :func:`install` replaces its S3
entries and refuses, naming the entry, unless every one is still the stock
``_s3`` -- the same fail-closed shape as
:func:`zamboni.committer.assert_supported_pyiceberg`. It does nothing against a
PyIceberg that already signs once per signer. Sharing one client between
identical signers relies on fsspec's ``_strip_tokenize_options``, which the
``s3fs>=2024.6.0`` floor's fsspec predates: there each call builds its own
client, still signed and never replaced, but not shared. Listed in
``docs/pyiceberg-private-api.md`` §2.6; delete this module in the change that
adopts a release carrying the fix.
"""

from __future__ import annotations

import functools
import hashlib
from typing import TYPE_CHECKING, Any

from zamboni.committer import UnsupportedPyIceberg

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from pyiceberg.typedef import Properties

__all__ = ["install", "library_signs_once"]

#: The schemes PyIceberg 0.12 routes to ``_s3``.
_S3_SCHEMES = ("s3", "s3a", "s3n")
#: The event and unique id PyIceberg registers its signer under.
_SIGNER_EVENT = "before-sign.s3"
_SIGNER_UNIQUE_ID = 1925


def library_signs_once() -> bool:
    """Whether the installed PyIceberg already registers its S3 signer once per signer.

    True on a fork carrying the fix, false on any released 0.12. Asked of the
    library's behaviour by :func:`zamboni.capabilities.detect`, and cached with
    the other probes; it was a check for the fork's function name until #245.
    """
    from .capabilities import detect

    return detect().s3_signer_registered_once


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

    session = AioSession()
    session.get_component("event_emitter").register_last(
        _SIGNER_EVENT, signer_class(properties), unique_id=_SIGNER_UNIQUE_ID
    )
    signer_key = _signer_cache_key(properties)
    return _signed_s3_filesystem()(**options, session=session, signer_key=signer_key)


def _signer_cache_key(properties: Properties) -> str:
    """A digest identifying the signer built from these properties, without keeping the secrets."""
    items = sorted((key, repr(value)) for key, value in properties.items())
    return hashlib.sha256(repr(items).encode()).hexdigest()


@functools.cache
def _signed_s3_filesystem() -> type[AbstractFileSystem]:
    from s3fs import S3FileSystem

    class SignedS3FileSystem(S3FileSystem):
        """An S3FileSystem whose clients are created from a session carrying a request signer.

        fsspec caches filesystems by their constructor arguments. The session is a
        new object on every call, so it is left out of that key, and ``signer_key``
        stands in for the signer it carries: tables signed through different
        endpoints get separate clients, and identical ones share one.
        """

        _strip_tokenize_options = ("session",)

        def __init__(self, *args: Any, signer_key: str, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)

    return SignedS3FileSystem
