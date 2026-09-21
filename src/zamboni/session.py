# SPDX-License-Identifier: Apache-2.0
"""Catalog + compute-engine wiring.

A :class:`CatalogSession` owns both halves of a maintenance run so they cannot
drift apart: the PyIceberg catalog that reads metadata and commits snapshots,
and the DuckDB connection used to sort and to spill large rewrite groups.

DuckDB is deliberately *not* asked to attach the Iceberg catalog here. Data is
read through PyIceberg's scan (see :mod:`zamboni.backends.duckdb_arrow`), which
works identically for a Lakekeeper REST catalog and for the local SQL catalog
used by the tests, and hands DuckDB a plain Arrow stream. The DuckDB Iceberg
extension is only needed for the equality-delete story, which phase 1 blocks on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self

import duckdb
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

if TYPE_CHECKING:
    from pyiceberg.io import FileIO

logger = logging.getLogger(__name__)


class CredentialUse(StrEnum):
    """Whether Zamboni talks to the object store on its **own** credentials.

    The warehouse system owns the storage. An Iceberg REST catalog is a service
    in front of it, not its owner -- and remote signing exists to constrain
    *external* readers such as BI tools, which is a different trust question
    from the one a maintenance job asks.

    That distinction is load-bearing because the two credential paths a catalog
    can offer are not equivalent (docs/user_guide.md, "Storage credentials"):

    * **STS vending** hands over temporary credentials with a session token, and
      the client signs locally. Every S3 verb works.
    * **Remote signing** hands over nothing; the client POSTs each request to the
      catalog and gets an ``Authorization`` header back. The catalog decides per
      request, and Lakekeeper declines exactly the verbs maintenance is made of.
      Measured against Lakekeeper 0.13.1: a ``ListObjectsV2`` is refused with
      ``SignError ... 400`` **even when its prefix is inside the table's own
      location**, and so is a multi-object ``DELETE``.

    So on a signing warehouse a reader works perfectly while ``expire`` commits
    and frees nothing and ``remove-orphans`` cannot run at all.
    """

    #: Use Zamboni's own storage credentials for every operation, whenever they
    #: are configured. The default: one credential path per run is one place to
    #: look when access fails.
    ALWAYS = "always"
    #: Override only where the catalog's signer refuses -- ``expire``'s deletion
    #: half and ``remove-orphans``. Reads and compaction keep going through
    #: catalog-vended credentials, so the catalog's audit trail still sees them.
    #: Safe rather than merely narrower: the reachable set is compared on
    #: ``bucket/key`` keys (:func:`zamboni.reachable.canonical`), which do not
    #: depend on which endpoint or credential produced them.
    RECLAIM_ONLY = "reclaim-only"
    #: Never override. The catalog governs Zamboni as it governs any client, and
    #: reclaim on a signing warehouse refuses rather than half-working.
    NEVER = "never"


class StorageCredentialsRequired(RuntimeError):
    """A run needs Zamboni's own storage credentials and none are configured.

    Raised before anything is attempted, never part-way through: the alternative
    is a reclaim pass that lists what it can and deletes what it managed to sign,
    which is the one outcome this package will not produce.
    """


@dataclass
class CatalogSession:
    """A catalog plus the DuckDB connection used to move data for it."""

    catalog: Catalog
    con: duckdb.DuckDBPyConnection
    #: Threads DuckDB may use. Kept low by default because a maintenance job
    #: usually runs beside something more important.
    threads: int = 4
    #: Zamboni's own object-store credentials, when it has been given any --
    #: for whichever provider the warehouse lives in.
    storage: StorageSettings | None = None
    #: When to prefer them over whatever the catalog vends. See
    #: :class:`CredentialUse`.
    credential_use: CredentialUse = CredentialUse.ALWAYS

    def table(self, identifier: str, *, reclaiming: bool = False) -> Table:
        """Load a table by ``namespace.name`` identifier.

        ``reclaiming`` says this table is about to be listed or have files
        deleted -- the operations a signing catalog refuses. It only changes
        anything under :attr:`CredentialUse.RECLAIM_ONLY`; under ``always`` the
        override is unconditional and under ``never`` it never happens.
        """
        table = self.catalog.load_table(identifier)
        if not self._should_own(reclaiming):
            return table
        scheme = _location_scheme(table)
        if self.storage is None:
            if _catalog_refuses_storage_access(table):
                raise StorageCredentialsRequired(
                    f"{identifier}: this catalog vends no usable storage credentials -- it "
                    "remote-signs, and its signer refuses the LIST and DELETE that reclaiming "
                    "storage is made of. Zamboni needs the object store's own read/write "
                    f"credentials. {_credential_hint(scheme)} -- in the environment, in "
                    ".env, or under `storage:` in zamboni.yml, which is then held to "
                    ".env's mode rule. Or set ZAMBONI_CREDENTIAL_USE=never to leave the "
                    "catalog in charge and accept that reclaim will not run."
                )
            # The catalog vends credentials that work, so there is nothing to
            # override and nothing to complain about.
            return table
        if scheme and scheme not in self.storage.schemes:
            # Refused rather than attempted. Handing S3 credentials to a `gs://`
            # table builds a FileIO that cannot address it at all, and the
            # failure would surface at the first read or -- worse -- as an empty
            # listing, which orphan removal reads as "everything is unreferenced".
            raise StorageCredentialsRequired(
                f"{identifier}: its location is {scheme}://, but the configured storage "
                f"credentials are for {'/'.join(self.storage.schemes)}. "
                f"{_credential_hint(scheme)}"
            )
        return _with_storage_owner_io(table, self.storage)

    def _should_own(self, reclaiming: bool) -> bool:
        if self.credential_use is CredentialUse.NEVER:
            return False
        if self.credential_use is CredentialUse.ALWAYS:
            return True
        return reclaiming

    def warehouses(self) -> list[str]:
        """Warehouse names this catalog knows about, sorted.

        **Not part of the Iceberg REST specification.** A warehouse is a concept
        the catalog implementation owns, so this asks Lakekeeper's management
        API and returns an empty list against anything that does not expose one.
        Empty means "cannot tell", not "none exist" -- the caller says so rather
        than presenting an empty fleet as fact.
        """
        properties = getattr(self.catalog, "properties", {}) or {}
        uri = properties.get("uri")
        if not uri:
            return []

        import requests

        # `.../catalog` -> `.../management/v1/warehouse`, the sibling endpoint.
        base = uri.rstrip("/")
        base = base[: -len("/catalog")] if base.endswith("/catalog") else base
        try:
            response = requests.get(f"{base}/management/v1/warehouse", timeout=10)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.debug("warehouse listing unavailable at %s: %s", base, exc)
            return []
        return sorted(
            w["name"] for w in payload.get("warehouses", []) if isinstance(w, dict) and "name" in w
        )

    def close(self) -> None:
        try:
            self.con.close()
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("closing DuckDB connection failed: %s", exc)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- constructors ----------------------------------------------------

    @classmethod
    def for_lakekeeper(
        cls,
        *,
        uri: str,
        warehouse: str,
        credential: str | None = None,
        token: str | None = None,
        oauth2_server_uri: str | None = None,
        scope: str | None = None,
        s3: S3Settings | None = None,
        storage: StorageSettings | None = None,
        threads: int = 4,
        credential_use: CredentialUse = CredentialUse.ALWAYS,
        extra: dict[str, Any] | None = None,
    ) -> CatalogSession:
        """Build a session against a Lakekeeper REST catalog backed by MinIO.

        Args:
            uri: Lakekeeper catalog endpoint, e.g. ``http://localhost:8181/catalog``.
            warehouse: Warehouse name configured in Lakekeeper.
            credential: ``client_id:client_secret`` for OAuth2 client credentials.
            token: A bearer token, as an alternative to ``credential``.
            oauth2_server_uri: Token endpoint, when not discoverable from ``uri``.
            scope: OAuth2 scope, e.g. ``lakekeeper``.
            s3: Zamboni's own object-store credentials, for an S3 or S3-compatible
                store. Required to reclaim storage from a warehouse whose catalog
                remote-signs; see :class:`CredentialUse`.
            storage: The same thing for any provider -- :class:`S3Settings`,
                :class:`GCSSettings` or :class:`AzureSettings`. `s3` is the older
                spelling and stays because it is public API that IWS calls; pass
                one or the other, not both.
            credential_use: When to prefer them over the catalog's. Defaults to
                ``always``.
        """
        props: dict[str, Any] = {"type": "rest", "uri": uri, "warehouse": warehouse}
        if credential:
            props["credential"] = credential
        if token:
            props["token"] = token
        if oauth2_server_uri:
            props["oauth2-server-uri"] = oauth2_server_uri
        if scope:
            props["scope"] = scope
        if s3 and storage:
            raise ValueError("pass `s3` or `storage`, not both -- they set the same thing")
        storage = storage or s3
        if storage:
            props.update(storage.as_properties())
        if extra:
            props.update(extra)

        catalog = load_catalog("lakekeeper", **props)
        # The credentials are passed to PyIceberg *and* kept here. Passing them
        # is what a non-signing catalog honours; keeping them is what lets us
        # override a signing one, which PyIceberg offers no way to ask for.
        return cls(
            catalog=catalog,
            con=_new_duckdb(threads),
            threads=threads,
            storage=storage,
            credential_use=credential_use,
        )

    @classmethod
    def for_local(
        cls,
        *,
        warehouse_path: str,
        uri: str | None = None,
        name: str = "local",
        threads: int = 4,
    ) -> CatalogSession:
        """Build a session against a local SQL catalog on a filesystem warehouse.

        Used by the test suite so the whole package is exercisable without
        Docker, MinIO, or Lakekeeper.
        """
        catalog = load_catalog(
            name,
            type="sql",
            uri=uri or f"sqlite:///{warehouse_path.rstrip('/')}/catalog.db",
            warehouse=f"file://{warehouse_path.rstrip('/')}",
        )
        return cls(catalog=catalog, con=_new_duckdb(threads), threads=threads)

    @classmethod
    def from_catalog(cls, catalog: Catalog, *, threads: int = 4) -> CatalogSession:
        """Wrap an already-configured PyIceberg catalog."""
        return cls(catalog=catalog, con=_new_duckdb(threads), threads=threads)


@dataclass(frozen=True)
class S3Settings:
    """MinIO / S3 connection details for PyIceberg's FileIO."""

    endpoint: str
    #: Optional, because a credential-vending catalog supplies them per table.
    #: Pointing at an endpoint while letting Lakekeeper vend the keys is a valid
    #: and useful combination, and passing `None` through to PyIceberg as if it
    #: were a key is not -- so an absent credential is omitted rather than sent.
    access_key_id: str | None = None
    secret_access_key: str | None = None
    region: str = "us-east-1"
    path_style_access: bool = True
    extra: dict[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:
        """Redacted, because the default dataclass repr is not.

        A frozen dataclass prints every field, so `secret_access_key` appeared
        in full anywhere this object reached a formatted string -- a traceback
        rendered with locals, a `logger.debug("%s", settings)`, an error
        aggregator, `pytest --showlocals`. Nothing in this package logs it
        today, which is precisely why it would have gone unnoticed until
        something did.

        The key id is kept: it is an identifier rather than a secret, and it is
        the field you need when the answer is "wrong credentials".
        """
        secret = "***" if self.secret_access_key else None
        return (
            f"S3Settings(endpoint={self.endpoint!r}, "
            f"access_key_id={self.access_key_id!r}, "
            f"secret_access_key={secret!r}, region={self.region!r}, "
            f"path_style_access={self.path_style_access!r}, "
            f"extra={{{len(self.extra)} key(s)}})"
        )

    #: Location schemes these credentials can serve. Ceph RGW, MinIO, Garage and
    #: every other S3-compatible store is here: they speak the S3 API and differ
    #: only in `endpoint`, which is why they need no provider of their own.
    schemes: ClassVar[tuple[str, ...]] = ("s3", "s3a", "s3n")

    def file_io(self) -> FileIO:
        """PyArrow, which is both PyIceberg's preference for `s3` and what this
        has always used. Unlike GCS there is no reason to diverge."""
        from pyiceberg.io.pyarrow import PyArrowFileIO

        return PyArrowFileIO(properties=dict(self.as_properties()))

    def as_properties(self) -> dict[str, str]:
        props = {
            "s3.endpoint": self.endpoint,
            "s3.region": self.region,
            # MinIO serves bucket-in-path, not bucket-as-subdomain.
            "s3.path-style-access": "true" if self.path_style_access else "false",
        }
        if self.access_key_id is not None:
            props["s3.access-key-id"] = self.access_key_id
        if self.secret_access_key is not None:
            props["s3.secret-access-key"] = self.secret_access_key
        props.update(self.extra)
        return props


@dataclass
class GCSSettings:
    """Google Cloud Storage credentials for PyIceberg's FileIO.

    **Routed through `gcsfs`, not pyarrow, and that is a deliberate divergence
    from PyIceberg** -- its `SCHEMA_TO_FILE_IO` maps `gs` to `PyArrowFileIO`
    *only*, with no fsspec fallback. The reason is what each accepts as a
    credential. `PyArrowFileIO._initialize_gcs_fs` reads `gcs.oauth2.token` and
    `gcs.oauth2.token-expires-at`: a bearer token with an expiry, and nothing
    else. A deployment holding a service-account key file has nothing to put
    there, and a token that expires part-way through a maintenance run is the
    same time bomb the catalog-auth path already warns about.

    `gcsfs` takes the same one property and understands three things by it,
    read from its `GoogleCredentials._connect_token`:

    * **A path to an existing file** -- loaded as a service-account file, which
      self-refreshes. This is the key-file case.
    * **`"google_default"`** -- Application Default Credentials, which is what
      GKE Workload Identity provides. A pod configured that way needs no secret
      at all, and this is the expected setting for ExperienceFlow's GKE
      deployments.
    * **Anything else** -- a raw, non-renewable session token. Accepted, and the
      one that can expire mid-run.

    So the property name is PyIceberg's; the range of values it usefully accepts
    is `gcsfs`'s, and that is the whole reason for the divergence.
    """

    #: A key-file path, ``"google_default"``, or a raw token. See the class
    #: docstring -- which of the three it is decides whether it can refresh.
    token: str | None = None
    project_id: str | None = None
    #: An alternate GCS endpoint, for an emulator or a private service host.
    service_host: str | None = None
    default_location: str | None = None
    extra: dict[str, str] = field(default_factory=dict)

    #: Location schemes these credentials can serve.
    schemes: ClassVar[tuple[str, ...]] = ("gs", "gcs")

    def __repr__(self) -> str:
        """Redacted, for the reason :meth:`S3Settings.__repr__` gives.

        `token` is shown only when it is a *name* rather than a secret:
        ``google_default`` and a file path identify a mechanism and a location,
        which are what you need to debug "wrong credentials". A raw token is the
        secret itself and is hidden.
        """
        token = self.token
        if token and token != "google_default" and not Path(token).exists():
            token = "***"
        return (
            f"GCSSettings(token={token!r}, project_id={self.project_id!r}, "
            f"service_host={self.service_host!r}, "
            f"default_location={self.default_location!r}, "
            f"extra={{{len(self.extra)} key(s)}})"
        )

    def as_properties(self) -> dict[str, str]:
        props: dict[str, str] = {}
        if self.token is not None:
            props["gcs.oauth2.token"] = self.token
        if self.project_id is not None:
            props["gcs.project-id"] = self.project_id
        if self.service_host is not None:
            props["gcs.service.host"] = self.service_host
        if self.default_location is not None:
            props["gcs.default-bucket-location"] = self.default_location
        props.update(self.extra)
        return props

    def file_io(self) -> FileIO:
        from pyiceberg.io.fsspec import FsspecFileIO

        _require_backend("gcsfs", "gcs", "GCS")
        return FsspecFileIO(properties=dict(self.as_properties()))


@dataclass
class AzureSettings:
    """Azure Blob / ADLS Gen2 credentials for PyIceberg's FileIO.

    Through `adlfs`, which is what PyIceberg itself prefers for these schemes --
    `SCHEMA_TO_FILE_IO` lists `FsspecFileIO` first for `abfs`/`abfss`, with
    pyarrow as the fallback. So unlike :class:`GCSSettings` this is not a
    divergence; it is the documented order.

    Three credential shapes, and exactly one should be set. All three are static,
    so none has GCS's expiry problem: an account key, a SAS token, or a service
    principal (`client_id` + `client_secret` + `tenant_id`).
    """

    account_name: str | None = None
    account_key: str | None = None
    sas_token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    tenant_id: str | None = None
    extra: dict[str, str] = field(default_factory=dict)

    #: `wasb`/`wasbs` are deliberately absent: PyIceberg maps them in
    #: `SCHEMA_TO_FILE_IO` but `fsspec.SCHEME_TO_FS` has no entry, so they raise
    #: "No registered filesystem" whatever is installed. Naming them would
    #: promise a path that cannot work.
    schemes: ClassVar[tuple[str, ...]] = ("abfs", "abfss")

    def __repr__(self) -> str:
        """Redacted, for the reason :meth:`S3Settings.__repr__` gives."""
        return (
            f"AzureSettings(account_name={self.account_name!r}, "
            f"account_key={'***' if self.account_key else None!r}, "
            f"sas_token={'***' if self.sas_token else None!r}, "
            f"client_id={self.client_id!r}, "
            f"client_secret={'***' if self.client_secret else None!r}, "
            f"tenant_id={self.tenant_id!r}, extra={{{len(self.extra)} key(s)}})"
        )

    def as_properties(self) -> dict[str, str]:
        named = {
            "adls.account-name": self.account_name,
            "adls.account-key": self.account_key,
            "adls.sas-token": self.sas_token,
            "adls.client-id": self.client_id,
            "adls.client-secret": self.client_secret,
            "adls.tenant-id": self.tenant_id,
        }
        props = {key: value for key, value in named.items() if value is not None}
        props.update(self.extra)
        return props

    def file_io(self) -> FileIO:
        from pyiceberg.io.fsspec import FsspecFileIO

        _require_backend("adlfs", "azure", "Azure")
        return FsspecFileIO(properties=dict(self.as_properties()))


#: Any provider's credentials. Named as a union rather than a base class because
#: `S3Settings` predates the other two and is public API -- IWS imports it -- so
#: it keeps its shape, including a required `endpoint` the other two have no use
#: for.
StorageSettings = S3Settings | GCSSettings | AzureSettings


def _require_backend(module: str, extra: str, provider: str) -> None:
    """Refuse now, with the fix, rather than at the first data file.

    `FsspecFileIO` constructs perfectly well without its cloud backend --
    `fsspec` itself is always present, and each cloud's package is imported
    lazily inside the scheme handler. So a missing `gcsfs` or `adlfs` surfaces
    as a bare `ModuleNotFoundError` part-way through a run, naming a module the
    operator never asked for, rather than as "install this extra".

    Checked by asking whether the import *would* work, not by importing: the
    cost of loading a cloud SDK belongs in the run that uses it.
    """
    from importlib.util import find_spec

    try:
        found = find_spec(module) is not None
    except (ImportError, ValueError):
        # `find_spec` raises for a module present without a usable spec, which
        # means the import would not give us a working backend either.
        found = False

    if not found:
        raise StorageCredentialsRequired(
            f"{provider} storage credentials are configured, but the {module!r} backend "
            f"is not installed -- so the FileIO would build and then fail at the first "
            f"data file. Install it: pip install 'iceberg-zamboni[{extra}]' "
            f"(or [cloud] for all three)."
        )


def _location_scheme(table: Table) -> str:
    """The URI scheme of a table's own location, lowercased, or `""` if local.

    Taken from the table rather than from configuration because it is what the
    run will actually address, and because a fleet can hold tables in more than
    one store under one catalog.

    **An unreadable location answers `""`, and the provider check then does not
    run.** That is a deliberate weakening rather than an oversight: refusing a
    run because a guard could not read its input would turn a diagnostic into an
    outage. What it costs is bounded -- credentials that cannot address the
    table fail at the first read, and the one genuinely dangerous outcome, an
    empty listing read as "everything is unreferenced", is already caught by the
    completeness invariant in :mod:`zamboni.orphans` (design.md §6.6). This
    check makes that failure early and legible; it is not what makes it safe.
    """
    from urllib.parse import urlparse

    metadata = getattr(table, "metadata", None)
    return urlparse(getattr(metadata, "location", "") or "").scheme.lower()


#: What to set, per storage scheme. Keyed by scheme because that is what a
#: caller has in hand -- the table said where it lives.
_CREDENTIAL_HINTS: dict[str, str] = {
    "gs": (
        "Set ZAMBONI_GCS_TOKEN to a service-account key file, or to `google_default` "
        "to use the ambient credentials (GKE Workload Identity provides these)"
    ),
    "abfs": (
        "Set ZAMBONI_AZURE_ACCOUNT_NAME with one of ZAMBONI_AZURE_ACCOUNT_KEY, "
        "ZAMBONI_AZURE_SAS_TOKEN, or ZAMBONI_AZURE_CLIENT_ID + "
        "ZAMBONI_AZURE_CLIENT_SECRET + ZAMBONI_AZURE_TENANT_ID"
    ),
    "s3": "Set ZAMBONI_S3_ACCESS_KEY_ID and ZAMBONI_S3_SECRET_ACCESS_KEY",
}
_CREDENTIAL_HINTS["gcs"] = _CREDENTIAL_HINTS["gs"]
_CREDENTIAL_HINTS["abfss"] = _CREDENTIAL_HINTS["abfs"]
_CREDENTIAL_HINTS["s3a"] = _CREDENTIAL_HINTS["s3"]
_CREDENTIAL_HINTS["s3n"] = _CREDENTIAL_HINTS["s3"]


def _credential_hint(scheme: str) -> str:
    """Which settings to reach for, named for the store the table is actually in.

    An unrecognised scheme gets the S3 answer, because every S3-compatible store
    -- Ceph RGW, MinIO, Garage -- arrives under `s3` anyway, and a wrong hint is
    better than none when the alternative is naming all three sets.
    """
    return _CREDENTIAL_HINTS.get(scheme, _CREDENTIAL_HINTS["s3"])


#: Properties a catalog returns when it will remote-sign rather than vend keys.
#: Presence of either means the client holds no credential of its own.
_SIGNING_PROPERTIES = ("s3.signer", "s3.remote-signing-enabled")


def _catalog_refuses_storage_access(table: Table) -> bool:
    """Whether this table's FileIO is driven by a signer rather than a credential.

    Read off the properties the load-table response actually returned, not off
    the warehouse's configuration, because the table is what the run will use.
    ``s3.remote-signing-enabled`` can be present and false, so the value is
    checked rather than the key.
    """
    properties = getattr(table.io, "properties", {}) or {}
    if str(properties.get("s3.remote-signing-enabled", "")).lower() == "true":
        return True
    return bool(properties.get("s3.signer"))


def _with_storage_owner_io(table: Table, storage: StorageSettings) -> Table:
    """The same table, reading and writing on Zamboni's own credentials.

    **Why this is a local override rather than configuration.** PyIceberg builds
    ``table.io`` from the load-table response, and those properties beat anything
    the client configured -- with no supported way to say "I own this storage,
    use my credentials". Measured against Lakekeeper 0.13.1: passing
    ``s3.access-key-id``/``s3.secret-access-key``/``s3.endpoint`` to
    ``for_lakekeeper`` changes nothing at all on a signing warehouse; the signer
    is still selected, the endpoint is still the catalog's, and a LIST still
    fails with ``SignError ... 400``. The keys are discarded silently, which is
    the objectionable part.

    So the FileIO is built from *only* our properties -- no catalog config, no
    signer -- and swapped in. **Delete this function when PyIceberg gains a
    supported precedence for client-supplied storage credentials**, which is
    tracked as ZMBNI-56 with the measurements; the same change should re-examine
    whether `CredentialUse` still needs three values.

    Kept deliberately narrow: it replaces the IO and nothing else. Metadata still
    comes from the catalog, commits still go through it, and the table object is
    otherwise untouched -- so this changes who talks to the object store, not who
    owns the table.
    """
    owned = storage.file_io()
    # `Table` is a pydantic-era object whose fields are not all assignable, so
    # the attribute is set through `object.__setattr__` rather than by
    # reconstructing a table -- reconstruction would need every field the
    # catalog set and would silently drop any this package does not know about.
    object.__setattr__(table, "io", owned)
    return table


def _new_duckdb(threads: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads = {int(threads)}")
    return con
