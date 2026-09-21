# SPDX-License-Identifier: Apache-2.0
"""Whose object-store credentials a run uses (ZMBNI-30).

The warehouse system owns the storage; an Iceberg REST catalog is a service in
front of it. Remote signing exists to constrain external readers, and it does
that by declining to sign the verbs maintenance is made of -- so a maintenance
tool governed by it can commit an expiry and free nothing.

These are the unit half: the decision table, the refusal, and that nothing is
overridden when nothing needs to be. The half that proves it against a live
remote-signing warehouse is in `test_dev_stack.py`, because only a real
Lakekeeper returns the properties that select a signer.
"""

from __future__ import annotations

import pytest

from zamboni.maintainers import Operation
from zamboni.session import (
    CatalogSession,
    CredentialUse,
    S3Settings,
    StorageCredentialsRequired,
    _catalog_refuses_storage_access,
)

CREDS = S3Settings(endpoint="http://minio:9000", access_key_id="k", secret_access_key="s")


class FakeIO:
    def __init__(self, **properties):
        self.properties = properties


class FakeMetadata:
    def __init__(self, location):
        self.location = location


class FakeTable:
    """A table that knows where it lives.

    The location is not decoration: `CatalogSession.table` reads its scheme to
    decide whether the configured credentials can address this store at all, so
    a fake without one exercises a different path than production takes.
    """

    def __init__(self, location="s3://warehouse/db/t", **properties):
        self.io = FakeIO(**properties)
        self.metadata = FakeMetadata(location)


class FakeCatalog:
    """Hands back a table whose FileIO carries the properties a catalog returned."""

    def __init__(self, location="s3://warehouse/db/t", **properties):
        self._location = location
        self._properties = properties

    def load_table(self, identifier):
        return FakeTable(location=self._location, **self._properties)


SIGNING = {"s3.signer": "S3V4RestSigner", "s3.remote-signing-enabled": "true"}
VENDED = {"s3.access-key-id": "vended", "s3.secret-access-key": "x", "s3.session-token": "t"}


def session(properties, storage, mode):
    return CatalogSession(
        catalog=FakeCatalog(**properties), con=None, storage=storage, credential_use=mode
    )


# -- which catalogs are refusing ------------------------------------------


@pytest.mark.parametrize(
    ("properties", "refuses"),
    [
        (SIGNING, True),
        ({"s3.signer": "S3V4RestSigner"}, True),
        # Present and false is not the same as present: Lakekeeper returns the
        # key either way, so the value has to be read rather than the key.
        ({"s3.remote-signing-enabled": "false"}, False),
        (VENDED, False),
        ({}, False),
    ],
)
def test_a_signing_catalog_is_recognised_from_what_it_returned(properties, refuses):
    assert _catalog_refuses_storage_access(FakeTable(**properties)) is refuses


# -- the decision table ---------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "reclaiming", "overridden"),
    [
        (CredentialUse.ALWAYS, True, True),
        (CredentialUse.ALWAYS, False, True),
        (CredentialUse.RECLAIM_ONLY, True, True),
        # The whole point of the middle setting: a read still goes through the
        # catalog, so its audit trail still sees it.
        (CredentialUse.RECLAIM_ONLY, False, False),
        (CredentialUse.NEVER, True, False),
        (CredentialUse.NEVER, False, False),
    ],
)
def test_when_zamboni_uses_its_own_credentials(mode, reclaiming, overridden):
    table = session(SIGNING, CREDS, mode).table("db.t", reclaiming=reclaiming)

    is_owned = type(table.io).__name__ == "PyArrowFileIO"
    assert is_owned is overridden, (
        f"{mode.value}, reclaiming={reclaiming}: expected "
        f"{'Zamboni' if overridden else 'the catalog'} to own the FileIO"
    )


def test_the_override_uses_our_properties_and_drops_the_signer():
    """Built from our settings alone, not merged over the catalog's.

    Merging would keep `s3.signer`, and a FileIO that still has a signer is the
    thing that does not work -- which is the entire defect.
    """
    table = session(SIGNING, CREDS, CredentialUse.ALWAYS).table("db.t", reclaiming=True)

    assert table.io.properties["s3.access-key-id"] == "k"
    assert table.io.properties["s3.endpoint"] == "http://minio:9000"
    assert "s3.signer" not in table.io.properties
    assert "s3.remote-signing-enabled" not in table.io.properties


# -- the requirement ------------------------------------------------------


@pytest.mark.parametrize("mode", [CredentialUse.ALWAYS, CredentialUse.RECLAIM_ONLY])
def test_a_signing_catalog_with_no_credentials_refuses_before_it_starts(mode):
    """Not part-way through. A reclaim pass that lists what it can and deletes
    what it managed to sign is the one outcome this package will not produce."""
    with pytest.raises(StorageCredentialsRequired) as caught:
        session(SIGNING, None, mode).table("db.t", reclaiming=True)

    message = str(caught.value)
    assert "db.t" in message, "the refusal must name the table"
    assert "ZAMBONI_S3_ACCESS_KEY_ID" in message, "and say what to set"
    assert "ZAMBONI_CREDENTIAL_USE=never" in message, "and name the way to opt out"


def test_a_vending_catalog_with_no_credentials_is_left_alone():
    """The STS path needs no credentials of ours and must not be made to.

    Refusing here would break every deployment that works today, which is why
    the check is on what the catalog returned rather than on whether we have
    keys.
    """
    table = session(VENDED, None, CredentialUse.ALWAYS).table("db.t", reclaiming=True)
    assert type(table.io).__name__ == "FakeIO", "the catalog's own FileIO survives untouched"


def test_never_does_not_refuse_even_on_a_signing_catalog():
    """`never` is the operator saying the catalog governs Zamboni too. That is a
    legitimate choice and must not raise -- the run fails later, at the signer,
    which is the outcome they asked for."""
    table = session(SIGNING, None, CredentialUse.NEVER).table("db.t", reclaiming=True)
    assert type(table.io).__name__ == "FakeIO"


# -- providers other than S3 (ZMBNI-97) -----------------------------------
#
# IWS provisions every warehouse with `sts-enabled: false`, so its catalogs
# remote-sign and the own-credentials path is not a fallback there -- it is the
# only path. ExperienceFlow's IWS deployments run in GCP and Azure, so an
# S3-only credential shape means those fleets cannot reclaim at all.


def test_each_provider_declares_the_schemes_it_can_serve():
    """Derived from the classes, so a new provider cannot forget to say.

    Schemes are what `CatalogSession.table` matches a table's location against,
    and a provider claiming none would silently never match.
    """
    from zamboni.session import AzureSettings, GCSSettings

    for settings in (CREDS, GCSSettings(token="google_default"), AzureSettings()):
        assert settings.schemes, f"{type(settings).__name__} declares no scheme"
        assert all(s == s.lower() for s in settings.schemes)


@pytest.fixture
def any_backend(monkeypatch):
    """Ignore whether the cloud package is installed, for tests about *selection*.

    `file_io()` refuses when its backend is missing, which is the right
    behaviour and a different question from which FileIO it chooses. The dev
    environment has neither `gcsfs` nor `adlfs`; the consumer CI leg has both,
    and runs these same assertions against the real thing.
    """
    from zamboni import session

    monkeypatch.setattr(session, "_require_backend", lambda *a: None)


def test_gcs_goes_through_gcsfs_not_pyarrow(any_backend):
    """A deliberate divergence from PyIceberg, and the point of ZMBNI-97.

    `SCHEMA_TO_FILE_IO` maps `gs` to `PyArrowFileIO` *only*, whose GCS path reads
    `gcs.oauth2.token` as a bearer token with an expiry. `gcsfs` reads the same
    property as a service-account key file or `google_default` (ADC, which is
    what GKE Workload Identity provides), so it is the one that can serve a
    deployment holding a key file rather than a token.
    """
    from zamboni.session import GCSSettings

    io = GCSSettings(token="google_default", project_id="proj").file_io()

    assert type(io).__name__ == "FsspecFileIO"
    assert io.properties["gcs.oauth2.token"] == "google_default"
    assert io.properties["gcs.project-id"] == "proj"


def test_azure_follows_pyicebergs_own_preference(any_backend):
    """Not a divergence: `SCHEMA_TO_FILE_IO` lists fsspec first for abfs/abfss."""
    from pyiceberg.io import SCHEMA_TO_FILE_IO

    from zamboni.session import AzureSettings

    preferred = SCHEMA_TO_FILE_IO["abfs"][0]
    io = AzureSettings(account_name="acme", account_key="k").file_io()

    assert preferred.endswith(type(io).__name__)
    assert io.properties["adls.account-name"] == "acme"


@pytest.mark.parametrize(
    ("location", "fits"),
    [
        ("s3://warehouse/db/t", True),
        ("s3a://warehouse/db/t", True),
        ("gs://warehouse/db/t", False),
        ("abfss://c@acme.dfs.core.windows.net/db/t", False),
    ],
)
def test_credentials_for_the_wrong_store_are_refused_rather_than_attempted(location, fits):
    """Handing S3 credentials to a `gs://` table builds a FileIO that cannot
    address it. The failure would otherwise surface at the first read, or -- far
    worse -- as an empty listing, which orphan removal reads as "everything is
    unreferenced"."""
    session = CatalogSession(
        catalog=FakeCatalog(location=location, **SIGNING),
        con=None,
        storage=CREDS,
        credential_use=CredentialUse.ALWAYS,
    )

    if fits:
        assert session.table("db.t", reclaiming=True) is not None
        return

    with pytest.raises(StorageCredentialsRequired) as caught:
        session.table("db.t", reclaiming=True)
    assert location.split("://")[0] in str(caught.value), "the refusal must name the scheme found"


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        ("gs://warehouse/db/t", "ZAMBONI_GCS_TOKEN"),
        ("abfss://c@acme.dfs.core.windows.net/db/t", "ZAMBONI_AZURE_ACCOUNT_NAME"),
        ("s3://warehouse/db/t", "ZAMBONI_S3_ACCESS_KEY_ID"),
    ],
)
def test_the_refusal_names_the_settings_for_the_store_the_table_is_in(location, expected):
    """A GCS warehouse told to set ZAMBONI_S3_ACCESS_KEY_ID sends an operator
    looking for a setting that cannot help them."""
    with pytest.raises(StorageCredentialsRequired) as caught:
        CatalogSession(
            catalog=FakeCatalog(location=location, **SIGNING),
            con=None,
            storage=None,
            credential_use=CredentialUse.ALWAYS,
        ).table("db.t", reclaiming=True)

    assert expected in str(caught.value)


def test_an_unreadable_location_does_not_fail_the_run():
    """The provider check is a diagnostic, not the safety net.

    A table whose location cannot be read still runs: refusing because a guard
    could not read its input would turn a diagnostic into an outage, and the
    genuinely dangerous outcome is caught by the completeness invariant instead.
    """
    from zamboni.session import _location_scheme

    assert _location_scheme(FakeTable(location="")) == ""
    assert _location_scheme(object()) == ""


def test_no_provider_leaks_a_secret_through_its_repr():
    """`S3Settings.__repr__` was redacted for a reason; the new ones inherit it.

    A frozen dataclass prints every field, so a secret reaches any formatted
    string -- a traceback with locals, `pytest --showlocals`, an aggregator.
    """
    from zamboni.session import AzureSettings, GCSSettings

    secrets = [
        (GCSSettings(token="ya29.raw-token-value"), ["ya29.raw-token-value"]),
        (
            AzureSettings(
                account_name="acme",
                account_key="account-key-value",
                sas_token="sas-value",
                client_secret="client-secret-value",
            ),
            ["account-key-value", "sas-value", "client-secret-value"],
        ),
        (
            S3Settings(
                endpoint="http://minio:9000",
                access_key_id="key-id-is-not-a-secret",
                secret_access_key="s3-secret-value",
            ),
            ["s3-secret-value"],
        ),
    ]
    for settings, values in secrets:
        rendered = repr(settings)
        for value in values:
            assert value not in rendered, f"{type(settings).__name__} leaked {value!r}"


def test_a_gcs_key_file_path_is_shown_because_it_is_not_the_secret():
    """The path names a mechanism and a location, which is what you need when
    the answer is "wrong credentials". The key's *contents* never come here."""
    from zamboni.session import GCSSettings

    assert "google_default" in repr(GCSSettings(token="google_default"))


def test_a_missing_cloud_backend_is_refused_with_the_extra_to_install():
    """`FsspecFileIO` builds without its backend, then fails at the first file.

    `fsspec` itself is always present and each cloud's package is imported
    lazily inside the scheme handler, so the natural failure is a bare
    `ModuleNotFoundError` naming a module the operator never asked for, part-way
    through a run. Measured: without `gcsfs`, `FsspecFileIO` constructs and
    `get_fs("gs")` raises.
    """
    from zamboni.session import AzureSettings, GCSSettings, _require_backend

    with pytest.raises(StorageCredentialsRequired) as caught:
        _require_backend("a_module_that_is_not_installed", "gcs", "GCS")
    assert "iceberg-zamboni[gcs]" in str(caught.value)

    # Whichever backends this environment happens to have, the guard must agree
    # with reality rather than with a hardcoded expectation.
    from importlib.util import find_spec

    for settings, module in ((GCSSettings(token="x"), "gcsfs"), (AzureSettings(), "adlfs")):
        installed = find_spec(module) is not None
        try:
            settings.file_io()
            refused = False
        except StorageCredentialsRequired:
            refused = True
        assert refused is not installed, f"{module}: guard disagrees with what is installed"


def test_no_usable_credentials_is_a_refusal_rather_than_a_traceback():
    """Exit 2, one clear line per table -- the shape ZMBNI-76 established.

    `StorageCredentialsRequired` used to escape `maintain()` uncaught, so a fleet
    run against a signing catalog with no credentials ended in a traceback from
    the first table instead of a reason for each. Asserted by running the loop
    against a maintainer that raises it, rather than by reading the `except`
    clause -- which would fail on a reformat and pass on a deletion.
    """
    from zamboni.maintainers import MaintenanceRequest
    from zamboni.maintainers.local import LocalMaintainer
    from zamboni.maintenance import _run
    from zamboni.session import StorageCredentialsRequired as Raised
    from zamboni.tableconfig import Retention

    class Refusing(LocalMaintainer):
        """The real engine, refusing where the session would.

        Subclassed rather than stubbed so the declaration `_run` consults is the
        genuine one -- a hand-written `capabilities()` could declare the
        operation unsupported and make this pass for the wrong reason.
        """

        def execute(self, operation, table, *, request, dry_run):
            raise Raised("db.t: this catalog vends no usable storage credentials")

    outcome = _run(
        Refusing(session=None),
        "db.t",
        Operation.EXPIRE,
        MaintenanceRequest(retention=Retention()),
        None,
        set(),
        commit=True,
    )

    assert outcome.exit_code == 2, "a configuration refusal is exit 2, not an exception"
    assert "storage credentials" in outcome.detail
