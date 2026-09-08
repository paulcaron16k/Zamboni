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


class FakeTable:
    def __init__(self, **properties):
        self.io = FakeIO(**properties)


class FakeCatalog:
    """Hands back a table whose FileIO carries the properties a catalog returned."""

    def __init__(self, **properties):
        self._properties = properties

    def load_table(self, identifier):
        return FakeTable(**self._properties)


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
