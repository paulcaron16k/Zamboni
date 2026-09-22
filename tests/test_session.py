"""Catalog and storage connection settings.

`S3Settings` grew optional credentials so that "point at this endpoint, let the
catalog vend the keys" is expressible. That is a behaviour change with a sharp
edge -- an absent credential must be *omitted*, not passed to PyIceberg as the
string `None` -- and it shipped untested, which is why these exist.
"""

from __future__ import annotations

import pytest

from zamboni import CatalogSession, S3Settings


def test_full_credentials_are_all_present():
    props = S3Settings(
        endpoint="http://localhost:9010",
        access_key_id="key",
        secret_access_key="secret",
        region="local-01",
    ).as_properties()

    assert props["s3.endpoint"] == "http://localhost:9010"
    assert props["s3.access-key-id"] == "key"
    assert props["s3.secret-access-key"] == "secret"
    assert props["s3.region"] == "local-01"
    assert props["s3.path-style-access"] == "true"


def test_absent_credentials_are_omitted_not_sent_as_none():
    """The bug this replaced: `None` reached PyIceberg as a credential value.

    A vending catalog supplies keys per table, so an endpoint without keys is a
    legitimate combination. Sending the key as `None` is not.
    """
    props = S3Settings(endpoint="http://localhost:9010").as_properties()

    assert "s3.access-key-id" not in props
    assert "s3.secret-access-key" not in props
    assert None not in props.values(), f"a None leaked into {props}"
    assert props["s3.endpoint"] == "http://localhost:9010"


def test_one_credential_without_the_other_is_still_omitted_individually():
    props = S3Settings(endpoint="http://x", access_key_id="key").as_properties()

    assert props["s3.access-key-id"] == "key"
    assert "s3.secret-access-key" not in props


def test_every_property_value_is_a_string():
    """PyIceberg's FileIO config is a str->str map; a bool or int here would be
    silently stringified differently by different backends."""
    props = S3Settings(
        endpoint="http://x", access_key_id="k", secret_access_key="s", path_style_access=False
    ).as_properties()

    assert all(isinstance(v, str) for v in props.values()), props
    assert props["s3.path-style-access"] == "false"


def test_extra_overrides_the_derived_properties():
    """`extra` is the escape hatch, so it must win rather than be overwritten."""
    props = S3Settings(
        endpoint="http://x",
        access_key_id="k",
        secret_access_key="s",
        extra={"s3.endpoint": "http://override", "s3.custom": "v"},
    ).as_properties()

    assert props["s3.endpoint"] == "http://override"
    assert props["s3.custom"] == "v"


def _lk(**over):
    """lakekeeper_properties with the two required args filled in."""
    base = {"uri": "https://catalog.example.com/catalog", "warehouse": "acme"}
    base.update(over)
    return CatalogSession.lakekeeper_properties(**base)


# -- transport security (ZMBNI-100) ---------------------------------------
#
# The key names come from PyIceberg rather than from string literals here. A
# test that spells them itself can only prove this function agrees with the test
# -- and the shape is exactly the kind of thing that is easy to get wrong and
# impossible to notice: PyIceberg reads `properties["ssl"]["cabundle"]`, nested,
# while every other property it takes is a flat dotted string. Measured: passing
# the flat `"ssl.cabundle"` instead leaves `session.verify` at True, so the
# setting silently does nothing and a literal-matching test stays green.


def test_ssl_ca_bundle_is_forwarded_under_the_keys_pyiceberg_reads():
    from pyiceberg.catalog.rest import CA_BUNDLE, SSL

    props = _lk(ssl_ca_bundle="/etc/ssl/certs/private-ca.pem")

    assert props[SSL] == {CA_BUNDLE: "/etc/ssl/certs/private-ca.pem"}


def test_ssl_insecure_becomes_a_real_false_cabundle():
    """`requests` assigns `session.verify = ssl.cabundle` directly, so only the
    bool False disables verification; the string "false" is truthy and would be
    read as the name of a certificate file."""
    from pyiceberg.catalog.rest import CA_BUNDLE, SSL

    props = _lk(ssl_insecure=True)

    assert props[SSL] == {CA_BUNDLE: False}
    assert props[SSL][CA_BUNDLE] is False


def test_ssl_insecure_wins_over_a_ca_bundle():
    """Skipping and trusting are contradictory; the escape hatch is the more
    explicit intent, and the safe reading of "both were set" is not to silently
    keep verifying against a bundle the operator may have meant to bypass."""
    from pyiceberg.catalog.rest import CA_BUNDLE, SSL

    props = _lk(ssl_ca_bundle="/etc/ssl/certs/private-ca.pem", ssl_insecure=True)

    assert props[SSL] == {CA_BUNDLE: False}


def test_ssl_is_absent_when_unset():
    from pyiceberg.catalog.rest import SSL

    assert SSL not in _lk()


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        ({}, True),
        ({"ssl_insecure": True}, False),
        ({"ssl_ca_bundle": "/etc/ssl/certs/private-ca.pem"}, "/etc/ssl/certs/private-ca.pem"),
    ],
)
def test_the_properties_actually_reach_the_http_session(settings, expected):
    """The one test that would survive PyIceberg renaming the key.

    Everything above asserts what this function produces. This asserts what
    `RestCatalog` *does* with it, which is the only thing the operator cares
    about -- `requests` verifies against `session.verify`, and nothing else in
    this package can see whether it was set.

    `_fetch_config` is patched out because it is the one part that needs a live
    catalog; it builds its own session from the same properties, so the value
    under test is the value that first request would use too.
    """
    from unittest.mock import patch

    from pyiceberg.catalog.rest import RestCatalog

    props = _lk(**settings)
    props.pop("type")

    with patch.object(RestCatalog, "_fetch_config", lambda self: None):
        catalog = RestCatalog("under-test", **props)

    assert catalog._session.verify == expected
    if expected is False:
        assert catalog._session.verify is False, "a truthy 'false' would verify against a filename"
