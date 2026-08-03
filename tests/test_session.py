"""Catalog and storage connection settings.

`S3Settings` grew optional credentials so that "point at this endpoint, let the
catalog vend the keys" is expressible. That is a behaviour change with a sharp
edge -- an absent credential must be *omitted*, not passed to PyIceberg as the
string `None` -- and it shipped untested, which is why these exist.
"""

from __future__ import annotations

from zamboni import S3Settings


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
