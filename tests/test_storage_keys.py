# SPDX-License-Identifier: Apache-2.0
"""Every key `storage:` in zamboni.yml accepts reaches the store (#263).

`settings.STORAGE_SETTINGS` is an allow-list so that a typo fails at load
rather than becoming a setting that does nothing. Two keys on it did nothing
anyway: `cli._storage_from` never passed `path_style_access` or
`default_location` on. The first case below is derived from the allow-list, so
a key added there without being wired through fails it.
"""

from __future__ import annotations

import pytest

from tests.test_env_flags import dotenv, settled, workdir  # noqa: F401 - fixture
from zamboni import cli
from zamboni.session import AzureSettings, GCSSettings, S3Settings
from zamboni.settings import (
    BOOLEAN_STORAGE_SETTINGS,
    REMOVED_STORAGE_SETTINGS,
    STORAGE_SETTINGS,
    ProfileError,
    load_profile,
)

#: The settings object `_storage_from` builds for each provider's block.
BUILDS = {"s3": S3Settings, "gcs": GCSSettings, "azure": AzureSettings}

#: Each variable `_storage_from` reads, unset so only the profile speaks.
STORAGE_VARIABLES = [
    f"ZAMBONI_{provider.upper()}_{key.upper()}"
    for provider, keys in STORAGE_SETTINGS.items()
    for key in keys
]


def profile(directory, provider: str, block: dict[str, str]) -> None:
    """A `storage:` block. It may hold a secret, so 0600 either way."""
    path = directory / "zamboni.yml"
    path.write_text(
        f"storage:\n  {provider}:\n" + "".join(f"    {k}: {v}\n" for k, v in block.items())
    )
    path.chmod(0o600)


def every_key(provider: str) -> dict[str, tuple[str, object]]:
    """A value for each accepted key, as written and as the settings object should
    hold it. A switch takes the opposite of its default, so arriving shows."""
    values: dict[str, tuple[str, object]] = {}
    for key in sorted(STORAGE_SETTINGS[provider]):
        if (provider, key) in BOOLEAN_STORAGE_SETTINGS:
            flipped = not getattr(BUILDS[provider], key)
            values[key] = (str(flipped).lower(), flipped)
        else:
            values[key] = (f"{provider}-{key}", f"{provider}-{key}")
    return values


@pytest.fixture
def quiet(workdir, monkeypatch):  # noqa: F811 - the imported fixture, by name
    for variable in STORAGE_VARIABLES:
        monkeypatch.delenv(variable, raising=False)
    return workdir


def test_each_provider_builds_the_settings_class_named_here():
    assert set(BUILDS) == set(STORAGE_SETTINGS)


@pytest.mark.parametrize("provider", sorted(STORAGE_SETTINGS))
def test_every_accepted_key_reaches_the_settings(quiet, provider):
    values = every_key(provider)
    profile(quiet, provider, {k: text for k, (text, _) in values.items()})

    built = cli._storage_from(settled(["compact", "x.y"]))

    assert isinstance(built, BUILDS[provider])
    dropped = {k: getattr(built, k, "<no field>") for k, (_, want) in values.items()}
    assert dropped == {k: want for k, (_, want) in values.items()}, (
        "a key the profile accepts must reach the store, or be refused at load"
    )


@pytest.mark.parametrize(("provider", "key"), sorted(REMOVED_STORAGE_SETTINGS))
def test_a_removed_key_is_refused_saying_why(tmp_path, provider, key):
    profile(tmp_path, provider, {key: "anything"})

    with pytest.raises(ProfileError, match=rf"storage\.{provider}\.{key}: removed\."):
        load_profile(tmp_path / "zamboni.yml")


def test_no_bucket_location_survives_on_the_gcs_settings():
    """Zamboni creates no bucket, so nothing may ask PyIceberg to place one."""
    assert "default_location" not in GCSSettings.__dataclass_fields__
    assert not any("bucket-location" in k for k in GCSSettings(token="t").as_properties())


@pytest.mark.parametrize(("provider", "key"), sorted(BOOLEAN_STORAGE_SETTINGS))
def test_a_switch_that_is_not_true_or_false_fails_at_load(tmp_path, provider, key):
    profile(tmp_path, provider, {key: "ture"})

    with pytest.raises(ProfileError, match=rf"storage\.{provider}\.{key}: must be true or false"):
        load_profile(tmp_path / "zamboni.yml")


S3 = {"endpoint": "http://s3", "access_key_id": "id", "secret_access_key": "s"}


@pytest.mark.parametrize(
    ("flags", "variable", "in_dotenv", "in_profile", "expected"),
    [
        ([], None, None, None, True),
        ([], None, None, "false", False),
        ([], None, "false", "true", False),
        ([], "true", "false", "false", True),
        (["--no-s3-path-style-access"], "true", None, "true", False),
        (["--s3-path-style-access"], "false", None, "false", True),
    ],
    ids=["default", "profile", "dotenv-over-profile", "process-over-dotenv", "flag-off", "flag-on"],
)
def test_path_style_resolves_flag_process_dotenv_profile_default(
    quiet, monkeypatch, flags, variable, in_dotenv, in_profile, expected
):
    block = dict(S3)
    if in_profile is not None:
        block["path_style_access"] = in_profile
    profile(quiet, "s3", block)
    if in_dotenv is not None:
        dotenv(quiet, ZAMBONI_S3_PATH_STYLE_ACCESS=in_dotenv)
    if variable is not None:
        monkeypatch.setenv("ZAMBONI_S3_PATH_STYLE_ACCESS", variable)

    built = cli._storage_from(settled(["compact", *flags, "x.y"]))

    assert built.path_style_access is expected
    assert built.as_properties()["s3.path-style-access"] == str(expected).lower()
