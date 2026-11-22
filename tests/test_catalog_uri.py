# SPDX-License-Identifier: Apache-2.0
"""A catalog URI with ``user:password@`` in it is refused, from every source (ZMBNI-211).

PyIceberg's REST client never sends URL userinfo -- the measurement is in
`settings.catalog_uri_userinfo_error` -- so a password there authenticated
nothing, and was printed by ``repr(Profile)`` and kept outside the ``.env``
mode rule. Each way the URI arrives gets its own case, and each asserts the
password is not in the message, since echoing it would be the defect again.
"""

from __future__ import annotations

import pytest

from zamboni import cli, settings
from zamboni.session import CatalogSession

#: A string nothing else contains, so its absence from a message proves something.
PLANTED = "s3cr3t-planted-211"
URI = f"https://svc:{PLANTED}@lakekeeper.internal/catalog"


@pytest.fixture
def workdir(tmp_path, monkeypatch, restore_environment):
    """No profile, no ``.env`` and no ``ZAMBONI_*`` of the developer's in reach."""
    monkeypatch.chdir(tmp_path)
    for variable in ("ZAMBONI_URI", "ZAMBONI_WAREHOUSE", "ZAMBONI_CREDENTIAL", "ZAMBONI_TOKEN"):
        monkeypatch.delenv(variable, raising=False)
    (tmp_path / "root").mkdir()
    monkeypatch.setenv("ZAMBONI_ROOT", str(tmp_path / "root"))
    return tmp_path


def refused_by_the_cli(argv: list[str], capsys) -> str:
    """Run ``main``; assert a usage error, and return what it said."""
    with pytest.raises(SystemExit) as caught:
        cli.main(argv)
    assert caught.value.code == 2
    message = capsys.readouterr().err
    assert PLANTED not in message
    assert "credentials written into it" in message
    return message


def test_in_the_profile(workdir, capsys):
    profile = workdir / "zamboni.yml"
    profile.write_text(f"uri: {URI}\nwarehouse: acme\n")

    with pytest.raises(settings.ProfileError) as caught:
        settings.load_profile(profile)
    assert f"{profile}: uri" in str(caught.value)
    assert PLANTED not in str(caught.value)

    # And through the command line, before anything connects.
    assert "uri" in refused_by_the_cli(["plan", "acme.events"], capsys)


def test_in_the_process_environment(workdir, monkeypatch, capsys):
    monkeypatch.setenv("ZAMBONI_URI", URI)

    with pytest.raises(settings.ProfileError, match="ZAMBONI_URI"):
        settings.load_profile(None)

    assert "ZAMBONI_URI" in refused_by_the_cli(
        ["plan", "--warehouse", "acme", "acme.events"], capsys
    )


def test_in_the_process_environment_beside_a_profile(workdir, monkeypatch):
    """With a profile, ZAMBONI_URI is still judged: `load_profile` reads it as the fallback."""
    (workdir / "zamboni.yml").write_text("warehouse: acme\n")
    monkeypatch.setenv("ZAMBONI_URI", URI)

    with pytest.raises(settings.ProfileError, match="ZAMBONI_URI"):
        settings.load_profile(workdir / "zamboni.yml")


def test_in_dotenv(workdir, capsys):
    """`.env` is loaded into the environment before the profile, so it is the same check."""
    dotenv = workdir / ".env"
    dotenv.write_text(f"ZAMBONI_URI={URI}\n")
    dotenv.chmod(0o600)

    assert "ZAMBONI_URI" in refused_by_the_cli(
        ["plan", "--warehouse", "acme", "acme.events"], capsys
    )


def test_on_the_command_line(workdir, capsys):
    message = refused_by_the_cli(
        ["plan", "--uri", URI, "--warehouse", "acme", "acme.events"], capsys
    )
    assert "--uri" in message


def test_in_a_library_session():
    """The backstop for callers that never load a profile: the demo, an embedding app."""
    with pytest.raises(ValueError, match="credentials written into it") as caught:
        CatalogSession.lakekeeper_properties(uri=URI, warehouse="acme")
    assert PLANTED not in str(caught.value)


@pytest.mark.parametrize(
    "uri",
    [
        f"https://{PLANTED}@lakekeeper.internal/catalog",  # a token alone
        f"svc:{PLANTED}@lakekeeper.internal/catalog",  # no scheme
    ],
    ids=["user-only", "no-scheme"],
)
def test_every_shape_of_userinfo_is_caught(uri):
    with pytest.raises(ValueError, match="credentials written into it"):
        CatalogSession.lakekeeper_properties(uri=uri, warehouse="acme")


@pytest.mark.parametrize(
    "uri",
    [
        "https://lakekeeper.internal/catalog",
        "http://127.0.0.1:8181/catalog",
        # An @ after the host is a path, not userinfo.
        "https://lakekeeper.internal/catalog/team@acme",
    ],
)
def test_a_uri_without_userinfo_is_accepted(workdir, uri):
    profile = workdir / "zamboni.yml"
    profile.write_text(f"uri: {uri}\nwarehouse: acme\n")

    assert settings.load_profile(profile).uri == uri
    assert CatalogSession.lakekeeper_properties(uri=uri, warehouse="acme")["uri"] == uri
