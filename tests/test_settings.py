"""Profile and dotenv discovery: the config a cron line does not have to type.

The behaviours worth pinning are the resolution *order* and the refusals. A
profile that silently ignores a misspelled key is a setting the operator
believes is in force and is not -- on this tool that can mean a retention or an
operation list that is not what the file says.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from zamboni import settings


def write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text)
    return path


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """Hermetic, for the reason ZMBNI-912 exists: these read ZAMBONI_* and a
    developer's shell may well have them exported."""
    for var in (
        "ZAMBONI_URI",
        "ZAMBONI_WAREHOUSE",
        "ZAMBONI_ENGINE",
        "ZAMBONI_ROOT",
        "ZAMBONI_CREDENTIAL",
    ):
        monkeypatch.delenv(var, raising=False)


# -- discovery ------------------------------------------------------------


def test_a_profile_in_the_working_directory_is_found(tmp_path):
    write(tmp_path, "zamboni.yml", "warehouse: acme\n")

    assert settings.find_profile(start=tmp_path) == tmp_path / "zamboni.yml"


def test_no_profile_anywhere_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.setenv("ZAMBONI_ROOT", str(tmp_path / "empty"))

    profile = settings.load_profile(settings.find_profile(start=tmp_path))

    assert profile.source is None
    assert profile.operations == settings.DEFAULT_OPERATIONS


def test_an_explicit_profile_that_does_not_exist_is_an_error(tmp_path):
    with pytest.raises(settings.ProfileError, match="no such file"):
        settings.find_profile(str(tmp_path / "absent.yml"))


def test_an_explicit_env_that_does_not_exist_is_an_error(tmp_path):
    with pytest.raises(settings.ProfileError, match="no such file"):
        settings.find_env(str(tmp_path / "absent"))


# -- the profile ----------------------------------------------------------


def test_a_profile_supplies_what_no_flag_did(tmp_path):
    path = write(
        tmp_path,
        "zamboni.yml",
        "uri: https://catalog.internal\nwarehouse: acme\nengine: trino\n",
    )

    profile = settings.load_profile(path)

    assert profile.uri == "https://catalog.internal"
    assert profile.warehouse == "acme"
    assert profile.engine == "trino"


def test_an_unknown_key_is_refused_rather_than_ignored(tmp_path):
    path = write(tmp_path, "zamboni.yml", "warehouse: acme\nwarehosue: typo\n")

    with pytest.raises(settings.ProfileError, match="unknown key"):
        settings.load_profile(path)


def test_an_unknown_operation_is_refused(tmp_path):
    path = write(tmp_path, "zamboni.yml", "operations:\n  - compact\n  - vacuum\n")

    with pytest.raises(settings.ProfileError, match="unknown operation"):
        settings.load_profile(path)


def test_operations_default_to_the_runbook_order(tmp_path):
    profile = settings.load_profile(write(tmp_path, "zamboni.yml", "warehouse: acme\n"))

    assert profile.operations == settings.DEFAULT_OPERATIONS
    assert profile.operations[0] == "compact"
    # remove-orphans last: it sweeps what the earlier steps unreferenced.
    assert profile.operations[-1] == "remove-orphans"


def test_a_subset_of_operations_is_honoured(tmp_path):
    path = write(tmp_path, "zamboni.yml", "operations:\n  - compact\n  - expire\n")

    assert settings.load_profile(path).operations == ("compact", "expire")


def test_malformed_yaml_names_the_file(tmp_path):
    path = write(tmp_path, "zamboni.yml", "warehouse: [unclosed\n")

    with pytest.raises(settings.ProfileError, match="not valid YAML"):
        settings.load_profile(path)


def test_a_top_level_list_is_refused(tmp_path):
    path = write(tmp_path, "zamboni.yml", "- compact\n- expire\n")

    with pytest.raises(settings.ProfileError, match="expected a mapping"):
        settings.load_profile(path)


# -- the multi-tenant layout ----------------------------------------------


def test_table_config_resolves_per_warehouse(tmp_path):
    """$ZAMBONI_ROOT/configs/{warehouse}/table-config.json -- one file per
    customer in a predictable place, so provisioning a tenant is writing that
    file and adding a cron line. docs/devops.md section 5."""
    path = write(tmp_path, "zamboni.yml", f"root: {tmp_path}\n")

    resolved = settings.load_profile(path).table_config_for("acme")

    assert resolved == tmp_path / "configs" / "acme" / "table-config.json"


def test_root_defaults_under_the_home_directory(tmp_path):
    profile = settings.load_profile(write(tmp_path, "zamboni.yml", "warehouse: acme\n"))

    assert profile.root == settings.DEFAULT_ROOT
    assert profile.root.name == ".zamboni"


def test_zamboni_root_in_the_environment_beats_the_default(tmp_path, monkeypatch):
    monkeypatch.setenv("ZAMBONI_ROOT", str(tmp_path / "srv"))

    profile = settings.load_profile(write(tmp_path, "zamboni.yml", "warehouse: acme\n"))

    assert profile.root == tmp_path / "srv"


def test_the_profile_beats_zamboni_root_from_the_environment(tmp_path, monkeypatch):
    """A flag beats the profile and the profile beats the environment, so a
    committed file is authoritative over an inherited shell."""
    monkeypatch.setenv("ZAMBONI_ROOT", str(tmp_path / "from-env"))
    path = write(tmp_path, "zamboni.yml", f"root: {tmp_path / 'from-file'}\n")

    assert settings.load_profile(path).root == tmp_path / "from-file"


# -- the dotenv file ------------------------------------------------------


def test_env_values_are_loaded(tmp_path, monkeypatch):
    path = write(tmp_path, ".env", "ZAMBONI_CREDENTIAL=id:secret\n")

    applied = settings.load_env(path)

    assert applied["ZAMBONI_CREDENTIAL"] == "id:secret"
    assert os.environ["ZAMBONI_CREDENTIAL"] == "id:secret"


def test_a_real_environment_variable_beats_the_file(tmp_path, monkeypatch):
    """Deliberate: a container or systemd unit that injects secrets properly
    must not be overridden by a stale .env left in the working directory."""
    monkeypatch.setenv("ZAMBONI_CREDENTIAL", "from-environment")
    write(tmp_path, ".env", "ZAMBONI_CREDENTIAL=from-file\n")

    settings.load_env(tmp_path / ".env")

    assert os.environ["ZAMBONI_CREDENTIAL"] == "from-environment"


def test_resolve_loads_env_before_the_profile(tmp_path, monkeypatch):
    """Order matters: the profile's own defaults read ZAMBONI_*, so a .env that
    sets ZAMBONI_WAREHOUSE has to be in effect before the profile is built."""
    write(tmp_path, ".env", "ZAMBONI_WAREHOUSE=from-env\n")
    write(tmp_path, "zamboni.yml", "engine: local\n")
    monkeypatch.setenv("ZAMBONI_ROOT", str(tmp_path))

    profile, env_file = settings.resolve(start=tmp_path)

    assert env_file == tmp_path / ".env"
    assert profile.warehouse == "from-env"
