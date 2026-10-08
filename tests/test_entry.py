# SPDX-License-Identifier: Apache-2.0
"""The console script answers probes without importing the engine (ZMBNI-154).

`zamboni service-status --probe ...` is a Kubernetes exec probe with a 1 s
default timeout. It took 1.4-1.5 s when every `zamboni` command imported
PyIceberg, DuckDB and Arrow first. These tests check the *property* -- the
engine is not in `sys.modules` after a probe -- in a fresh interpreter, so a
future top-level import anywhere on the path fails here rather than in a pod.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

import zamboni
from zamboni.entry import PROBE_COMMANDS, _command_of

ENGINE = ("pyiceberg", "duckdb", "pyarrow")


def engine_loaded_after(code: str) -> list[str]:
    """Run `code` in a fresh interpreter; which engine modules did it import?"""
    script = f"""
import json, sys
{code}
print(json.dumps(sorted(m for m in {ENGINE!r} if m in sys.modules)))
"""
    out = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    ).stdout
    return json.loads(out.strip().splitlines()[-1])


@pytest.mark.parametrize(
    "argv",
    [
        ["service-status", "--state-file", "/nonexistent", "--probe", "liveness"],
        ["service-status", "--state-file", "/nonexistent"],
        ["config-reload", "--pid-file", "/nonexistent"],
    ],
)
def test_a_probe_command_imports_no_engine(argv):
    code = f"from zamboni.entry import main\nmain({argv!r})"
    assert engine_loaded_after(code) == []


def test_version_imports_no_engine():
    code = (
        "from zamboni.entry import main\n"
        "try:\n    main(['--version'])\nexcept SystemExit:\n    pass"
    )
    assert engine_loaded_after(code) == []


def test_importing_the_package_imports_no_engine():
    assert engine_loaded_after("import zamboni") == []


def test_every_public_name_still_resolves():
    """Lazy, not removed: everything in __all__ is there when asked for."""
    missing = [name for name in zamboni.__all__ if not hasattr(zamboni, name)]
    assert missing == []


def test_the_lazy_table_and_all_agree():
    """A name exported but not loadable, or loadable but not exported, is a
    drift between two lists that used to be one import block."""
    defined_here = {"__version__", "version_banner", "versions"}
    assert set(zamboni._LAZY) == set(zamboni.__all__) - defined_here


def test_submodules_are_still_attributes_of_the_package():
    """`import zamboni; zamboni.session` worked as a side effect of the eager
    imports. A consumer may rely on it."""
    assert zamboni.session.CatalogSession is zamboni.CatalogSession


def test_an_unknown_attribute_is_an_attribute_error():
    with pytest.raises(AttributeError, match="no attribute 'no_such_name'"):
        zamboni.no_such_name  # noqa: B018


def test_credential_use_is_one_enum_wherever_it_is_imported_from():
    """Moved to `zamboni.settings` so validating a profile stops importing
    PyIceberg; `zamboni.session.CredentialUse` must stay the same object."""
    from zamboni.session import CredentialUse as from_session
    from zamboni.settings import CredentialUse as from_settings

    assert from_session is from_settings


@pytest.mark.parametrize(
    ("argv", "command"),
    [
        (["service-status", "--probe", "liveness"], "service-status"),
        (["--profile", "p.yml", "--env", ".env", "config-reload"], "config-reload"),
        (["-v", "maintenance", "db.t"], "maintenance"),
        (["--version"], None),
        ([], None),
    ],
)
def test_the_router_finds_the_command_past_global_options(argv, command):
    assert _command_of(argv) == command


def test_the_full_parser_still_lists_the_probe_commands(capsys):
    """Defined once in zamboni.entry, registered by zamboni.cli too."""
    from zamboni.cli import _build_parser

    help_text = _build_parser().format_help()
    for name in PROBE_COMMANDS:
        assert name in help_text


def test_other_commands_still_reach_the_full_cli(tmp_path, capsys):
    from tests.test_runlog import record
    from zamboni.entry import main

    log = tmp_path / "runs.jsonl"
    log.write_text(json.dumps(record("acme")) + "\n")
    assert main(["runs", str(log)]) == 0
    assert capsys.readouterr().out.startswith("1 run(s)")
