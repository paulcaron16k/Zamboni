# SPDX-License-Identifier: Apache-2.0
"""Where Zamboni may write, and what happens when it may not.

DuckDB spills sorts and hash tables to disk whenever a rewrite will not fit the
memory budget, and until ZMBNI-90 the location was whatever DuckDB chose --
`.tmp`, relative to the current working directory. A container with a read-only
root filesystem cannot create it, and creates it lazily, so the failure landed
partway through a rewrite of a large table.
"""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path

import pytest

from zamboni.workdir import (
    WorkspaceUnavailable,
    assert_writable,
    describe_temp_directory,
    resolve_temp_directory,
)


def test_the_default_is_the_system_temp_directory_not_a_relative_path():
    """The whole defect in one assertion.

    DuckDB's own default is `.tmp`, resolved against the process's working
    directory -- so it depends on where the operator happened to be standing,
    and on a hardened container it cannot be created at all. The system temp
    directory honours TMPDIR, which is how a Kubernetes emptyDir mounted at
    /tmp reaches it with no configuration.
    """
    resolved = resolve_temp_directory(None)

    assert Path(resolved).is_absolute(), f"{resolved!r} is relative, like DuckDB's '.tmp'"
    assert resolved == tempfile.gettempdir()


def test_an_explicit_setting_wins_even_when_it_is_wrong(tmp_path):
    """An operator who named a path is told that path is wrong.

    Silently relocating their spill would hide a misconfiguration and put
    possibly-large files somewhere they did not choose.
    """
    named = str(tmp_path / "somewhere-specific")

    assert resolve_temp_directory(named) == named


def test_an_unwritable_location_is_refused_with_the_setting_that_fixes_it(tmp_path):
    read_only = tmp_path / "ro"
    read_only.mkdir()
    read_only.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        with pytest.raises(WorkspaceUnavailable) as caught:
            assert_writable(
                str(read_only / "spill"), setting="temp_directory", flag="--temp-directory"
            )
    finally:
        read_only.chmod(stat.S_IRWXU)

    message = str(caught.value)
    # An operator reading this at 08:00 needs the fix, not the diagnosis.
    assert "temp_directory" in message and "--temp-directory" in message
    assert "TMPDIR" in message and "emptyDir" in message


def test_writability_is_checked_by_writing_not_by_asking(tmp_path, monkeypatch):
    """`os.access` answers for the *real* uid and lies for root.

    A maintenance container frequently runs as root, where `os.access` reports
    success on a directory mounted read-only. Only a write settles it.
    """
    called = False

    def fake_access(*_args, **_kwargs):
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(os, "access", fake_access)
    assert_writable(str(tmp_path), setting="temp_directory", flag="--temp-directory")

    assert not called, "writability was decided by os.access rather than by writing"


def test_the_probe_file_does_not_survive(tmp_path):
    before = set(tmp_path.iterdir())
    assert_writable(str(tmp_path), setting="temp_directory", flag="--temp-directory")

    assert set(tmp_path.iterdir()) == before, "the writability probe left a file behind"


def test_doctor_reports_rather_than_raises(tmp_path):
    """A diagnostic that fails to produce a diagnosis is the opposite of useful."""
    read_only = tmp_path / "ro"
    read_only.mkdir()
    read_only.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        path, state = describe_temp_directory(str(read_only / "spill"))
    finally:
        read_only.chmod(stat.S_IRWXU)

    assert "NOT WRITABLE" in state
    assert str(read_only) in path

    path, state = describe_temp_directory(None)
    assert path == tempfile.gettempdir()
    assert "writable" in state
