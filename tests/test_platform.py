# SPDX-License-Identifier: Apache-2.0
"""What Zamboni does where the OS differs (ZMBNI-236).

Linux and macOS are tested by running the suite on both. Native Windows is
not tested anywhere, so its branches are held two ways: `mypy --platform
win32` type-checks them in CI, and the tests below run each one here by
setting ``sys.platform`` -- which proves the branch is taken and says what it
should, not that Windows agrees. The ``msvcrt`` lock is the one branch that
cannot run off Windows, because the module does not exist.

The encoding checks are the other half: a text-mode open without an encoding
reads the locale's code page on Windows, and the suite fails on one from
zamboni's code -- but only if the warning is switched on, so the first test
here refuses a CI run that forgot to.
"""

from __future__ import annotations

import asyncio
import errno
import os
import sys
import warnings

import pytest

from zamboni import reload, service, settings


def test_ci_runs_the_suite_with_encoding_warnings_on():
    """Without PYTHONWARNDEFAULTENCODING the encoding filter below checks nothing, and passes."""
    if not os.environ.get("CI"):
        pytest.skip("CI sets PYTHONWARNDEFAULTENCODING=1; locally `make test` does")
    assert sys.flags.warn_default_encoding, (
        "CI must run the suite with PYTHONWARNDEFAULTENCODING=1 (.github/workflows/ci.yml env)"
    )


def test_an_encoding_warning_from_zamboni_fails_the_suite():
    with pytest.raises(EncodingWarning):
        warnings.warn_explicit(
            "'encoding' argument not specified", EncodingWarning, "x.py", 1, module="zamboni.x"
        )


def test_an_encoding_warning_from_elsewhere_does_not():
    with warnings.catch_warnings(record=True):
        warnings.warn_explicit(
            "'encoding' argument not specified", EncodingWarning, "x.py", 1, module="pyiceberg.x"
        )


# -- Windows branches, run by setting sys.platform ------------------------------------


@pytest.fixture
def windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")


def test_on_windows_a_credential_file_is_not_refused_for_its_mode(tmp_path, windows, caplog):
    """Windows reports every file as group- and other-readable; refusing would refuse all.

    Skipped with a warning naming the file and what to do instead.
    """
    env = tmp_path / ".env"
    env.write_text("ZAMBONI_TOKEN=x\n", encoding="utf-8")
    env.chmod(0o644)  # what the POSIX rule refuses
    settings.check_env_permissions(env)
    settings.check_profile_permissions(env)
    assert [r.levelname for r in caplog.records] == ["WARNING", "WARNING"]
    assert "ACL" in caplog.records[0].getMessage()
    assert str(env) in caplog.records[0].getMessage()


def test_off_windows_the_same_file_is_refused(tmp_path):
    """The negative case: the skip must be Windows-only."""
    env = tmp_path / ".env"
    env.write_text("ZAMBONI_TOKEN=x\n", encoding="utf-8")
    env.chmod(0o644)
    with pytest.raises(settings.ProfileError, match="chmod 600"):
        settings.check_env_permissions(env)


def test_on_windows_config_reload_refuses_and_names_the_poll(tmp_path, windows):
    pid_file = tmp_path / "serve.pid"
    pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")
    with pytest.raises(reload.PidFileError, match=rf"within {reload.POLL_SECONDS} s"):
        reload.send_reload(pid_file)


def test_on_windows_no_hangup_handler_is_installed(windows):
    """Windows' event loop has no add_signal_handler; installing one would raise."""

    class Loop:
        def add_signal_handler(self, *_):
            raise AssertionError("add_signal_handler called on Windows")

    reload.install_hangup(Loop(), asyncio.Event())  # type: ignore[arg-type]


def test_a_lock_failure_that_is_not_another_serve_is_raised_as_itself(tmp_path, monkeypatch):
    """ENOLCK (a filesystem without locks) must not read as "another serve is running"."""
    import fcntl

    def no_locks(*_):
        raise OSError(errno.ENOLCK, "No locks available")

    monkeypatch.setattr(fcntl, "flock", no_locks)
    with (
        pytest.raises(OSError, match="No locks available"),
        service.single_instance(tmp_path / "serve.pid"),
    ):
        pass
