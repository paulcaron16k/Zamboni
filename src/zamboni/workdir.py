# SPDX-License-Identifier: Apache-2.0
"""Where this process may write, and whether it actually can.

Zamboni needs somewhere on disk that is not the warehouse: DuckDB spills sorts
and hash tables there whenever a rewrite will not fit ``memory_budget_bytes``.
That requirement was never declared, never validated and never reported, and
its default was the worst of the available answers -- DuckDB's own
``temp_directory`` is ``.tmp``, **relative to the current working directory**.

On a container with ``readOnlyRootFilesystem: true`` that directory cannot be
created. It is created lazily, only when a spill actually happens, so the
failure lands partway through a rewrite of a large table rather than at
startup, and it arrives as an exception from inside DuckDB:

    IOException: IO Error: Failed to create directory ".tmp": Permission denied

Reproduced before this module existed (ZMBNI-90), which is why the default is
now ``tempfile.gettempdir()``: it honours ``TMPDIR``, so a tmpfs ``emptyDir``
mounted at ``/tmp`` -- the ordinary way to give a hardened pod scratch space --
works with no configuration at all.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


class WorkspaceUnavailable(RuntimeError):
    """No usable scratch directory. A refusal, not a failure."""


def resolve_temp_directory(configured: str | None) -> str:
    """The directory DuckDB may spill into.

    An explicitly configured value always wins, including one that turns out to
    be unwritable: an operator who named a path deserves to be told that path is
    wrong rather than to be silently moved somewhere else.
    """
    return configured or tempfile.gettempdir()


def assert_writable(path: str, *, setting: str, flag: str) -> None:
    """Refuse early, naming the setting and the flag that fix it.

    Checked by writing rather than by ``os.access``: the interesting cases are a
    read-only mount and a directory owned by another uid, and ``os.access`` on a
    process running as root reports success for both.
    """
    directory = Path(path)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=directory, prefix=".zamboni-writable-"):
            pass
    except OSError as exc:
        raise WorkspaceUnavailable(
            f"{path!r} is not writable ({exc.strerror or exc}), and compaction needs "
            f"somewhere to spill: a rewrite larger than the memory budget streams "
            f"through DuckDB, which writes sorts and hash tables to disk. Set "
            f"{setting} in zamboni.yml, pass {flag}, or set TMPDIR. On Kubernetes "
            f"with a read-only root filesystem, mount an emptyDir at /tmp."
        ) from exc


def describe_temp_directory(configured: str | None) -> tuple[str, str]:
    """``(path, state)`` for ``zamboni doctor``, never raising.

    A diagnostic that fails to produce a diagnosis is the opposite of useful, so
    an unwritable location is reported rather than raised.
    """
    path = resolve_temp_directory(configured)
    try:
        assert_writable(path, setting="temp_directory", flag="--temp-directory")
    except WorkspaceUnavailable as exc:
        return path, f"NOT WRITABLE -- {str(exc).split(',')[0]}"
    origin = "configured" if configured else f"default (TMPDIR={os.environ.get('TMPDIR', 'unset')})"
    return path, f"writable, {origin}"
