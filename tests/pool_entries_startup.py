# SPDX-License-Identifier: Apache-2.0
"""A worker entry whose module kills its process the first time it is imported.

A spawned worker imports its entry *before* reading its first job, so this is a
worker dying during startup with a job already sent to it -- the case #155 was
about. The marker file makes it die once: the replacement worker finds the
marker and imports normally.
"""

from __future__ import annotations

import os
from pathlib import Path

_marker = Path(os.environ["ZAMBONI_TEST_STARTUP_MARKER"])
if not _marker.exists():
    _marker.write_text("died once")
    os._exit(3)

from tests.pool_entries import record  # noqa: E402,F401 - after the deliberate exit
