# SPDX-License-Identifier: Apache-2.0
"""A worker entry whose module is slow to import, or hangs importing (#272).

A spawned worker imports its entry *before* reading its first job, so time
spent here is a worker starting up with a job already sent to it -- the window
in which the pool once ran the table's clocks.

Both settings are read from the real environment, not ``base_environ``, which
a worker restores only per job, after this import:

- ``ZAMBONI_TEST_STARTUP_SECONDS``: sleep that long at every import.
- ``ZAMBONI_TEST_HANG_ONCE_MARKER``: the first import to find no file there
  writes it and hangs; a later worker finds it and imports normally.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

if (marker := os.environ.get("ZAMBONI_TEST_HANG_ONCE_MARKER")) and not Path(marker).exists():
    Path(marker).write_text(str(os.getpid()), encoding="ascii")
    time.sleep(600)

time.sleep(float(os.environ.get("ZAMBONI_TEST_STARTUP_SECONDS", "0")))

from tests.pool_entries import record  # noqa: E402,F401 - after the deliberate delay
