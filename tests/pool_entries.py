# SPDX-License-Identifier: Apache-2.0
"""Worker entry points for tests/test_pool.py.

A module of its own because a *spawned* worker imports its entry by name: it
must be importable from a fresh interpreter, which a function defined inside a
test is not.
"""

from __future__ import annotations

import json
import os
import signal
import sys
from pathlib import Path


def record(argv: list[str]) -> int:
    """Write what the worker saw into the run record, then behave per table."""
    table = argv[1]
    if table == "raw.die":
        os.kill(os.getpid(), signal.SIGKILL)
    if table == "raw.boom":
        raise RuntimeError("boom")
    if table == "raw.usage":
        sys.exit(2)
    path = argv[argv.index("--json") + 1]
    with open(path, "a") as out:
        out.write(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "argv": argv,
                    "marker": os.environ.get("ZAMBONI_TEST_MARKER"),
                    "table_config": Path(argv[argv.index("--table-config") + 1]).read_text(),
                }
            )
            + "\n"
        )
    # Left behind on purpose: the next table on this worker must not see it.
    os.environ["ZAMBONI_TEST_MARKER"] = "left over from the previous table"
    print(f"maintained {table}")
    return 3 if table == "raw.blocked" else 0
