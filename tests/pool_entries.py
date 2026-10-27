# SPDX-License-Identifier: Apache-2.0
"""Worker entry points for tests/test_pool.py.

A module of its own because a *spawned* worker imports its entry by name: it
must be importable from a fresh interpreter, which a function defined inside a
test is not.

:func:`record` is the default entry with only the CLI swapped out: it goes
through :func:`zamboni.pool.run_cli`, so the command line, the table-config
file and the run record are the default entry's own, and :func:`_cli` stands
in for ``zamboni.cli.main`` to crash on cue.
"""

from __future__ import annotations

import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

from zamboni.pool import WorkerContext, run_cli
from zamboni.scheduler import WorkItem


def record(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    return run_cli(item, context, main=_cli)


def progress(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Report progress, then end as ``item.table`` says: die, or return a record."""
    context.progress("first")
    context.progress(f"second {item.table}")
    if item.table == "raw.die":
        os.kill(os.getpid(), signal.SIGKILL)
    return {"exit_code": 0, "commit": item.commit}


def not_a_record(item: WorkItem, context: WorkerContext) -> Any:
    """Breaks rule 3: returns what a CLI ``main`` would, an exit code."""
    return 0


def _cli(argv: list[str]) -> int:
    """Write what the worker saw into the run record, then behave per table."""
    table = argv[1]
    if table == "raw.die":
        os.kill(os.getpid(), signal.SIGKILL)
    if table == "raw.boom":
        raise RuntimeError("boom")
    if table == "raw.usage":
        sys.exit(2)
    path = argv[argv.index("--json") + 1]
    with Path(path).open("a", encoding="utf-8") as out:
        out.write(
            json.dumps(
                {
                    "exit_code": 3 if table == "raw.blocked" else 0,
                    "pid": os.getpid(),
                    "argv": argv,
                    "marker": os.environ.get("ZAMBONI_TEST_MARKER"),
                    "table_config": Path(argv[argv.index("--table-config") + 1]).read_text(
                        encoding="utf-8"
                    ),
                }
            )
            + "\n"
        )
    # Left behind on purpose: the next table on this worker must not see it.
    os.environ["ZAMBONI_TEST_MARKER"] = "left over from the previous table"
    print(f"maintained {table}")
    return 3 if table == "raw.blocked" else 0
