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

import dataclasses
import json
import os
import signal
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from zamboni.pool import WorkerContext, run_cli, run_table
from zamboni.scheduler import PressureCheck, WorkItem


def record(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    return run_cli(item, context, main=_cli)


def progress(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Report progress, then end as ``item.table`` says: die, or return a record."""
    context.progress("first")
    context.progress(f"second {item.table}")
    if item.table == "raw.die":
        os.kill(os.getpid(), signal.SIGKILL)
    return {"exit_code": 0, "commit": item.commit}


def recorded(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """The real default entry, every note it reports also kept in its record."""
    notes: list[str] = []

    def report(note: str) -> None:
        notes.append(note)
        context.report(note)

    record = run_table(item, dataclasses.replace(context, report=report))
    return {**record, "notes": notes}


def own_session_health(check: PressureCheck, context: WorkerContext) -> dict[str, Any]:
    """A health entry that builds its catalog its own way: from a variable no
    profile or flag knows, as an application with its own store would."""
    from zamboni import CatalogSession
    from zamboni.health import check_tables

    session = CatalogSession.for_local(warehouse_path=os.environ["TEST_OWN_WAREHOUSE"])
    try:
        return check_tables(
            session.catalog, sorted(check.table_config.tables), warehouse=check.warehouse
        )
    finally:
        session.close()


def health_that_raises(check: PressureCheck, context: WorkerContext) -> dict[str, Any]:
    raise RuntimeError("the catalog is down")


def health_that_exits(check: PressureCheck, context: WorkerContext) -> dict[str, Any]:
    raise SystemExit(5)


def health_without_tables(check: PressureCheck, context: WorkerContext) -> dict[str, Any]:
    """A run record, not a health record: what a table entry returns."""
    return {"exit_code": 0}


def sleeps(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Writes its pid where ``ZAMBONI_TEST_PID_FILE`` says, then sleeps far past
    any test: a table still running when its parent is killed."""
    Path(os.environ["ZAMBONI_TEST_PID_FILE"]).write_text(str(os.getpid()), encoding="ascii")
    time.sleep(600)
    return {"exit_code": 0}


def goes_quiet(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Reports once, then nothing: hung, as far as anyone outside can tell."""
    context.progress("starting, then silence")
    time.sleep(600)
    return {"exit_code": 0}


def keeps_talking(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Reports every 0.1 s for ``ZAMBONI_TEST_TALK_SECONDS``, then succeeds: a
    long table that is plainly still working."""
    until = time.monotonic() + float(os.environ["ZAMBONI_TEST_TALK_SECONDS"])
    while time.monotonic() < until:
        context.progress("still rewriting")
        time.sleep(0.1)
    return {"exit_code": 0}


def health_that_hangs(check: PressureCheck, context: WorkerContext) -> dict[str, Any]:
    time.sleep(600)
    return {"tables": {}}


def not_a_record(item: WorkItem, context: WorkerContext) -> Any:
    """Breaks rule 3: returns what a CLI ``main`` would, an exit code."""
    return 0


def _cli(argv: list[str], *, progress: Callable[[str], None] | None = None) -> int:
    """Write what the worker saw into the run record, then behave per table."""
    table = argv[1]
    if table == "raw.die":
        os.kill(os.getpid(), signal.SIGKILL)
    if table == "raw.boom":
        raise RuntimeError("boom")
    if table == "raw.usage":
        sys.exit(2)
    code = {"raw.blocked": 3, "raw.aborted": 4}.get(table, 0)
    path = argv[argv.index("--json") + 1]
    with Path(path).open("a", encoding="utf-8") as out:
        out.write(
            json.dumps(
                {
                    "exit_code": code,
                    "pid": os.getpid(),
                    "nice": os.getpriority(os.PRIO_PROCESS, 0),
                    "oom_score_adj": _oom_score_adj(),
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
    return code


def _oom_score_adj() -> int | None:
    try:
        return int(Path("/proc/self/oom_score_adj").read_text(encoding="ascii"))
    except FileNotFoundError:  # not Linux
        return None
