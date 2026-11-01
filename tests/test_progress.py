# SPDX-License-Identifier: Apache-2.0
"""`maintain(progress=...)`: a note as each unit of work starts (#232).

docs/maintenance-runtime.md §7 rule 6: an entry reports progress at every
operation boundary and within a long operation at its natural unit, because a
long table that reports nothing is indistinguishable from a hung one. These
tests hold `maintain()` to that, so the default worker entry can pass it on.
"""

from __future__ import annotations

import json
import logging
import re

import pytest

from zamboni import maintain
from zamboni.config import CompactionConfig
from zamboni.maintenance import RUNBOOK_ORDER, Outcome

GROUP = re.compile(r"^(?P<table>[\w.]+): rewriting group (?P<n>\d+)/(?P<total>\d+): ")


def config(tmp_path, *tables: str):
    path = tmp_path / "table-config.json"
    namespaces: dict[str, dict] = {}
    for name in tables:
        ns, table = name.split(".")
        namespaces.setdefault(ns, {"tables": {}})["tables"][table] = {}
    path.write_text(json.dumps({"version": 2, "warehouse": "local", "namespaces": namespaces}))
    return path


def operation_notes(table: str) -> list[str]:
    """What `maintain` says as each operation starts, derived from its order."""
    n = len(RUNBOOK_ORDER)
    return [f"{table}: {op.value} ({i}/{n})" for i, op in enumerate(RUNBOOK_ORDER, 1)]


def groups(notes: list[str]) -> list[re.Match]:
    return [m for m in map(GROUP.match, notes) if m]


@pytest.mark.parametrize("partial_progress", [False, True], ids=["atomic", "partial-progress"])
def test_a_committing_run_reports_every_operation_and_every_rewrite_group(
    session, partitioned, tmp_path, partial_progress
):
    notes: list[str] = []
    report = maintain(
        session,
        table_config=config(tmp_path, "db.partitioned"),
        commit=True,
        base_config=CompactionConfig(partial_progress=partial_progress),
        progress=notes.append,
    )
    assert report.exit_code == 0, report.describe()

    assert [n for n in notes if not GROUP.match(n)] == operation_notes("db.partitioned")
    compact = next(o for o in report.outcomes if o.operation.value == "compact")
    rewritten = len(compact.result.groups) + len(compact.result.evolved)
    assert rewritten >= 2, "the fixture must give compaction more than one group"
    announced = groups(notes)
    assert [int(m["n"]) for m in announced] == list(range(1, rewritten + 1))
    assert {int(m["total"]) for m in announced} == {rewritten}
    # Inside compaction: straight after its own note, before the next operation's.
    start = notes.index(next(n for n in notes if n.startswith("db.partitioned: compact (")))
    assert notes[start + 1 : start + 1 + rewritten] == [m.string for m in announced]


def test_each_operation_is_reported_before_it_runs(session, partitioned, tmp_path):
    """The note is the start of the work, not its end: `observer` already hears
    the end, and a stall timer reset only afterwards would fire mid-operation."""
    events: list[str | Outcome] = []
    maintain(
        session,
        table_config=config(tmp_path, "db.partitioned"),
        commit=True,
        progress=events.append,
        observer=events.append,
    )
    started = [e for e in events if isinstance(e, Outcome) or not GROUP.match(e)]
    assert len(started) == 2 * len(RUNBOOK_ORDER)
    for note, outcome in zip(started[::2], started[1::2], strict=True):
        assert isinstance(note, str) and isinstance(outcome, Outcome), (note, outcome)
        assert note.startswith(f"db.partitioned: {outcome.operation.value} (")


def test_a_preview_reports_every_operation_and_no_group(session, partitioned, tmp_path):
    """A preview rewrites nothing, so there is no group to announce."""
    notes: list[str] = []
    maintain(session, table_config=config(tmp_path, "db.partitioned"), progress=notes.append)
    assert notes == operation_notes("db.partitioned")


def test_every_table_is_reported_in_the_order_it_runs(
    session, partitioned, unpartitioned, tmp_path
):
    notes: list[str] = []
    maintain(
        session,
        table_config=config(tmp_path, "db.partitioned", "db.unpartitioned"),
        progress=notes.append,
    )
    assert notes == operation_notes("db.partitioned") + operation_notes("db.unpartitioned")


def test_reporting_progress_changes_nothing_in_the_report(session, partitioned, tmp_path):
    path = config(tmp_path, "db.partitioned")
    quiet = maintain(session, table_config=path)
    told = maintain(session, table_config=path, progress=lambda note: None)
    assert [o.as_dict() for o in told.outcomes] == [o.as_dict() for o in quiet.outcomes]


def test_a_progress_that_raises_neither_stops_nor_fails_the_run(
    session, partitioned, tmp_path, caplog
):
    calls = []

    def broken(note: str) -> None:
        calls.append(note)
        raise BrokenPipeError("the runtime went away")

    with caplog.at_level(logging.WARNING, logger="zamboni.maintenance"):
        report = maintain(
            session,
            table_config=config(tmp_path, "db.partitioned"),
            commit=True,
            progress=broken,
        )

    assert report.exit_code == 0, report.describe()
    assert report.stopped is None
    assert [o.operation for o in report.outcomes] == list(RUNBOOK_ORDER)
    assert groups(calls), "it went on being told, groups included"
    assert [r.getMessage() for r in caplog.records] == ["progress raised; the run carries on"]


def test_a_group_a_stop_withheld_is_not_announced(session, partitioned, tmp_path):
    """The note says a group is starting, so it comes after `should_stop` lets it."""
    notes: list[str] = []
    report = maintain(
        session,
        table_config=config(tmp_path, "db.partitioned"),
        commit=True,
        progress=notes.append,
        should_stop=lambda: bool(groups(notes)),
    )
    compact = report.outcomes[0]
    assert compact.result.not_reached >= 1
    assert len(groups(notes)) == len(compact.result.groups) == 1
