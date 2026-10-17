# SPDX-License-Identifier: Apache-2.0
"""`maintain()` can be stopped, and returns what it completed (ZMBNI-201).

Asked for by an integrator (ELT-605) whose only lever was raising from
`observer`, which stopped the run and took every collected outcome with it. A
stop is checked before each table, before each operation and between
compaction's rewrite groups -- each a point where every commit before it is
whole -- and never inside a commit.

Each test asserts three things: what completed is in the report, nothing after
the stop ran, and no commit was cut short.
"""

from __future__ import annotations

import datetime as dt
import json

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import NestedField, TimestamptzType

from zamboni import CatalogSession, maintain
from zamboni import committer as committer_module
from zamboni.compactor import TableCompactor
from zamboni.health import COMPACT_INCOMPLETE
from zamboni.maintainers import Operation
from zamboni.maintenance import RUNBOOK_ORDER

SCHEMA = Schema(NestedField(1, "ts", TimestamptzType(), required=False))
ARROW = pa.schema([pa.field("ts", pa.timestamp("us", tz="UTC"))])
SPEC = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="d"))

#: Two closed day partitions of three one-row files each: two compaction groups,
#: both outside the default hold.
TWO_GROUPS = {5: 3, 6: 3}


@pytest.fixture
def session(tmp_path):
    root = tmp_path / "wh"
    root.mkdir()
    sess = CatalogSession.for_local(warehouse_path=str(root))
    sess.catalog.create_namespace("db")
    yield sess
    sess.close()


def daily_table(session, name: str, files: dict[int, int]):
    """One day partition per key, `days ago -> how many one-row files`."""
    tbl = session.catalog.create_table(
        f"db.{name}", schema=SCHEMA, partition_spec=SPEC, properties={"format-version": "2"}
    )
    now = dt.datetime.now(dt.UTC)
    for offset, count in files.items():
        for _ in range(count):
            tbl.append(pa.table({"ts": [now - dt.timedelta(days=offset)]}, schema=ARROW))
    return tbl


def config(tmp_path, *tables: str):
    path = tmp_path / "table-config.json"
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "namespaces": {"db": {"tables": {t: {} for t in tables}}},
            }
        )
    )
    return path


def snapshot_ids(session, table: str) -> list[int]:
    return [s.snapshot_id for s in session.catalog.load_table(f"db.{table}").snapshots()]


def files_per_partition(session, table: str) -> list[int]:
    counts: dict[object, int] = {}
    for task in session.catalog.load_table(f"db.{table}").scan().plan_files():
        key = tuple(task.file.partition)
        counts[key] = counts.get(key, 0) + 1
    return sorted(counts.values())


def rows(session, table: str) -> int:
    return session.catalog.load_table(f"db.{table}").scan().to_arrow().num_rows


@pytest.fixture
def commits(monkeypatch):
    """Record every commit, and fail if `should_stop` is asked during one."""
    state = {"inside": False, "count": 0}
    real = committer_module.ReplaceCommitter.commit

    def commit(self, *args, **kwargs):
        state["inside"] = True
        try:
            return real(self, *args, **kwargs)
        finally:
            state["inside"] = False
            state["count"] += 1

    monkeypatch.setattr(committer_module.ReplaceCommitter, "commit", commit)
    return state


def guarded(commits, answer):
    """A `should_stop` that asserts it is never asked mid-commit."""

    def should_stop() -> bool:
        assert not commits["inside"], "should_stop was asked inside a commit"
        return answer()

    return should_stop


def test_a_stop_before_the_first_table_runs_nothing_and_says_so(session, tmp_path, commits):
    daily_table(session, "a", TWO_GROUPS)
    daily_table(session, "b", TWO_GROUPS)
    before = {t: snapshot_ids(session, t) for t in ("a", "b")}

    report = maintain(
        session,
        table_config=config(tmp_path, "a", "b"),
        commit=True,
        should_stop=guarded(commits, lambda: True),
    )

    assert report.outcomes == ()
    assert report.stopped is not None
    assert report.stopped.not_run == tuple((t, o) for t in ("db.a", "db.b") for o in RUNBOOK_ORDER)
    assert {t: snapshot_ids(session, t) for t in ("a", "b")} == before
    assert report.exit_code == 0, "a stop is not a failure"
    assert report.counters.considered == 0, "work never started is not counted"


def ran(report) -> list[tuple[str, Operation]]:
    return [(o.table, o.operation) for o in report.outcomes]


def stop_after(seen: list, operation: Operation):
    """True once `operation` has produced an outcome."""
    return lambda: any(o.operation is operation for o in seen)


#: Where a stop after compaction has committed takes effect: after the clean-up
#: it owes, which is the last of the two in runbook order.
AFTER_CLEANUP = (
    max(
        RUNBOOK_ORDER.index(o)
        for o in (Operation.REMOVE_DANGLING_DELETES, Operation.REWRITE_MANIFESTS)
    )
    + 1
)


def test_a_stop_between_operations_keeps_what_ran_and_starts_nothing_more(
    session, tmp_path, commits
):
    daily_table(session, "a", TWO_GROUPS)
    daily_table(session, "b", TWO_GROUPS)
    seen: list = []
    stop_at = RUNBOOK_ORDER.index(Operation.EXPIRE) + 1

    report = maintain(
        session,
        table_config=config(tmp_path, "a", "b"),
        commit=True,
        observer=seen.append,
        should_stop=guarded(commits, stop_after(seen, Operation.EXPIRE)),
    )

    assert ran(report) == [("db.a", o) for o in RUNBOOK_ORDER[:stop_at]]
    assert report.stopped is not None
    assert report.stopped.not_run == tuple(
        [("db.a", o) for o in RUNBOOK_ORDER[stop_at:]] + [("db.b", o) for o in RUNBOOK_ORDER]
    )
    assert files_per_partition(session, "a") == [1, 1], "the completed compaction committed"
    assert files_per_partition(session, "b") == [3, 3], "nothing ran on the second table"
    assert rows(session, "a") == 6
    assert report.as_dict()["stopped"]["not_run"][0] == {
        "table": "db.a",
        "operation": RUNBOOK_ORDER[stop_at].value,
    }


def test_a_stop_after_compaction_committed_waits_for_its_clean_up(session, tmp_path, commits):
    """Stopped between compaction's commit and the operations that clean up
    after it, the next run would read compaction's stamp as "nothing written
    since" and skip them -- on a quiet table, indefinitely. So the stop waits
    for them, and only them."""
    daily_table(session, "a", TWO_GROUPS)
    daily_table(session, "b", TWO_GROUPS)
    seen: list = []

    report = maintain(
        session,
        table_config=config(tmp_path, "a", "b"),
        commit=True,
        observer=seen.append,
        should_stop=guarded(commits, stop_after(seen, Operation.COMPACT)),
    )

    assert ran(report) == [("db.a", o) for o in RUNBOOK_ORDER[:AFTER_CLEANUP]]
    for operation in (Operation.REMOVE_DANGLING_DELETES, Operation.REWRITE_MANIFESTS):
        detail = next(o for o in report.outcomes if o.operation is operation).detail
        assert "nothing written since" not in detail, f"{operation.value} was skipped"
    assert report.stopped is not None
    assert report.stopped.not_run[0] == ("db.a", RUNBOOK_ORDER[AFTER_CLEANUP])
    assert files_per_partition(session, "b") == [3, 3]


@pytest.mark.parametrize("partial_progress", [False, True], ids=["atomic", "partial-progress"])
def test_a_stop_between_compaction_groups_commits_what_was_rewritten(
    session, tmp_path, monkeypatch, commits, partial_progress
):
    """The default (atomic) mode rewrites every group before one commit, so a
    stop there commits the groups already rewritten rather than discarding them.
    Either way the commit is marked unfinished, and the next run compacts the
    rest instead of reading "nothing written since" (ZMBNI-196)."""
    from zamboni.config import CompactionConfig

    daily_table(session, "a", TWO_GROUPS)
    rewritten = {"n": 0}
    real = TableCompactor._rewrite_group

    def counting(self, tbl, group):
        out = real(self, tbl, group)
        rewritten["n"] += 1
        return out

    monkeypatch.setattr(TableCompactor, "_rewrite_group", counting)

    report = maintain(
        session,
        table_config=config(tmp_path, "a"),
        commit=True,
        base_config=CompactionConfig(partial_progress=partial_progress),
        should_stop=guarded(commits, lambda: rewritten["n"] >= 1),
    )

    compact = report.outcomes[0]
    assert compact.operation is Operation.COMPACT
    assert compact.result.not_reached == 1
    assert len(compact.result.groups) == 1
    assert "1 group(s) not reached" in compact.detail
    assert commits["count"] == 1, "exactly the rewritten group was committed"
    assert files_per_partition(session, "a") == [1, 3]
    assert rows(session, "a") == 6, "a stop never loses or duplicates a row"
    newest = session.catalog.load_table("db.a").current_snapshot()
    assert newest.summary.additional_properties.get(COMPACT_INCOMPLETE) == (
        "1 group(s) still to commit"
    )
    assert report.stopped is not None
    assert report.stopped.not_run[0] == ("db.a", RUNBOOK_ORDER[AFTER_CLEANUP])

    resumed = maintain(session, table_config=config(tmp_path, "a"), commit=True)
    assert resumed.stopped is None
    assert files_per_partition(session, "a") == [1, 1], "the next run compacted the rest"
    for operation in (Operation.REMOVE_DANGLING_DELETES, Operation.REWRITE_MANIFESTS):
        detail = next(o for o in resumed.outcomes if o.operation is operation).detail
        assert "nothing written since" not in detail, (
            f"{operation.value} was skipped on a run whose compaction committed"
        )


def test_a_stop_before_any_group_commits_nothing(session, tmp_path, commits):
    daily_table(session, "a", TWO_GROUPS)
    calls = {"n": 0}

    def third_call_on() -> bool:
        # 1: before the table, 2: before compact, 3: before the first group.
        calls["n"] += 1
        return calls["n"] >= 3

    report = maintain(
        session,
        table_config=config(tmp_path, "a"),
        commit=True,
        should_stop=guarded(commits, third_call_on),
    )

    assert report.outcomes[0].result.not_reached == 2
    assert commits["count"] == 0
    assert files_per_partition(session, "a") == [3, 3]


def test_an_observer_that_raises_stops_the_run_and_the_report_survives(session, tmp_path, caplog):
    """Raising from the observer was ELT-605's only stop lever. It still stops
    the run, and the outcomes collected so far are returned rather than lost."""
    daily_table(session, "a", TWO_GROUPS)

    def observer(outcome):
        raise RuntimeError("operator pressed stop")

    report = maintain(session, table_config=config(tmp_path, "a"), commit=True, observer=observer)

    assert [o.operation for o in report.outcomes] == list(RUNBOOK_ORDER[:AFTER_CLEANUP]), (
        "compaction committed, so the stop waited for its clean-up"
    )
    assert report.stopped is not None
    assert report.stopped.reason == "observer raised RuntimeError: operator pressed stop"
    assert "observer raised" in caplog.text
    assert files_per_partition(session, "a") == [1, 1]


def test_a_should_stop_that_raises_is_read_as_stop(session, tmp_path):
    daily_table(session, "a", TWO_GROUPS)

    def broken() -> bool:
        raise ValueError("lost the flag")

    report = maintain(session, table_config=config(tmp_path, "a"), commit=True, should_stop=broken)

    assert report.outcomes == ()
    assert report.stopped is not None
    assert report.stopped.reason == "should_stop() raised ValueError: lost the flag"


def test_a_run_that_is_never_stopped_reports_no_stop(session, tmp_path):
    daily_table(session, "a", TWO_GROUPS)

    report = maintain(
        session, table_config=config(tmp_path, "a"), commit=True, should_stop=lambda: False
    )

    assert report.stopped is None
    assert report.as_dict()["stopped"] is None
    assert [o.operation for o in report.outcomes] == list(RUNBOOK_ORDER)


def test_an_observer_raising_on_the_last_outcome_stopped_nothing(session, tmp_path):
    """Nothing was left to start, so the run completed; it is not reported as
    stopped, though the exception is still logged."""
    daily_table(session, "a", TWO_GROUPS)
    last = RUNBOOK_ORDER[-1]

    def observer(outcome):
        if outcome.operation is last:
            raise RuntimeError("late")

    report = maintain(session, table_config=config(tmp_path, "a"), commit=True, observer=observer)

    assert report.stopped is None
    assert report.outcomes[-1].operation is last
