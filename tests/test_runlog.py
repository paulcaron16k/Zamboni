# SPDX-License-Identifier: Apache-2.0
"""Reading a fleet's run summaries back.

The other end of `maintenance --json`, and the command the weekly review in
docs/devops.md runs. These tests are mostly about the two things that make a run
log different from a report: it is written by a machine nobody is watching, so
it *will* contain a half-written record; and it spans warehouses, where a table
identifier is no longer unique.
"""

from __future__ import annotations

import json

import pytest

from zamboni.cli import main
from zamboni.runlog import read_records, run_logs, summarise, summarise_logs


def record(warehouse="acme", *, considered=6, skipped=3, tables=("db.events",), **extra):
    doc = {
        "versions": {"zamboni": "0.5.1", "pyiceberg": "0.12.0", "python": "3.13.14"},
        "exit_code": 0,
        "warehouse": warehouse,
        "started_at": "2026-09-20T02:00:00Z",
        "ended_at": "2026-09-20T02:00:12Z",
        "duration_seconds": 12.0,
        "tables": list(tables),
        "failures": 0,
        "counters": {
            "tables": len(tables),
            "considered": considered,
            "skipped": skipped,
            "maintained": considered - skipped,
            "failed": 0,
            "skip_rate": skipped / considered if considered else None,
        },
        "outcomes": [],
    }
    doc.update(extra)
    return doc


def write(path, *records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return path


def test_units_of_work_sum_across_nights(tmp_path):
    """A week of runs is a week of work: the counters add up."""
    log = write(tmp_path / "a.jsonl", record(), record(), record())

    summary = summarise_logs([log])

    assert summary.runs == 3
    assert summary.counters.considered == 18
    assert summary.counters.skipped == 9
    assert summary.counters.skip_rate == 0.5


def test_tables_are_counted_per_warehouse_not_by_bare_identifier(tmp_path):
    """`db.events` exists in every tenant of a multi-tenant deployment, so a
    set of identifiers collapses them and reports a forty-table fleet as four.

    Found by running the real command over two warehouses that both have one.
    """
    log = write(
        tmp_path / "a.jsonl",
        record("acme", tables=("db.events", "db.orders")),
        record("globex", tables=("db.events", "db.audit", "db.ledger")),
    )

    summary = summarise_logs([log])

    assert summary.counters.tables == 5, "two acme tables and three globex ones"
    assert [w.counters.tables for w in summary.warehouses] == [2, 3]


def test_the_same_table_on_many_nights_is_counted_once(tmp_path):
    """The other half of the same property: summing would report four."""
    log = write(tmp_path / "a.jsonl", record(), record(), record(), record())

    assert summarise_logs([log]).counters.tables == 1


def test_a_half_written_record_is_counted_not_raised_on(tmp_path):
    """A run log is written by a cron line on a machine nobody is watching, and
    it will contain the night the box was rebooted. Refusing to read the series
    over one bad line makes the tool useless exactly when it is needed."""
    log = tmp_path / "a.jsonl"
    log.write_text(json.dumps(record()) + "\n" + '{"counters": {"considered": 3, "sk\n')

    summary = summarise_logs([log])

    assert summary.runs == 1, "the good record still counted"
    assert summary.unreadable == 1
    assert "unreadable records       1" in summary.describe()


def test_a_json_line_that_is_not_a_run_record_is_unreadable_too(tmp_path):
    """Something else's log rotated into the same directory."""
    log = tmp_path / "a.jsonl"
    log.write_text('{"level":"info","msg":"hello"}\n[]\n"text"\n')

    records, unreadable = read_records([log])

    assert records == []
    assert unreadable == 3


def test_blank_lines_are_not_corruption(tmp_path):
    log = tmp_path / "a.jsonl"
    log.write_text(json.dumps(record()) + "\n\n\n")

    summary = summarise_logs([log])

    assert (summary.runs, summary.unreadable) == (1, 0)


def test_a_missing_file_is_reported_rather_than_raised(tmp_path):
    summary = summarise_logs([tmp_path / "nothing.jsonl"])

    assert summary.runs == 0
    assert summary.unreadable == 1
    assert summary.describe() == "no runs found, 1 unreadable record(s)"


def test_a_directory_reads_the_run_logs_in_it(tmp_path):
    """The documented weekly command names a directory, not a shell glob an
    operator has to get right."""
    write(tmp_path / "acme.jsonl", record("acme"))
    write(tmp_path / "globex.jsonl", record("globex"))
    (tmp_path / "notes.txt").write_text("not a run log")
    (tmp_path / "sub").mkdir()

    assert [p.name for p in run_logs([tmp_path])] == ["acme.jsonl", "globex.jsonl"]
    assert summarise_logs([tmp_path]).runs == 2


def test_a_named_file_is_read_whatever_it_is_called(tmp_path):
    log = write(tmp_path / "zamboni.log", record())

    assert summarise_logs([log]).runs == 1


def test_failures_are_counted_per_run_and_the_worst_code_survives(tmp_path):
    log = write(
        tmp_path / "a.jsonl",
        record(),
        record(exit_code=3),
        record(exit_code=4),
    )

    summary = summarise_logs([log])

    assert summary.failed_runs == 2
    assert summary.worst_exit_code == 4
    assert "runs with failures       2 (worst exit 4)" in summary.describe()


def test_each_warehouse_is_broken_out(tmp_path):
    """ "The fleet is at 50%" is not actionable; "globex is at 5%" is."""
    log = write(
        tmp_path / "a.jsonl",
        record("acme", considered=6, skipped=3),
        record("globex", considered=6, skipped=0, exit_code=3),
    )

    summary = summarise_logs([log])

    names = [w.warehouse for w in summary.warehouses]
    assert names == ["acme", "globex"], "sorted, so two weeks' output can be diffed"
    globex = summary.warehouses[1]
    assert globex.counters.skip_rate == 0.0
    assert globex.failed_runs == 1
    assert "globex" in summary.describe()


def test_a_series_spanning_two_builds_says_so(tmp_path):
    """Which operations are even attempted depends on the installed PyIceberg,
    so a figure that moved may be a library change and not a workload change.
    Averaging over that silently is how a gate gets answered wrongly."""
    older = record()
    older["versions"] = {"zamboni": "0.5.0", "pyiceberg": "0.12.0", "python": "3.13.14"}
    log = write(tmp_path / "a.jsonl", record(), older)

    described = summarise_logs([log]).describe()

    assert "2 version(s)" in described
    assert "zamboni 0.5.0" in described and "zamboni 0.5.1" in described


def test_one_build_is_not_worth_a_line(tmp_path):
    """Noise in a weekly report is how a weekly report stops being read."""
    log = write(tmp_path / "a.jsonl", record(), record())

    assert "version(s)" not in summarise_logs([log]).describe()


def test_an_empty_series_says_so_rather_than_dividing(tmp_path):
    summary = summarise([])

    assert summary.runs == 0
    assert summary.counters.skip_rate is None
    assert summary.describe() == "no runs found"


def test_the_aggregate_serialises(tmp_path):
    """A dashboard keys on these names."""
    log = write(tmp_path / "a.jsonl", record("acme"), record("globex"))

    doc = summarise_logs([log]).as_dict()

    assert json.loads(json.dumps(doc)) == doc
    assert doc["counters"]["skip_rate"] == 0.5
    assert [w["warehouse"] for w in doc["warehouses"]] == ["acme", "globex"]


# -- the verb ------------------------------------------------------------


def test_the_verb_reports_the_fleet(tmp_path, capsys):
    write(tmp_path / "acme.jsonl", record("acme"), record("acme"))

    assert main(["runs", str(tmp_path)]) == 0

    out = capsys.readouterr().out
    assert "2 run(s)" in out
    assert "50% of the work had no input" in out


def test_the_verb_exits_zero_on_a_series_full_of_failures(tmp_path, capsys):
    """A report that exits non-zero for successfully telling you bad news gets
    wrapped in `|| true` and then ignored."""
    write(tmp_path / "a.jsonl", record(exit_code=4), record(exit_code=3))

    assert main(["runs", str(tmp_path)]) == 0

    assert "runs with failures       2" in capsys.readouterr().out


def test_no_run_logs_at_all_is_a_usage_error(tmp_path, capsys):
    """A path matching nothing is a mistyped path far more often than it is a
    fleet that did not run."""
    assert main(["runs", str(tmp_path)]) == 2

    assert "no run summaries found" in capsys.readouterr().err


def test_the_verb_can_emit_json(tmp_path, capsys):
    write(tmp_path / "a.jsonl", record())

    assert main(["runs", str(tmp_path), "--json"]) == 0

    assert json.loads(capsys.readouterr().out)["runs"] == 1


@pytest.mark.parametrize("flag", ["--json"])
def test_the_verb_needs_no_catalog(tmp_path, capsys, monkeypatch, flag):
    """It reads files. Requiring a warehouse to read last week's logs would put
    the weekly review behind a working connection to production."""
    from zamboni import cli

    def explode(*args, **kwargs):  # pragma: no cover - must not be reached
        raise AssertionError("the runs verb built a catalog session")

    monkeypatch.setattr(cli, "_session_from", explode)
    write(tmp_path / "a.jsonl", record())

    assert main(["runs", str(tmp_path), flag]) == 0
