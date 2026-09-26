# SPDX-License-Identifier: Apache-2.0
"""Iceberg's metrics vocabulary, and the reports built from real snapshots.

Two kinds of test here, and the split is deliberate.

The first kind pins the **mapping**: names and units read from
`CommitMetricsResult.java`, `CommitMetrics.java` and `SnapshotSummary.java` on
`apache/iceberg@main`. These assert the conversions that are easy to get wrong
-- five summary properties are *renamed* on their way to a metric name, and four
more swap "position" for "positional".

The second kind runs a real operation and builds a report from the snapshot it
committed. That is what catches PyIceberg changing what it writes into a
summary, which no amount of pinning constants would notice.
"""

from __future__ import annotations

import json

import pytest

from zamboni.compactor import TableCompactor
from zamboni.config import CompactionConfig
from zamboni.manifests import ManifestRewriter
from zamboni.metrics import (
    ADDED_FILES_SIZE_BYTES,
    ADDED_POSITIONAL_DELETE_FILES,
    BYTES,
    COUNT,
    MANIFEST_ENTRIES_PROCESSED,
    MANIFESTS_CREATED,
    MANIFESTS_KEPT,
    MANIFESTS_REPLACED,
    OPERATION_STAMP,
    REMOVED_DATA_FILES,
    REMOVED_FILES_SIZE_BYTES,
    SNAPSHOTS_IN_OPERATION,
    SUMMARY_TO_METRIC,
    TOTAL_DURATION,
    CommitReport,
    CounterResult,
    TimerResult,
    commit_report,
    commit_reports,
    metrics_from_summary,
    nanos,
    reclaim_metrics,
)

# -- the wire shapes, which a plausible guess gets wrong ------------------


def test_a_counter_is_unit_and_value():
    """`{unit, value}`, not `{type, value}`. Asserted because the wrong shape is
    the one a reconstruction produces, and a body the catalog rejects is a
    metric nobody ever sees."""
    assert CounterResult(unit=COUNT, value=3).as_dict() == {"unit": "count", "value": 3}


def test_a_timer_is_time_unit_count_and_total_duration():
    assert nanos(1_234).as_dict() == {
        "time-unit": "nanoseconds",
        "count": 1,
        "total-duration": 1_234,
    }


def test_the_report_carries_every_field_the_spec_requires():
    report = CommitReport(
        table_name="db.events",
        snapshot_id=7,
        sequence_number=3,
        operation="replace",
        metrics={REMOVED_DATA_FILES: CounterResult(unit=COUNT, value=6)},
    )

    doc = report.as_dict()

    assert set(doc) >= {"table-name", "snapshot-id", "sequence-number", "operation", "metrics"}
    assert doc["metrics"][REMOVED_DATA_FILES] == {"unit": "count", "value": 6}
    assert json.loads(json.dumps(doc)) == doc


def test_empty_metadata_is_omitted_rather_than_sent_as_an_empty_map():
    """`metadata` is optional, and `{}` says "this producer records no
    metadata" -- a different claim from having none this time."""
    report = CommitReport("db.events", 7, 3, "replace")

    assert "metadata" not in report.as_dict()


# -- the mapping ---------------------------------------------------------


def test_the_five_renamed_properties_are_renamed():
    """The reason this is a table and not a loop over the summary. Copying the
    summary key through as the metric name looks right and is wrong here."""
    summary = {
        "deleted-data-files": "6",
        "deleted-records": "12",
        "added-files-size": "584",
        "removed-files-size": "3372",
        "total-files-size": "584",
    }

    metrics = metrics_from_summary(summary)

    assert REMOVED_DATA_FILES in metrics, "deleted-data-files is removed-data-files"
    assert "removed-records" in metrics
    assert ADDED_FILES_SIZE_BYTES in metrics, "added-files-size gains a -bytes suffix"
    assert not set(metrics) & set(summary), "no summary spelling survived as a metric name"


def test_position_becomes_positional():
    """Four more that differ by one syllable between the summary and the metric."""
    metrics = metrics_from_summary({"added-position-delete-files": "2"})

    assert ADDED_POSITIONAL_DELETE_FILES in metrics
    assert "added-position-delete-files" not in metrics


def test_only_the_size_counters_are_in_bytes():
    """Per `CommitMetricsResult.from`, which passes Unit.BYTES for exactly three."""
    in_bytes = {name for _, name, unit in SUMMARY_TO_METRIC if unit == BYTES}

    assert in_bytes == {
        ADDED_FILES_SIZE_BYTES,
        REMOVED_FILES_SIZE_BYTES,
        "total-files-size-bytes",
    }
    assert all(unit in (COUNT, BYTES) for _, _, unit in SUMMARY_TO_METRIC)


def test_the_mapping_is_one_to_one():
    """A property mapped twice, or two properties onto one metric, would make a
    counter quietly depend on dict ordering."""
    props = [prop for prop, _, _ in SUMMARY_TO_METRIC]
    names = [name for _, name, _ in SUMMARY_TO_METRIC]

    assert len(set(props)) == len(props)
    assert len(set(names)) == len(names)


def test_a_missing_property_is_absent_not_zero():
    """ "This commit removed no data files" and "this writer does not report
    removed data files" are different facts. Java returns null; a zero here
    would make a PyIceberg table look like it claimed something it never did."""
    metrics = metrics_from_summary({"added-data-files": "1"})

    assert REMOVED_DATA_FILES not in metrics
    assert metrics["added-data-files"].value == 1


def test_a_non_numeric_value_is_skipped_rather_than_crashing():
    metrics = metrics_from_summary({"added-data-files": "lots", "total-records": "6"})

    assert "added-data-files" not in metrics
    assert metrics["total-records"].value == 6


# -- built from a real commit --------------------------------------------


def test_a_real_compaction_reports_what_the_summary_said(session, unpartitioned):
    """The test that catches PyIceberg changing what it writes, which pinning
    constants never would."""
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()
    table = session.catalog.load_table("db.unpartitioned")

    reports = commit_reports(result, table.metadata.snapshots, duration_ns=1_234)
    assert len(reports) == 1
    report = reports[0]

    snapshot = next(s for s in table.metadata.snapshots if s.snapshot_id == report.snapshot_id)
    summary = snapshot.summary.additional_properties
    assert report.metrics[REMOVED_DATA_FILES].value == int(summary["deleted-data-files"])
    assert report.metrics[ADDED_FILES_SIZE_BYTES].value == int(summary["added-files-size"])
    assert report.metrics[ADDED_FILES_SIZE_BYTES].unit == BYTES
    assert report.sequence_number == snapshot.sequence_number


def test_the_operation_is_icebergs_and_the_verb_is_metadata(session, unpartitioned):
    """`operation` is Iceberg's enumeration -- a consumer grouping by it expects
    Iceberg's values -- so Zamboni's verb goes in the free-form metadata."""
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()
    table = session.catalog.load_table("db.unpartitioned")

    report = commit_reports(result, table.metadata.snapshots)[0]

    assert report.operation == "replace", "the Iceberg snapshot operation"
    assert report.metadata[OPERATION_STAMP] not in ("", None)
    assert not any(name.startswith("zamboni.") for name in report.metrics), (
        "metric names stay unprefixed and standard"
    )


def test_the_metadata_stamp_agrees_with_the_snapshot_it_describes(session, unpartitioned):
    """The committer writes "compaction" where `Operation.COMPACT.value` is
    "compact" (ZMBNI-135). Until that is settled the report must say what the
    snapshot says, or the two cannot be joined."""
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()
    table = session.catalog.load_table("db.unpartitioned")

    report = commit_reports(result, table.metadata.snapshots)[0]
    snapshot = next(s for s in table.metadata.snapshots if s.snapshot_id == report.snapshot_id)

    assert (
        report.metadata[OPERATION_STAMP] == snapshot.summary.additional_properties[OPERATION_STAMP]
    )


def test_rewrite_manifests_supplies_the_counters_the_summary_lacks(session, unpartitioned):
    """Iceberg defines the four manifest counters and Java fills them from
    summary properties PyIceberg does not write. Zamboni's rewriter counts them
    itself, so this closes a real gap -- and the second assertion is what tells
    us to stop, if PyIceberg ever starts writing them."""
    table = session.catalog.load_table("db.unpartitioned")
    result = ManifestRewriter(min_input_manifests=2).run(table)
    table = session.catalog.load_table("db.unpartitioned")

    report = commit_reports(result, table.metadata.snapshots)[0]
    summary = next(
        s for s in table.metadata.snapshots if s.snapshot_id == report.snapshot_id
    ).summary.additional_properties

    for name in (MANIFESTS_CREATED, MANIFESTS_KEPT, MANIFESTS_REPLACED, MANIFEST_ENTRIES_PROCESSED):
        assert name in report.metrics, name
    assert not {
        "created-manifests-count",
        "kept-manifests-count",
        "replaced-manifests-count",
        "processed-manifest-entry-count",
    } & set(summary), "PyIceberg started writing these; stop supplying them by hand"


def test_manifest_totals_are_not_mapped(session, unpartitioned):
    """`manifests_before`/`after` are totals; Iceberg's three are a partition of
    what this commit touched. A conversion, not a rename -- so the totals have
    no metric and must not acquire one by accident."""
    table = session.catalog.load_table("db.unpartitioned")
    result = ManifestRewriter(min_input_manifests=2).run(table)
    table = session.catalog.load_table("db.unpartitioned")

    report = commit_reports(result, table.metadata.snapshots)[0]

    assert not [name for name in report.metrics if "before" in name or "after" in name]


# -- what does not produce a report --------------------------------------


def test_a_dry_run_reports_no_commit(session, unpartitioned):
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute(dry_run=True)
    table = session.catalog.load_table("db.unpartitioned")

    assert commit_reports(result, table.metadata.snapshots) == []


def test_a_snapshot_the_table_no_longer_has_is_skipped(session, unpartitioned):
    """Expiry can remove the snapshot between the commit and the report."""
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()

    assert commit_reports(result, []) == []


def test_a_snapshot_without_an_operation_makes_no_report():
    """`operation` is required by the spec. A body the catalog rejects is worse
    than no body, and substituting a placeholder would put an invented
    operation into someone's metrics store."""

    class Bare:
        snapshot_id = 7
        sequence_number = 1
        summary = None

    assert commit_report("db.events", Bare()) is None


# -- several commits from one operation ----------------------------------


def test_one_operation_can_be_several_commits(session, partitioned):
    """Compaction commits one snapshot per rewrite group unless asked for a
    single commit, and Iceberg's unit is the commit."""
    result = TableCompactor(session, "db.partitioned", CompactionConfig()).execute()
    table = session.catalog.load_table("db.partitioned")
    committed = result.as_dict()["snapshot_ids"]
    if len(committed) < 2:
        pytest.skip("this fixture compacted in a single commit")

    reports = commit_reports(result, table.metadata.snapshots, duration_ns=9_999)

    assert len(reports) == len(committed)
    assert all(TOTAL_DURATION not in r.metrics for r in reports), (
        "the operation's duration must not be copied onto every commit and trebled"
    )
    assert all(r.metadata[SNAPSHOTS_IN_OPERATION] == str(len(committed)) for r in reports), (
        "and the omission is visible rather than silent"
    )


# -- the operations Iceberg has no report type for -----------------------


def test_reclaim_uses_icebergs_primitives(session, unpartitioned):
    """No `CommitReport` and no defined names -- but expressed as CounterResult
    so a reclaim report type, if Iceberg defines one, is a mapping."""
    from zamboni.expire import RetentionPolicy, SnapshotExpirer

    result = SnapshotExpirer(RetentionPolicy(), dry_run=True).run(unpartitioned)

    metrics = reclaim_metrics(result, duration_ns=500)

    assert metrics, "something was counted"
    assert all(isinstance(m, CounterResult | TimerResult) for m in metrics.values())
    assert metrics[TOTAL_DURATION].total_duration == 500
    assert all("_" not in name for name in metrics), "hyphenated, like every Iceberg metric"


def test_reclaim_counts_bytes_as_bytes():
    class Result:
        def as_dict(self):
            return {
                "operation": "remove-orphans",
                "table": "db.unpartitioned",
                "deleted": 4,
                "deleted_bytes": 2048,
                "dry_run": False,
                "roots": ["s3://x"],
            }

    metrics = reclaim_metrics(Result())

    assert metrics["deleted"].unit == COUNT
    assert metrics["deleted-bytes"].unit == BYTES
    assert "dry-run" not in metrics, "a flag is not a metric"
    assert "roots" not in metrics, "a path is not a metric"
    assert "operation" not in metrics and "table" not in metrics


# -- the two surfaces must not drift (ZMBNI-128) -------------------------

#: `Reportable.as_dict()` key -> the Iceberg metric that means the same thing.
#:
#: The pairs exist because the two are counted **independently**: the left is
#: Zamboni's own bookkeeping through the rewrite, the right is what PyIceberg
#: computed from the `DataFile` objects the commit actually wrote. Agreement is
#: therefore evidence; a shared code path would have been none.
AGREEING_COUNTERS = {
    "data_files_rewritten": REMOVED_DATA_FILES,
    "data_files_added": "added-data-files",
    "bytes_rewritten": REMOVED_FILES_SIZE_BYTES,
    "bytes_added": ADDED_FILES_SIZE_BYTES,
}


def test_zamboni_and_iceberg_agree_on_what_the_commit_did(session, unpartitioned):
    """The anti-drift guarantee, and the reason `as_dict()` was **not**
    regenerated from the report.

    Regenerating it would rename every key an integrator reads -- a breaking
    change to the surface `Reportable` promises is stable for years -- and a
    generated dict agrees with itself by construction, so it would catch
    nothing. Two independent counts of one commit agreeing is the real check.
    """
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()
    table = session.catalog.load_table("db.unpartitioned")

    doc = result.as_dict()
    report = commit_reports(result, table.metadata.snapshots)[0]

    compared = 0
    for key, metric in AGREEING_COUNTERS.items():
        assert metric in report.metrics, f"{metric} missing; the mapping lost a counter"
        assert doc[key] == report.metrics[metric].value, (
            f"{key}={doc[key]} but {metric}={report.metrics[metric].value}: Zamboni and "
            "Iceberg disagree about the same commit, so one of them is wrong"
        )
        compared += 1
    assert compared == len(AGREEING_COUNTERS) == 4


def test_every_agreeing_counter_is_still_a_key_on_both_sides(session, unpartitioned):
    """Guards the guard. If a result renames a key or the mapping drops a
    metric, the loop above would silently compare nothing."""
    result = TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()

    doc = result.as_dict()
    metric_names = {name for _, name, _ in SUMMARY_TO_METRIC}

    assert set(AGREEING_COUNTERS) <= set(doc), "a result key was renamed"
    assert set(AGREEING_COUNTERS.values()) <= metric_names, "a metric left the mapping"
