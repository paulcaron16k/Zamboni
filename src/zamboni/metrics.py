# SPDX-License-Identifier: Apache-2.0
"""Iceberg's own metrics vocabulary, built from the snapshots Zamboni commits.

Iceberg defines a metrics standard -- `MetricsReporter` / `MetricsReport` since
1.1.0, carrying `ScanReport` and `CommitReport`, transported by
``POST /v1/{prefix}/namespaces/{ns}/tables/{table}/metrics``. `RESTMetricsReporter`
is the **default** when Java uses a REST catalog. **PyIceberg implements none of
it** (verified against 0.12.0; upstream `iceberg-python#847`, open), so a Java
Spark job reports commit metrics through Lakekeeper by default and everything on
this stack is silent.

This module is the report *currency*: the types and the mapping. Where the
reports go is the reporter seam (ZMBNI-128).

**Built from the snapshot summary, not from our own bookkeeping.** That is not a
shortcut, it is what Java does: `CommitMetricsResult.from(commitMetrics,
snapshotSummary)` reads all but two of its counters straight out of the summary
the commit wrote. Deriving them from `CompactionResult` instead would produce
numbers that agree with Iceberg's only by coincidence and drift the first time
either side changed what it counted. Reading the summary means "removed-data-files"
means exactly what it means on a Java Spark job against the same table.

The two exceptions, which no summary carries, come from the operation:
``total-duration`` and ``attempts``.

**Names verified against upstream, not remembered.** Every constant below was
read from `CommitMetricsResult.java` and `CommitMetrics.java`, the summary
property names from `SnapshotSummary.java`, and the wire shapes from
`open-api/rest-catalog-open-api.yaml`, all on `apache/iceberg@main` on
2026-09-26. This matters more than usual here because a first attempt at the
schemas from a summarising fetch got every field of `CounterResult` and
`TimerResult` wrong -- it returned a plausible reconstruction rather than the
file.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .health import OPERATION_STAMP as _OPERATION_STAMP

# -- the vocabulary ------------------------------------------------------
#
# CommitMetricsResult.java. Reproduced as constants rather than written inline
# so that a typo is a NameError here instead of a metric a dashboard silently
# never sees.

ADDED_DATA_FILES = "added-data-files"
REMOVED_DATA_FILES = "removed-data-files"
TOTAL_DATA_FILES = "total-data-files"
ADDED_DELETE_FILES = "added-delete-files"
ADDED_EQUALITY_DELETE_FILES = "added-equality-delete-files"
ADDED_POSITIONAL_DELETE_FILES = "added-positional-delete-files"
ADDED_DVS = "added-dvs"
REMOVED_POSITIONAL_DELETE_FILES = "removed-positional-delete-files"
REMOVED_DVS = "removed-dvs"
REMOVED_EQUALITY_DELETE_FILES = "removed-equality-delete-files"
REMOVED_DELETE_FILES = "removed-delete-files"
TOTAL_DELETE_FILES = "total-delete-files"
ADDED_RECORDS = "added-records"
REMOVED_RECORDS = "removed-records"
TOTAL_RECORDS = "total-records"
ADDED_FILES_SIZE_BYTES = "added-files-size-bytes"
REMOVED_FILES_SIZE_BYTES = "removed-files-size-bytes"
TOTAL_FILES_SIZE_BYTES = "total-files-size-bytes"
ADDED_POSITIONAL_DELETES = "added-positional-deletes"
REMOVED_POSITIONAL_DELETES = "removed-positional-deletes"
TOTAL_POSITIONAL_DELETES = "total-positional-deletes"
ADDED_EQUALITY_DELETES = "added-equality-deletes"
REMOVED_EQUALITY_DELETES = "removed-equality-deletes"
TOTAL_EQUALITY_DELETES = "total-equality-deletes"
MANIFESTS_KEPT = "manifests-kept"
MANIFESTS_CREATED = "manifests-created"
MANIFESTS_REPLACED = "manifests-replaced"
MANIFEST_ENTRIES_PROCESSED = "manifest-entries-processed"

#: CommitMetrics.java. The two an operation must supply, because no snapshot
#: summary carries them.
TOTAL_DURATION = "total-duration"
ATTEMPTS = "attempts"

#: MetricsContext.Unit. Three values, and "undefined" is a real one.
COUNT = "count"
BYTES = "bytes"
UNDEFINED = "undefined"

#: `ReportMetricsRequest.report-type`, from
#: `ReportMetricsRequestParser.fromReportType`: the enum name, underscores to
#: hyphens, lower-cased. Not guessed -- "commit_report" and "commitReport" are
#: both plausible and both wrong.
COMMIT_REPORT = "commit-report"

#: Ours. No upstream equivalent, and namespaced so it can never collide with a
#: report type Iceberg later defines.
RECLAIM_REPORT = "zamboni-reclaim-report"

#: Free-form `metadata` keys. The report's `metadata` is a string map Iceberg
#: leaves to the producer, which is where a tool-specific fact belongs -- the
#: metric *names* stay unprefixed and standard, as Iceberg defines them.
#: The same key `health.OPERATION_STAMP` names, imported rather than repeated:
#: the stamp Zamboni writes into a snapshot summary and the stamp this report
#: carries to a metrics endpoint must be the same string, or a consumer cannot
#: join them.
OPERATION_STAMP = _OPERATION_STAMP
SNAPSHOTS_IN_OPERATION = "zamboni.snapshots-in-operation"

#: (snapshot summary property, CommitReport metric name, unit).
#:
#: **Five of these are renames, not copies**, which is the whole reason this is
#: a table rather than a loop over the summary:
#:
#:   deleted-data-files            -> removed-data-files
#:   deleted-records               -> removed-records
#:   added-files-size              -> added-files-size-bytes
#:   removed-files-size            -> removed-files-size-bytes
#:   total-files-size              -> total-files-size-bytes
#:
#: and four more swap "position" for "positional" between the summary and the
#: metric. Copying the summary key through as the metric name would look right
#: and be wrong on nine of twenty-four counters.
#:
#: Units follow `CommitMetricsResult.from`: the three size counters are BYTES,
#: every other counter is COUNT.
SUMMARY_TO_METRIC: tuple[tuple[str, str, str], ...] = (
    ("added-data-files", ADDED_DATA_FILES, COUNT),
    ("deleted-data-files", REMOVED_DATA_FILES, COUNT),
    ("total-data-files", TOTAL_DATA_FILES, COUNT),
    ("added-delete-files", ADDED_DELETE_FILES, COUNT),
    ("added-equality-delete-files", ADDED_EQUALITY_DELETE_FILES, COUNT),
    ("added-position-delete-files", ADDED_POSITIONAL_DELETE_FILES, COUNT),
    ("added-dvs", ADDED_DVS, COUNT),
    ("removed-position-delete-files", REMOVED_POSITIONAL_DELETE_FILES, COUNT),
    ("removed-dvs", REMOVED_DVS, COUNT),
    ("removed-equality-delete-files", REMOVED_EQUALITY_DELETE_FILES, COUNT),
    ("removed-delete-files", REMOVED_DELETE_FILES, COUNT),
    ("total-delete-files", TOTAL_DELETE_FILES, COUNT),
    ("added-records", ADDED_RECORDS, COUNT),
    ("deleted-records", REMOVED_RECORDS, COUNT),
    ("total-records", TOTAL_RECORDS, COUNT),
    ("added-files-size", ADDED_FILES_SIZE_BYTES, BYTES),
    ("removed-files-size", REMOVED_FILES_SIZE_BYTES, BYTES),
    ("total-files-size", TOTAL_FILES_SIZE_BYTES, BYTES),
    ("added-position-deletes", ADDED_POSITIONAL_DELETES, COUNT),
    ("removed-position-deletes", REMOVED_POSITIONAL_DELETES, COUNT),
    ("total-position-deletes", TOTAL_POSITIONAL_DELETES, COUNT),
    ("added-equality-deletes", ADDED_EQUALITY_DELETES, COUNT),
    ("removed-equality-deletes", REMOVED_EQUALITY_DELETES, COUNT),
    ("total-equality-deletes", TOTAL_EQUALITY_DELETES, COUNT),
    ("created-manifests-count", MANIFESTS_CREATED, COUNT),
    ("replaced-manifests-count", MANIFESTS_REPLACED, COUNT),
    ("kept-manifests-count", MANIFESTS_KEPT, COUNT),
    ("processed-manifest-entry-count", MANIFEST_ENTRIES_PROCESSED, COUNT),
)


# -- the wire types ------------------------------------------------------


@dataclass(frozen=True)
class CounterResult:
    """``{unit, value}``, per the REST spec's `CounterResult`.

    Not ``{type, value}``. The distinction is worth a line because it is what a
    plausible-looking guess produces, and a body a catalog rejects is a metric
    nobody ever sees.
    """

    unit: str
    value: int

    def as_dict(self) -> dict[str, Any]:
        return {"unit": self.unit, "value": self.value}


@dataclass(frozen=True)
class TimerResult:
    """``{time-unit, count, total-duration}``, per the REST spec's `TimerResult`.

    `count` is how many times the timer ran, not a quantity of anything
    measured. Iceberg's own commit timer is in **nanoseconds**
    (``CommitMetrics.totalDuration``), which is why :func:`nanos` exists rather
    than callers picking a unit.
    """

    time_unit: str
    count: int
    total_duration: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "time-unit": self.time_unit,
            "count": self.count,
            "total-duration": self.total_duration,
        }


MetricResult = CounterResult | TimerResult


def nanos(duration_ns: int, count: int = 1) -> TimerResult:
    """A timer in the unit Iceberg's own commit timer uses."""
    return TimerResult(time_unit="nanoseconds", count=count, total_duration=int(duration_ns))


@dataclass(frozen=True)
class CommitReport:
    """One commit, in Iceberg's shape.

    `table-name`, `snapshot-id`, `sequence-number`, `operation` and `metrics`
    are all **required** by the spec; `metadata` is the optional free-form
    string map. `operation` is Iceberg's snapshot operation -- ``replace``,
    ``overwrite`` -- and *not* Zamboni's verb, which goes in `metadata` under
    :data:`OPERATION_STAMP`. Putting the verb in `operation` would be the
    obvious mistake: the field is Iceberg's enumeration and a consumer grouping
    by it expects Iceberg's values.
    """

    table_name: str
    snapshot_id: int
    sequence_number: int
    operation: str
    metrics: Mapping[str, MetricResult] = field(default_factory=dict)
    metadata: Mapping[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """The spec's `CommitReport` shape, ready to be a request body.

        `metadata` is omitted when empty rather than sent as ``{}``: it is an
        optional field, and an empty map says "this producer records no
        metadata", which is a different claim from not having any this time.
        """
        doc: dict[str, Any] = {
            "table-name": self.table_name,
            "snapshot-id": self.snapshot_id,
            "sequence-number": self.sequence_number,
            "operation": self.operation,
            "metrics": {name: metric.as_dict() for name, metric in self.metrics.items()},
        }
        if self.metadata:
            doc["metadata"] = dict(self.metadata)
        return doc

    def describe(self) -> str:
        verb = self.metadata.get(OPERATION_STAMP, self.operation)
        counters = ", ".join(
            f"{name}={metric.value}"
            for name, metric in sorted(self.metrics.items())
            if isinstance(metric, CounterResult)
        )
        return f"{self.table_name} {verb} snapshot {self.snapshot_id}: {counters or 'no counters'}"


@dataclass(frozen=True)
class ReclaimReport:
    """What `expire`, `remove-orphans` and `apply-properties` did.

    **Not an Iceberg type, and deliberately shaped like one.** Those three
    commit no snapshot -- expiry *removes* snapshots, the other two touch no
    data -- so there is no `CommitReport` to carry them and Iceberg defines no
    report that fits. Without this they would bypass the reporter seam
    altogether and half of Zamboni's operations would be invisible to the
    telemetry that exists for the other half.

    The counters keep Zamboni's own names, because there is nothing upstream to
    conform to, but are :class:`CounterResult` and :class:`TimerResult` so that
    an Iceberg reclaim report, if one is ever defined, is a mapping rather than
    a re-model.

    `operation` is Zamboni's verb here, unlike :class:`CommitReport` where the
    field is Iceberg's enumeration -- there being no Iceberg operation for "we
    deleted some files nothing referenced".
    """

    table_name: str
    operation: str
    metrics: Mapping[str, MetricResult] = field(default_factory=dict)
    metadata: Mapping[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "report-type": RECLAIM_REPORT,
            "table-name": self.table_name,
            "operation": self.operation,
            "metrics": {name: metric.as_dict() for name, metric in self.metrics.items()},
        }
        if self.metadata:
            doc["metadata"] = dict(self.metadata)
        return doc

    def describe(self) -> str:
        counters = ", ".join(
            f"{name}={metric.value}"
            for name, metric in sorted(self.metrics.items())
            if isinstance(metric, CounterResult)
        )
        return f"{self.table_name} {self.operation}: {counters or 'no counters'}"


#: Every report the seam carries. Iceberg's own `MetricsReport` is the supertype
#: of `ScanReport` and `CommitReport`; Zamboni produces no scans, and adds the
#: reclaim shape Iceberg has no type for.
MetricsReport = CommitReport | ReclaimReport


# -- building them -------------------------------------------------------


def metrics_from_summary(summary: Mapping[str, str]) -> dict[str, MetricResult]:
    """Every defined counter the summary carries, under its metric name.

    A key the summary does not have is **absent**, never zero. Java does the
    same (`counterFrom` returns null when the key is missing), and the
    difference matters: "this commit removed no data files" and "this writer
    does not report removed data files" are different facts, and a zero would
    make a PyIceberg table look like it had done something it never claimed.

    A value that is not an integer is skipped for the same reason.
    """
    out: dict[str, MetricResult] = {}
    for prop, name, unit in SUMMARY_TO_METRIC:
        raw = summary.get(prop)
        if raw is None:
            continue
        try:
            out[name] = CounterResult(unit=unit, value=int(raw))
        except (TypeError, ValueError):
            continue
    return out


def _summary_of(snapshot: Any) -> dict[str, str]:
    """The snapshot's summary as a plain string map.

    PyIceberg splits it: `operation` is a typed field and everything else lands
    in `additional_properties`. Java sees one map, and so does the mapping
    above.
    """
    summary = getattr(snapshot, "summary", None)
    if summary is None:
        return {}
    return dict(getattr(summary, "additional_properties", {}) or {})


def _operation_of(snapshot: Any) -> str | None:
    summary = getattr(snapshot, "summary", None)
    operation = getattr(summary, "operation", None)
    if operation is None:
        return None
    return getattr(operation, "value", str(operation))


def commit_report(
    table_name: str,
    snapshot: Any,
    *,
    metrics: Mapping[str, MetricResult] | None = None,
    metadata: Mapping[str, str] | None = None,
) -> CommitReport | None:
    """One snapshot's `CommitReport`, or ``None`` if it cannot be a valid one.

    ``None`` when the snapshot carries no `operation`, because that field is
    required: a body the catalog rejects is worse than no body, and silently
    substituting a placeholder would put an invented operation into someone's
    metrics store.

    `metrics` is merged *over* the summary-derived counters, for the facts only
    the operation knows.
    """
    operation = _operation_of(snapshot)
    if operation is None:
        return None

    derived = metrics_from_summary(_summary_of(snapshot))
    derived.update(metrics or {})
    return CommitReport(
        table_name=table_name,
        snapshot_id=int(snapshot.snapshot_id),
        sequence_number=int(getattr(snapshot, "sequence_number", 0) or 0),
        operation=operation,
        metrics=derived,
        metadata=dict(metadata or {}),
    )


def _snapshot_ids(result: Any) -> list[int]:
    """Which snapshots this result committed, from its own `as_dict()`.

    Read from the reported mapping rather than by walking each result type,
    because `as_dict()` is the surface those types already promise to keep --
    `Reportable` says so -- and a second accessor per type is a second thing to
    keep in step. Compaction reports `snapshot_ids`, because it commits one
    snapshot **per rewrite group** unless asked for a single commit; the other
    two report a single `snapshot_id`.
    """
    doc = result.as_dict() if hasattr(result, "as_dict") else {}
    if doc.get("dry_run"):
        # Nothing was committed, so there is no commit to report.
        return []
    many = doc.get("snapshot_ids")
    if isinstance(many, list):
        return [int(s) for s in many if s is not None]
    one = doc.get("snapshot_id")
    return [int(one)] if one is not None else []


def _operation_only_metrics(result: Any) -> dict[str, MetricResult]:
    """Counters Iceberg defines that the summary PyIceberg writes does not carry.

    Only manifest rewriting has any: `manifests-created`, `manifests-kept`,
    `manifests-replaced` and `manifest-entries-processed` are defined metrics,
    and Java fills them from summary properties (`created-manifests-count` and
    friends) that **PyIceberg does not write** -- checked against 0.12.0's
    `snapshots.py`, which has no such constant. Zamboni's rewriter counts them
    itself, so supplying them here closes a real gap rather than duplicating
    the summary.

    A conversion, not a rename: `manifests_before`/`manifests_after` are totals
    and are deliberately *not* mapped, because Iceberg's three are a partition
    of the manifests this commit touched, which is a different quantity.
    """
    doc = result.as_dict() if hasattr(result, "as_dict") else {}
    if doc.get("operation") != "rewrite-manifests":
        return {}

    pairs = (
        (MANIFESTS_CREATED, "manifests_written"),
        (MANIFESTS_KEPT, "manifests_kept"),
        (MANIFESTS_REPLACED, "manifests_replaced"),
        (MANIFEST_ENTRIES_PROCESSED, "entries"),
    )
    out: dict[str, MetricResult] = {}
    for name, key in pairs:
        value = doc.get(key)
        if isinstance(value, int):
            out[name] = CounterResult(unit=COUNT, value=value)
    return out


def commit_reports(
    result: Any,
    snapshots: Iterable[Any],
    *,
    duration_ns: int | None = None,
) -> list[CommitReport]:
    """Every `CommitReport` one operation's result produced.

    A list, not one report, because compaction commits **one snapshot per
    rewrite group** unless asked for a single commit -- so one Zamboni operation
    can be several Iceberg commits, and Iceberg's unit is the commit.

    Empty for a dry run, for an operation that commits nothing (`expire`,
    `remove-orphans`, `apply-properties` produce no snapshot), and for a
    snapshot the table no longer has.

    Args:
        result: any operation result; its `as_dict()` says what it committed.
        snapshots: the table's snapshots, e.g. ``table.metadata.snapshots``.
        duration_ns: how long the operation took. Attached as `total-duration`
            **only when the operation produced exactly one snapshot**, because
            it is the operation's duration and Iceberg's timer is per commit --
            copying it onto three reports would treble it for anyone summing.
            Where it is dropped, `metadata` records how many snapshots there
            were so the omission is visible rather than silent.

    `attempts` is deliberately never set. Iceberg means the number of commit
    attempts, PyIceberg retries internally up to `commit.retry.num-retries`
    without surfacing a count, and a hardcoded 1 would be a measurement we did
    not take. Absent is honest; wrong is not.
    """
    wanted = _snapshot_ids(result)
    if not wanted:
        return []

    by_id = {int(s.snapshot_id): s for s in snapshots}
    doc = result.as_dict() if hasattr(result, "as_dict") else {}
    table_name = str(doc.get("table") or "")
    extra = _operation_only_metrics(result)

    verb = str(doc.get("operation") or "")

    reports = []
    for snapshot_id in wanted:
        snapshot = by_id.get(snapshot_id)
        if snapshot is None:
            continue

        # The snapshot's own stamp wins over the result's verb, because the two
        # do not always agree and the report is derived from the snapshot. The
        # committer writes "compaction" where `Operation.COMPACT.value` is
        # "compact" (the other two stamps do match their enum value) -- a real
        # inconsistency, filed separately rather than fixed here, because
        # changing it rewrites a key in user tables and that is a decision of
        # its own. Until then, a report that said "compact" while the snapshot
        # it describes says "compaction" would make the two impossible to join.
        metadata = {OPERATION_STAMP: _summary_of(snapshot).get(OPERATION_STAMP) or verb}
        if len(wanted) > 1:
            metadata[SNAPSHOTS_IN_OPERATION] = str(len(wanted))

        metrics = dict(extra)
        if duration_ns is not None and len(wanted) == 1:
            metrics[TOTAL_DURATION] = nanos(duration_ns)
        report = commit_report(table_name, snapshot, metrics=metrics, metadata=metadata)
        if report is not None:
            reports.append(report)
    return reports


def reclaim_report(result: Any, *, duration_ns: int | None = None) -> ReclaimReport | None:
    """A :class:`ReclaimReport` for an operation that commits no snapshot.

    ``None`` for a dry run -- nothing happened, so there is nothing to report --
    and for a result that is not one of the reclaim operations, so a caller can
    hand any result to both builders and let each decide.
    """
    doc = result.as_dict() if hasattr(result, "as_dict") else {}
    operation = str(doc.get("operation") or "")
    if not operation or doc.get("dry_run"):
        return None
    if doc.get("snapshot_id") is not None or doc.get("snapshot_ids"):
        # It committed. That is a CommitReport's business.
        return None
    return ReclaimReport(
        table_name=str(doc.get("table") or ""),
        operation=operation,
        metrics=reclaim_metrics(result, duration_ns=duration_ns),
        metadata={OPERATION_STAMP: operation},
    )


def reclaim_metrics(result: Any, *, duration_ns: int | None = None) -> dict[str, MetricResult]:
    """Iceberg's *primitives* for the operations Iceberg has no report type for.

    `expire`, `remove-orphans` and `apply-properties` commit no snapshot, so
    there is no `CommitReport` to carry them and no defined names to use. They
    keep Zamboni's own names -- there is nothing upstream to conform to -- but
    are expressed as :class:`CounterResult` and :class:`TimerResult` so that a
    reclaim report type, if Iceberg ever defines one, is a mapping rather than a
    re-model. That is the whole point of doing it this way now.

    Names are the result's own `as_dict()` keys with underscores hyphenated, so
    there is exactly one place the counter names for these operations are
    decided and it is already the documented one.

    Only integers become counters: paths, flags and the operation name are not
    metrics, and `bytes`-suffixed keys carry the BYTES unit.
    """
    doc = result.as_dict() if hasattr(result, "as_dict") else {}
    out: dict[str, MetricResult] = {}
    for key, value in doc.items():
        if key in ("operation", "table", "snapshot_id", "dry_run"):
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            continue
        unit = BYTES if key.endswith("_bytes") else COUNT
        out[key.replace("_", "-")] = CounterResult(unit=unit, value=value)
    if duration_ns is not None:
        out[TOTAL_DURATION] = nanos(duration_ns)
    return out


__all__ = [
    "ATTEMPTS",
    "BYTES",
    "COMMIT_REPORT",
    "COUNT",
    "OPERATION_STAMP",
    "RECLAIM_REPORT",
    "SNAPSHOTS_IN_OPERATION",
    "SUMMARY_TO_METRIC",
    "TOTAL_DURATION",
    "UNDEFINED",
    "CommitReport",
    "CounterResult",
    "MetricResult",
    "MetricsReport",
    "ReclaimReport",
    "TimerResult",
    "commit_report",
    "commit_reports",
    "metrics_from_summary",
    "nanos",
    "reclaim_metrics",
    "reclaim_report",
]
