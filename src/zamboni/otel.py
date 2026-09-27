# SPDX-License-Identifier: Apache-2.0
"""An OpenTelemetry reporter for the metrics seam.

**Nothing here is imported unless an OTel reporter is constructed.** The
`opentelemetry` import lives in this module rather than in
:mod:`zamboni.reporters`, so a cron run that asks for no telemetry never pays
the 34-50 ms to import it -- measured against ~1.5-1.9 s to import `zamboni`
itself, so it would have been small, but "pays nothing" should mean nothing.

**The API, never the SDK.** `opentelemetry-api` is a base dependency and
`opentelemetry-sdk` is not: with no SDK configured, `get_meter` returns a proxy
and every `add()` is a no-op costing 0.19 microseconds. That is OTel's intended
behaviour for a library, and the reason for the split -- an application chooses
the exporter, a library only chooses to be instrumented. An integrator who has
configured an SDK therefore gets Zamboni's telemetry with no adapter to write;
`zamboni[otel]` adds an SDK and an OTLP exporter for a deployment that has none.

**Naming.** There is **no OpenTelemetry semantic convention for Iceberg**, so
the counters mirror Iceberg's own defined names under an `iceberg.` namespace
with hyphens turned to underscores -- `added-data-files` becomes
`iceberg.added_data_files`. Where OTel has a rule, OTel wins: durations are
**seconds** in a histogram, and units live in the instrument's unit field and
never in the name. The units are UCUM, so bytes are ``By`` and not ``bytes``.

If Iceberg and OTel agree a convention, these names change and this module is
where that happens; the seam above it does not move.
"""

from __future__ import annotations

import logging
from typing import Any

from opentelemetry import metrics as otel_metrics

from . import __version__
from .metrics import (
    ADDED_DATA_FILES,
    ADDED_DELETE_FILES,
    ADDED_DVS,
    ADDED_EQUALITY_DELETE_FILES,
    ADDED_EQUALITY_DELETES,
    ADDED_FILES_SIZE_BYTES,
    ADDED_POSITIONAL_DELETE_FILES,
    ADDED_POSITIONAL_DELETES,
    ADDED_RECORDS,
    MANIFEST_ENTRIES_PROCESSED,
    MANIFESTS_CREATED,
    MANIFESTS_KEPT,
    MANIFESTS_REPLACED,
    OPERATION_STAMP,
    REMOVED_DATA_FILES,
    REMOVED_DELETE_FILES,
    REMOVED_DVS,
    REMOVED_EQUALITY_DELETE_FILES,
    REMOVED_EQUALITY_DELETES,
    REMOVED_FILES_SIZE_BYTES,
    REMOVED_POSITIONAL_DELETE_FILES,
    REMOVED_POSITIONAL_DELETES,
    REMOVED_RECORDS,
    TOTAL_DATA_FILES,
    TOTAL_DELETE_FILES,
    TOTAL_EQUALITY_DELETES,
    TOTAL_FILES_SIZE_BYTES,
    TOTAL_POSITIONAL_DELETES,
    TOTAL_RECORDS,
    CommitReport,
    CounterResult,
    MetricsReport,
    NoCommitReport,
    TimerResult,
)

logger = logging.getLogger(__name__)

#: Instrumentation scope. `get_meter` takes the *library's* name and version,
#: not the service's -- `service.name` is a resource attribute the application
#: sets, and a library that sets it would be overwriting its host's identity.
SCOPE = "zamboni"

#: UCUM, as OTel requires. A count of things is annotated with the singular
#: thing in braces; bytes are ``By``.
FILE = "{file}"
RECORD = "{record}"
MANIFEST = "{manifest}"
ENTRY = "{entry}"
ROW = "{row}"
SNAPSHOT = "{snapshot}"
PROPERTY = "{property}"
BY = "By"
SECOND = "s"

#: Unit per Iceberg counter. Explicit rather than derived from the name: a rule
#: like "contains 'files'" reads well and puts `{file}` on
#: `added-files-size-bytes`, which is bytes. `test_every_iceberg_counter_has_a_unit`
#: keeps this complete against the mapping in :mod:`zamboni.metrics`.
ICEBERG_UNITS: dict[str, str] = {
    ADDED_DATA_FILES: FILE,
    REMOVED_DATA_FILES: FILE,
    TOTAL_DATA_FILES: FILE,
    ADDED_DELETE_FILES: FILE,
    ADDED_EQUALITY_DELETE_FILES: FILE,
    ADDED_POSITIONAL_DELETE_FILES: FILE,
    ADDED_DVS: FILE,
    REMOVED_POSITIONAL_DELETE_FILES: FILE,
    REMOVED_DVS: FILE,
    REMOVED_EQUALITY_DELETE_FILES: FILE,
    REMOVED_DELETE_FILES: FILE,
    TOTAL_DELETE_FILES: FILE,
    ADDED_RECORDS: RECORD,
    REMOVED_RECORDS: RECORD,
    TOTAL_RECORDS: RECORD,
    ADDED_FILES_SIZE_BYTES: BY,
    REMOVED_FILES_SIZE_BYTES: BY,
    TOTAL_FILES_SIZE_BYTES: BY,
    # Deleted *rows*, not delete files -- the pair of names differs by one word
    # upstream and means two different quantities.
    ADDED_POSITIONAL_DELETES: ROW,
    REMOVED_POSITIONAL_DELETES: ROW,
    TOTAL_POSITIONAL_DELETES: ROW,
    ADDED_EQUALITY_DELETES: ROW,
    REMOVED_EQUALITY_DELETES: ROW,
    TOTAL_EQUALITY_DELETES: ROW,
    MANIFESTS_CREATED: MANIFEST,
    MANIFESTS_KEPT: MANIFEST,
    MANIFESTS_REPLACED: MANIFEST,
    MANIFEST_ENTRIES_PROCESSED: ENTRY,
}

#: Unit per no-commit counter. These names are Zamboni's -- Iceberg defines no
#: report for an operation that commits no snapshot -- and come from each
#: result's own `as_dict()` keys, hyphenated.
#:
#: `test_every_no_commit_counter_has_a_unit` runs a whole maintenance and checks
#: that nothing it emits is missing here. That test is why this table is right:
#: the first version of it was written from the result *dataclass fields* and
#: was wrong on almost every entry, because `as_dict()` renames them
#: (`expired_snapshots` is reported as `snapshots-expired`).
NO_COMMIT_UNITS: dict[str, str] = {
    # expire
    "snapshots-expired": SNAPSHOT,
    "snapshots-retained": SNAPSHOT,
    "files-deleted": FILE,
    "deletes-failed": FILE,
    # remove-orphans
    "files-scanned": FILE,
    "files-referenced": FILE,
    "orphans-found": FILE,
    "orphan-bytes": BY,
    "bytes-deleted": BY,
    "too-young": FILE,
    "too-young-bytes": BY,
    # apply-properties
    "changed": PROPERTY,
    # remove-dangling-deletes, compact and rewrite-manifests when they find
    # nothing to do: they commit no snapshot, so their counters arrive here
    # rather than in a CommitReport.
    "delete-files": FILE,
    "dangling-files": FILE,
    "removable": FILE,
    "removable-bytes": BY,
    "stuck": FILE,
    "manifests-dropped": MANIFEST,
    "files-removed": FILE,
    "bytes-removed": BY,
    "manifests-before": MANIFEST,
    "manifests-after": MANIFEST,
    "manifests-replaced": MANIFEST,
    "manifests-kept": MANIFEST,
    "manifests-written": MANIFEST,
    "entries": ENTRY,
    "partition-spread-before": "{partition}",
    "partition-spread-after": "{partition}",
    "data-files-rewritten": FILE,
    "data-files-added": FILE,
    "bytes-rewritten": BY,
    "bytes-added": BY,
    "groups-rewritten": "{group}",
    "groups-evolved": "{group}",
    "groups-skipped": "{group}",
    "dangling-delete-files": FILE,
}

#: Where a counter has no unit we can name. UCUM's dimensionless unit: wrong is
#: worse than unlabelled, and a guessed `{file}` on something that is not a file
#: would be wrong in a dashboard nobody re-derives.
DIMENSIONLESS = "1"

#: Attribute keys. Coined, because there is no Iceberg semantic convention to
#: follow; namespaced so they cannot collide with one that later exists.
TABLE_ATTRIBUTE = "iceberg.table"
ICEBERG_OPERATION_ATTRIBUTE = "iceberg.operation"

#: The **same string** as the snapshot-summary stamp and the report's metadata
#: key, imported rather than repeated. A consumer that joins a metric to the
#: snapshot that produced it should not have to learn two names for one fact.
OPERATION_ATTRIBUTE = OPERATION_STAMP

#: Duration histograms. Iceberg's `total-duration` is a *field* name; OTel's
#: rule for a duration is `<thing>.duration`, in seconds, with the unit in the
#: unit field. OTel wins where the two have a rule about the same thing.
COMMIT_DURATION = "iceberg.commit.duration"
NO_COMMIT_DURATION = "zamboni.operation.duration"

_NANOS_PER_SECOND = 1_000_000_000


def _instrument_name(metric: str, namespace: str) -> str:
    return f"{namespace}{metric.replace('-', '_')}"


class OTelReporter:
    """Reports Iceberg metrics through the OpenTelemetry API.

    Instruments are created once and cached: OTel warns about duplicate
    registration of the same instrument name, and a fleet run would otherwise
    create one per commit.

    Silent by default and correct about it. With no SDK configured this records
    into proxy instruments that discard everything -- which is the intended
    outcome for a library, not a failure to report.
    """

    def __init__(self, meter: Any = None) -> None:
        self._meter = meter or otel_metrics.get_meter(SCOPE, __version__)
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}

    def report(self, report: MetricsReport) -> None:
        if isinstance(report, CommitReport):
            attributes = {
                TABLE_ATTRIBUTE: report.table_name,
                ICEBERG_OPERATION_ATTRIBUTE: report.operation,
                OPERATION_ATTRIBUTE: report.metadata.get(OPERATION_ATTRIBUTE, ""),
            }
            self._emit(report, attributes, namespace="iceberg.", duration=COMMIT_DURATION)
            return
        if isinstance(report, NoCommitReport):
            attributes = {
                TABLE_ATTRIBUTE: report.table_name,
                OPERATION_ATTRIBUTE: report.operation,
            }
            self._emit(report, attributes, namespace="zamboni.", duration=NO_COMMIT_DURATION)
            return
        logger.debug("no OTel mapping for %s", type(report).__name__)

    def _emit(
        self,
        report: MetricsReport,
        attributes: dict[str, str],
        *,
        namespace: str,
        duration: str,
    ) -> None:
        units = ICEBERG_UNITS if namespace == "iceberg." else NO_COMMIT_UNITS
        for metric, result in report.metrics.items():
            if isinstance(result, TimerResult):
                # Seconds, always. Iceberg times commits in nanoseconds; OTel's
                # rule is seconds, and the instrument's unit says so.
                self._histogram(duration).record(
                    result.total_duration / _NANOS_PER_SECOND, attributes
                )
                continue
            if isinstance(result, CounterResult):
                name = _instrument_name(metric, namespace)
                self._counter(name, units.get(metric, DIMENSIONLESS)).add(result.value, attributes)

    def _counter(self, name: str, unit: str) -> Any:
        if name not in self._counters:
            self._counters[name] = self._meter.create_counter(name, unit=unit)
        return self._counters[name]

    def _histogram(self, name: str) -> Any:
        if name not in self._histograms:
            self._histograms[name] = self._meter.create_histogram(name, unit=SECOND)
        return self._histograms[name]


def sdk_configured() -> bool:
    """Has an application configured an OTel SDK in this process?

    A proxy provider means no SDK, so everything this module records is
    discarded. Used only to *tell an operator* that `--metrics otel` will be
    silent; it deliberately does not change behaviour, because a library
    switching itself on because an SDK happens to exist is a surprise.
    """
    provider = otel_metrics.get_meter_provider()
    return type(provider).__name__ not in ("_ProxyMeterProvider", "NoOpMeterProvider")


__all__ = [
    "COMMIT_DURATION",
    "ICEBERG_OPERATION_ATTRIBUTE",
    "ICEBERG_UNITS",
    "NO_COMMIT_DURATION",
    "NO_COMMIT_UNITS",
    "OPERATION_ATTRIBUTE",
    "TABLE_ATTRIBUTE",
    "OTelReporter",
    "sdk_configured",
]
