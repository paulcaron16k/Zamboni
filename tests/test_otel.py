# SPDX-License-Identifier: Apache-2.0
"""The OpenTelemetry reporter, and the cost of being instrumented.

Two things are being proved. That the instruments carry OTel-correct names and
units -- durations in seconds, UCUM units in the unit field and never in the
name. And that a run which asks for no telemetry pays nothing for the dependency
being there at all, which is the whole argument for putting the API in the base
install.
"""

from __future__ import annotations

import pytest

from zamboni.metrics import (
    ADDED_FILES_SIZE_BYTES,
    MANIFEST_ENTRIES_PROCESSED,
    MANIFESTS_CREATED,
    MANIFESTS_KEPT,
    MANIFESTS_REPLACED,
    SUMMARY_TO_METRIC,
    TOTAL_DURATION,
    CommitReport,
    CounterResult,
    ReclaimReport,
    nanos,
)
from zamboni.otel import (
    COMMIT_DURATION,
    ICEBERG_UNITS,
    OPERATION_ATTRIBUTE,
    RECLAIM_DURATION,
    RECLAIM_UNITS,
    TABLE_ATTRIBUTE,
    OTelReporter,
    sdk_configured,
)


class FakeInstrument:
    def __init__(self, name, unit):
        self.name = name
        self.unit = unit
        self.points: list[tuple[float, dict]] = []

    def add(self, value, attributes=None):
        self.points.append((value, dict(attributes or {})))

    def record(self, value, attributes=None):
        self.points.append((value, dict(attributes or {})))


class FakeMeter:
    def __init__(self):
        self.counters: dict[str, FakeInstrument] = {}
        self.histograms: dict[str, FakeInstrument] = {}
        self.creations = 0

    def create_counter(self, name, unit=None, description=None):
        self.creations += 1
        self.counters[name] = FakeInstrument(name, unit)
        return self.counters[name]

    def create_histogram(self, name, unit=None, description=None):
        self.creations += 1
        self.histograms[name] = FakeInstrument(name, unit)
        return self.histograms[name]


COMMIT = CommitReport(
    table_name="db.events",
    snapshot_id=7,
    sequence_number=3,
    operation="replace",
    metrics={
        "removed-data-files": CounterResult(unit="count", value=6),
        ADDED_FILES_SIZE_BYTES: CounterResult(unit="bytes", value=584),
        TOTAL_DURATION: nanos(1_500_000_000),
    },
    metadata={"zamboni.operation": "compact"},
)
RECLAIM = ReclaimReport(
    table_name="db.events",
    operation="remove-orphans",
    metrics={
        "files-deleted": CounterResult(unit="count", value=4),
        "bytes-deleted": CounterResult(unit="bytes", value=2048),
        TOTAL_DURATION: nanos(250_000_000),
    },
)


# -- names and units -----------------------------------------------------


def test_iceberg_counters_are_mirrored_under_their_own_namespace():
    """No OTel semantic convention exists for Iceberg, so the counters keep
    Iceberg's defined names, namespaced, with hyphens turned to underscores."""
    meter = FakeMeter()

    OTelReporter(meter).report(COMMIT)

    assert "iceberg.removed_data_files" in meter.counters
    assert not any("-" in name for name in meter.counters), "hyphens are not OTel names"


def test_the_unit_is_in_the_unit_field_and_never_in_the_name():
    """OTel's rule, and the one most often broken by a name like
    `added_files_size_bytes_total`."""
    meter = FakeMeter()

    OTelReporter(meter).report(COMMIT)

    size = meter.counters["iceberg.added_files_size_bytes"]
    assert size.unit == "By", "UCUM: By, not 'bytes'"
    assert meter.counters["iceberg.removed_data_files"].unit == "{file}"


def test_durations_are_seconds_in_a_histogram():
    """Iceberg times a commit in nanoseconds; OTel's rule is seconds."""
    meter = FakeMeter()

    OTelReporter(meter).report(COMMIT)

    histogram = meter.histograms[COMMIT_DURATION]
    assert histogram.unit == "s"
    assert histogram.points[0][0] == pytest.approx(1.5)
    assert TOTAL_DURATION not in meter.counters, "a timer is not a counter"


def test_a_reclaim_report_uses_its_own_namespace_and_histogram():
    """Those operations commit no snapshot, so the names are Zamboni's -- there
    is nothing upstream to conform to -- but the shape is still OTel's."""
    meter = FakeMeter()

    OTelReporter(meter).report(RECLAIM)

    assert meter.counters["zamboni.files_deleted"].unit == "{file}"
    assert meter.counters["zamboni.bytes_deleted"].unit == "By"
    assert meter.histograms[RECLAIM_DURATION].points[0][0] == pytest.approx(0.25)


def test_every_iceberg_counter_has_a_unit():
    """Keeps the unit table complete against the mapping in `metrics.py`. A
    counter that fell through would be published as dimensionless, which is
    wrong rather than merely unlabelled."""
    mapped = {name for _, name, _ in SUMMARY_TO_METRIC}
    mapped |= {MANIFESTS_CREATED, MANIFESTS_KEPT, MANIFESTS_REPLACED, MANIFEST_ENTRIES_PROCESSED}

    missing = sorted(mapped - set(ICEBERG_UNITS))

    assert not missing, f"no OTel unit declared for {missing}"


def test_every_reclaim_counter_has_a_unit(session, unpartitioned, tmp_path):
    """Runs a whole maintenance and checks every counter the three
    non-committing operations actually emit, rather than trusting a list
    written from memory. If one of them grows a counter, this fails.
    """
    import json

    from zamboni import maintain
    from zamboni.reporters import CollectingReporter

    config = tmp_path / "table-config.json"
    config.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "namespaces": {"db": {"tables": {"unpartitioned": {}}}},
            }
        )
    )
    collecting = CollectingReporter()
    maintain(session, table_config=config, commit=True, reporter=collecting)

    emitted: set[str] = set()
    for report in collecting.reports:
        if isinstance(report, ReclaimReport):
            emitted |= {name for name in report.metrics if name != TOTAL_DURATION}

    assert emitted, "no reclaim report was produced; this test would prove nothing"
    missing = sorted(emitted - set(RECLAIM_UNITS))
    assert not missing, f"no OTel unit declared for {missing}"


def test_the_operation_attribute_is_the_same_name_as_the_snapshot_stamp():
    """A consumer joining a metric to the snapshot that produced it should not
    have to learn two names for one fact."""
    from zamboni.health import OPERATION_STAMP

    meter = FakeMeter()
    OTelReporter(meter).report(COMMIT)

    attributes = meter.counters["iceberg.removed_data_files"].points[0][1]
    assert OPERATION_ATTRIBUTE == OPERATION_STAMP
    assert attributes[OPERATION_ATTRIBUTE] == "compact"
    assert attributes[TABLE_ATTRIBUTE] == "db.events"
    assert attributes["iceberg.operation"] == "replace"


# -- instruments are made once -------------------------------------------


def test_instruments_are_cached_across_reports():
    """OTel warns about duplicate registration, and a fleet run would otherwise
    create one instrument per commit."""
    meter = FakeMeter()
    reporter = OTelReporter(meter)

    for _ in range(10):
        reporter.report(COMMIT)

    assert meter.creations == 3, "two counters and one histogram, once each"
    assert len(meter.counters["iceberg.removed_data_files"].points) == 10


# -- the cost of being instrumented --------------------------------------


def test_nothing_is_emitted_without_an_sdk():
    """The whole argument for the API in the base install: instrumented, and
    silent until an application opts in."""
    assert sdk_configured() is False

    OTelReporter().report(COMMIT)  # must not raise, must do nothing


def test_importing_zamboni_does_not_import_opentelemetry():
    """ "Pays nothing" should mean nothing. A cron run that asks for no telemetry
    must not even pay the import, which is why `opentelemetry` is imported in
    `zamboni.otel` and nothing imports that module unless a reporter is built."""
    import subprocess
    import sys

    probe = (
        "import sys, zamboni, zamboni.cli, zamboni.reporters;"
        "print(any(m.startswith('opentelemetry') for m in sys.modules))"
    )
    out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)

    assert out.stdout.strip() == "False", "importing zamboni pulled in opentelemetry"
