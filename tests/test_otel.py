# SPDX-License-Identifier: Apache-2.0
"""The OpenTelemetry reporter, and the cost of being instrumented.

Two things are being proved. That the instruments carry OTel-correct names and
units -- durations in seconds, UCUM units in the unit field and never in the
name. And that a run which asks for no telemetry pays nothing for the dependency
being there at all, which is the whole argument for putting the API in the base
install.
"""

from __future__ import annotations

from typing import Any

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
    NoCommitReport,
    nanos,
)
from zamboni.otel import (
    COMMIT_DURATION,
    ICEBERG_UNITS,
    NO_COMMIT_DURATION,
    NO_COMMIT_UNITS,
    OPERATION_ATTRIBUTE,
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
NO_COMMIT = NoCommitReport(
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


def test_a_no_commit_report_uses_its_own_namespace_and_histogram():
    """Those operations commit no snapshot, so the names are Zamboni's -- there
    is nothing upstream to conform to -- but the shape is still OTel's."""
    meter = FakeMeter()

    OTelReporter(meter).report(NO_COMMIT)

    assert meter.counters["zamboni.files_deleted"].unit == "{file}"
    assert meter.counters["zamboni.bytes_deleted"].unit == "By"
    assert meter.histograms[NO_COMMIT_DURATION].points[0][0] == pytest.approx(0.25)


def test_every_iceberg_counter_has_a_unit():
    """Keeps the unit table complete against the mapping in `metrics.py`. A
    counter that fell through would be published as dimensionless, which is
    wrong rather than merely unlabelled."""
    mapped = {name for _, name, _ in SUMMARY_TO_METRIC}
    mapped |= {MANIFESTS_CREATED, MANIFESTS_KEPT, MANIFESTS_REPLACED, MANIFEST_ENTRIES_PROCESSED}

    missing = sorted(mapped - set(ICEBERG_UNITS))

    assert not missing, f"no OTel unit declared for {missing}"


def test_every_no_commit_counter_has_a_unit(session, unpartitioned, tmp_path):
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
        if isinstance(report, NoCommitReport):
            emitted |= {name for name in report.metrics if name != TOTAL_DURATION}

    assert emitted, "no no-commit report was produced; this test would prove nothing"
    missing = sorted(emitted - set(NO_COMMIT_UNITS))
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


# -- the write.completed consumer (#181) -----------------------------------------


def consumer_metrics(payloads: list[bytes], source_attributes: Any) -> dict[str, Any]:
    """Run a real feed over ``payloads`` into a real SDK meter; the metrics read
    back, by name. One payload, `boom`, raises inside handling."""
    import asyncio
    from datetime import UTC, datetime

    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    from zamboni.events.feed import QUIET, EventFeed
    from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse
    from zamboni.otel import OTelFeedObserver
    from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

    class Message:
        def __init__(self, data: bytes) -> None:
            self.payload = data

        async def ack(self) -> None: ...

        async def nak(self) -> None: ...

    class Source:
        name = "nats"
        connected = True
        telemetry_attributes = source_attributes.telemetry_attributes
        receive_operation = source_attributes.receive_operation

        async def _iterate(self):
            for data in payloads:
                yield Message(data)

        def __aiter__(self):
            return self._iterate()

    config = TableConfig(
        warehouse="acme", namespaces={"raw": NamespaceSettings(tables={"events": TableSettings()})}
    )
    fleet = FleetConfig(warehouses=(FleetWarehouse("acme", CronSchedule("0 2 * * *"), config),))
    now = [datetime(2026, 10, 13, tzinfo=UTC)]
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("test")
    feed = EventFeed(
        Source(),
        fleet=lambda: fleet,
        offer=lambda c: True,
        clock=lambda: now[0],
        observer=OTelFeedObserver(meter),
    )
    handle = feed.handle

    def raising(data: bytes) -> str:
        if data == b"boom":
            raise KeyError("internal")
        return handle(data)

    feed.handle = raising  # type: ignore[method-assign]
    asyncio.run(feed._read())
    now[0] += QUIET
    feed.flush_due()

    metrics = {}
    for resource in reader.get_metrics_data().resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                metrics[metric.name] = metric
    return metrics


def event_payload(snapshot_id: int, warehouse: str = "acme") -> bytes:
    import json

    return json.dumps(
        {
            "specversion": "1.0",
            "type": "writeCompleted",
            "source": "uri:test:host",
            "id": f"{warehouse}/raw/events@{snapshot_id}",
            "data": {
                "warehouse": warehouse,
                "namespace": ["raw"],
                "table": "events",
                "snapshot_id": snapshot_id,
                "completed_at": "2026-10-13T02:00:00Z",
            },
        }
    ).encode()


def jetstream_source() -> Any:
    from zamboni.events.nats import NatsEventSource, NatsSettings

    async def never(**options):
        raise AssertionError("no broker in this test")

    source = NatsEventSource(
        NatsSettings(servers=("nats://n:4222",), subject="prod.iceberg.write.completed"),
        connect=never,
    )
    source.mode = "jetstream"
    return source


ONE_OF_EACH = [
    event_payload(1),  # pending
    event_payload(1),  # duplicate
    event_payload(2),  # coalesced
    event_payload(3, warehouse="initech"),  # unmapped
    b"{not json",  # rejected
    b"boom",  # error
]


def test_the_consumer_emits_the_convention_metrics_with_their_units():
    from zamboni.otel import CONSUMED_MESSAGES, EVENTS_OFFERED, PROCESS_DURATION

    metrics = consumer_metrics(ONE_OF_EACH, jetstream_source())

    assert set(metrics) == {CONSUMED_MESSAGES, PROCESS_DURATION, EVENTS_OFFERED}
    assert metrics["messaging.client.consumed.messages"].unit == "{message}"
    assert metrics["messaging.process.duration"].unit == "s"
    assert metrics["zamboni.events.offered"].unit == "{table}"


def test_every_outcome_is_distinguishable_and_each_message_is_counted_once():
    """What an operator needs to tell "no events" from "events, all unmapped"."""
    from zamboni.events.feed import OUTCOMES

    metrics = consumer_metrics(ONE_OF_EACH, jetstream_source())

    consumed = {
        p.attributes["zamboni.event.outcome"]: p.value
        for p in metrics["messaging.client.consumed.messages"].data.data_points
    }
    assert consumed == dict.fromkeys(OUTCOMES, 1)
    offered = metrics["zamboni.events.offered"].data.data_points
    assert [p.value for p in offered] == [1]


def test_the_attributes_are_the_conventions_and_error_type_marks_only_failures():
    metrics = consumer_metrics(ONE_OF_EACH, jetstream_source())

    for point in metrics["messaging.client.consumed.messages"].data.data_points:
        attributes = dict(point.attributes)
        assert attributes["messaging.system"] == "nats"
        assert attributes["messaging.destination.name"] == "prod.iceberg.write.completed"
        assert attributes["messaging.consumer.group.name"] == "zamboni_prod_iceberg_write_completed"
        assert attributes["messaging.operation.name"] == "fetch"
        assert attributes["messaging.operation.type"] == "receive"
        assert "error.type" not in attributes, "receiving never failed"

    errors = {
        p.attributes["zamboni.event.outcome"]: p.attributes.get("error.type")
        for p in metrics["messaging.process.duration"].data.data_points
    }
    assert errors == {
        "pending": None,
        "duplicate": None,
        "coalesced": None,
        "unmapped": None,
        "rejected": "zamboni.events.EventError",
        "error": "KeyError",
    }
    for point in metrics["messaging.process.duration"].data.data_points:
        assert point.attributes["messaging.operation.name"] == "process"
        assert point.count == 1 and point.sum >= 0


def test_core_nats_receives_and_has_no_consumer_group():
    source = jetstream_source()
    source.mode = "core"

    assert source.receive_operation == "receive"
    assert source.telemetry_attributes == {
        "messaging.system": "nats",
        "messaging.destination.name": "prod.iceberg.write.completed",
    }
