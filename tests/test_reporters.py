# SPDX-License-Identifier: Apache-2.0
"""The reporter seam: one report type, many destinations.

The properties worth holding are mostly about what must *not* happen. Telemetry
is the least important thing in a maintenance run and has to behave like it: a
reporter that raises, a catalog that has no metrics endpoint, a network that is
down -- none of them may change what the run did or what it exited with.
"""

from __future__ import annotations

import json
import logging

import pytest

from zamboni.metrics import COMMIT_REPORT, CommitReport, CounterResult, ReclaimReport
from zamboni.reporters import (
    CollectingReporter,
    LoggingReporter,
    MetricsReporter,
    MultiReporter,
    NoopReporter,
    RestMetricsReporter,
    emit,
    reporter_for,
)

COMMIT = CommitReport(
    table_name="db.events",
    snapshot_id=7,
    sequence_number=3,
    operation="replace",
    metrics={"removed-data-files": CounterResult(unit="count", value=6)},
)
RECLAIM = ReclaimReport(
    table_name="db.events",
    operation="remove-orphans",
    metrics={"deleted": CounterResult(unit="count", value=2)},
)


class Boom:
    """A reporter that fails the way a real one does: at report time."""

    def __init__(self) -> None:
        self.calls = 0

    def report(self, report):
        self.calls += 1
        raise RuntimeError("the metrics endpoint is on fire")


class FakeResponse:
    def __init__(self, status_code=204):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, status_code=204):
        self.posts: list[tuple[str, dict]] = []
        self._status = status_code

    def post(self, url, json):
        self.posts.append((url, json))
        return FakeResponse(self._status)


class FakeRestCatalog:
    def __init__(self, status_code=204):
        self._session_obj = FakeSession(status_code)
        self._session = self._session_obj

    def url(self, endpoint, **kwargs):
        return "https://lakekeeper/v1/wh/" + endpoint.format(**kwargs)


# -- the interface -------------------------------------------------------


@pytest.mark.parametrize(
    "reporter",
    [NoopReporter(), CollectingReporter(), LoggingReporter(), MultiReporter()],
)
def test_every_reporter_satisfies_the_one_method_interface(reporter):
    """One method, as Iceberg's `MetricsReporter` has one method."""
    assert isinstance(reporter, MetricsReporter)


def test_the_default_emits_nothing(caplog):
    """Telemetry is opt-in: a deployment that wants none pays for none."""
    with caplog.at_level(logging.DEBUG):
        NoopReporter().report(COMMIT)

    assert caplog.records == []


def test_no_reporter_is_the_same_as_the_noop_one():
    """So a caller that has not configured telemetry needs no branch of its own."""
    emit(None, [COMMIT, RECLAIM])  # must not raise


# -- a reporter can never fail a run -------------------------------------


def test_a_reporter_that_raises_does_not_reach_the_caller(caplog):
    """A run that did its work and then failed because a metrics endpoint was
    down teaches an operator to distrust the exit code."""
    boom = Boom()

    with caplog.at_level(logging.WARNING):
        emit(boom, [COMMIT])

    assert boom.calls == 1
    assert "the run is unaffected" in caplog.text


def test_one_failing_reporter_does_not_cost_you_the_others():
    """A misconfigured catalog endpoint should not also lose the log line."""
    collecting = CollectingReporter()

    MultiReporter(Boom(), collecting, Boom()).report(COMMIT)

    assert collecting.reports == [COMMIT]


def test_every_report_is_attempted_even_after_one_fails():
    seen = CollectingReporter()

    class FailsFirst:
        def __init__(self):
            self.n = 0

        def report(self, report):
            self.n += 1
            if self.n == 1:
                raise RuntimeError("nope")
            seen.report(report)

    emit(FailsFirst(), [COMMIT, RECLAIM])

    assert seen.reports == [RECLAIM]


# -- the destinations ----------------------------------------------------


def test_the_collecting_reporter_hands_back_serialisable_reports():
    """Deployment model 3: IWS has a telemetry pipeline and wants the numbers
    handed to it rather than posted independently."""
    collecting = CollectingReporter()

    collecting.report(COMMIT)
    collecting.report(RECLAIM)

    docs = collecting.as_dicts()
    assert json.loads(json.dumps(docs)) == docs
    assert docs[0]["snapshot-id"] == 7
    assert docs[1]["report-type"] == "zamboni-reclaim-report"


def test_the_logging_reporter_writes_one_json_line(caplog):
    """The zero-infrastructure destination: a cron box with a log shipper."""
    with caplog.at_level(logging.INFO, logger="zamboni.metrics"):
        LoggingReporter().report(COMMIT)

    assert len(caplog.records) == 1
    assert json.loads(caplog.records[0].getMessage())["table-name"] == "db.events"


# -- the catalog endpoint ------------------------------------------------


def test_the_rest_reporter_posts_the_spec_shaped_request():
    """`report-type` plus the report's own fields, flattened -- which is what
    `ReportMetricsRequestParser.toJson` writes."""
    catalog = FakeRestCatalog()

    RestMetricsReporter(catalog).report(COMMIT)

    url, body = catalog._session_obj.posts[0]
    assert url == "https://lakekeeper/v1/wh/namespaces/db/tables/events/metrics"
    assert body["report-type"] == COMMIT_REPORT == "commit-report"
    assert body["snapshot-id"] == 7 and body["operation"] == "replace"
    assert "metrics" in body


def test_a_catalog_without_the_endpoint_is_disabled_after_one_try(caplog):
    """Metrics reporting is optional in the REST spec. A fleet of five hundred
    tables must not produce five hundred warnings about it."""
    catalog = FakeRestCatalog(status_code=404)
    reporter = RestMetricsReporter(catalog)

    with caplog.at_level(logging.INFO):
        for _ in range(5):
            reporter.report(COMMIT)

    assert len(catalog._session_obj.posts) == 1, "it stopped asking"
    assert reporter.supported is False
    assert caplog.text.count("does not implement the metrics endpoint") == 1


def test_a_real_error_is_raised_for_emit_to_catch():
    """A reporter that swallows its own errors makes the "telemetry never fails
    a run" decision unreviewable -- `emit` is what decides that, not this."""
    reporter = RestMetricsReporter(FakeRestCatalog(status_code=500))

    with pytest.raises(RuntimeError):
        reporter.report(COMMIT)

    assert reporter.supported is True, "a 500 is not 'unsupported'"


def test_reclaim_reports_are_not_posted_to_the_catalog():
    """`ReclaimReport` is Zamboni's own shape. The endpoint's schema is
    `anyOf(ScanReport, CommitReport)`, so posting it would be sending a body the
    spec does not describe."""
    catalog = FakeRestCatalog()

    RestMetricsReporter(catalog).report(RECLAIM)

    assert catalog._session_obj.posts == []


def test_a_catalog_with_no_session_says_so_rather_than_failing_quietly():
    class NotRest:
        def url(self, endpoint, **kwargs):
            return "x"

    with pytest.raises(TypeError, match="needs a REST catalog"):
        RestMetricsReporter(NotRest()).report(COMMIT)


# -- choosing one --------------------------------------------------------


def test_a_rest_catalog_gets_the_catalog_reporter():
    assert isinstance(reporter_for(FakeRestCatalog()), RestMetricsReporter)


def test_a_catalog_with_no_metrics_endpoint_gets_nothing_to_post_to(session):
    """The local SQL catalog has no endpoint, so there is nothing to post to and
    the result must still be callable."""
    reporter = reporter_for(session.catalog)

    assert isinstance(reporter, NoopReporter)
    reporter.report(COMMIT)


def test_extra_reporters_are_kept_whatever_the_catalog_is(session):
    collecting = CollectingReporter()

    reporter_for(session.catalog, [collecting]).report(COMMIT)
    reporter_for(FakeRestCatalog(), [collecting]).report(COMMIT)

    assert len(collecting.reports) == 2


# -- through a real run --------------------------------------------------


def test_a_run_reports_what_it_committed(session, unpartitioned, tmp_path):
    """The seam is only real if the run loop uses it."""
    import json as _json

    from zamboni import maintain

    config = tmp_path / "table-config.json"
    config.write_text(
        _json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "namespaces": {"db": {"tables": {"unpartitioned": {}}}},
            }
        )
    )
    collecting = CollectingReporter()

    report = maintain(session, table_config=config, commit=True, reporter=collecting)

    assert report.exit_code == 0
    kinds = {type(r).__name__ for r in collecting.reports}
    assert "CommitReport" in kinds, "compaction committed and was reported"
    assert "ReclaimReport" in kinds, "the operations that commit nothing are on the seam too"
    assert all("total-duration" in r.metrics for r in collecting.reports), (
        "the run knows how long each operation took; nothing else does"
    )


def test_a_reporter_that_explodes_does_not_change_the_run(session, unpartitioned, tmp_path):
    import json as _json

    from zamboni import maintain

    config = tmp_path / "table-config.json"
    config.write_text(
        _json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "namespaces": {"db": {"tables": {"unpartitioned": {}}}},
            }
        )
    )

    quiet = maintain(session, table_config=config, commit=False)
    noisy = maintain(session, table_config=config, commit=False, reporter=Boom())

    assert noisy.exit_code == quiet.exit_code == 0
    assert [o.operation for o in noisy.outcomes] == [o.operation for o in quiet.outcomes]
