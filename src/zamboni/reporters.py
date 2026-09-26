# SPDX-License-Identifier: Apache-2.0
"""Where reports go. One report type is the currency; a destination is a transform.

Iceberg's own architecture, and the reason for copying it rather than inventing:
`MetricsReporter` is a single method, `report(MetricsReport)`, and everything
downstream -- its REST reporter, the OTel reporter proposed in the project -- is
an implementation of that one method. Keeping the same seam means each upstream
possibility collapses an implementation here rather than changing the shape:

* PyIceberg metrics reporting (`iceberg-python#847`) -- the builder delegates to
  PyIceberg's and :class:`RestMetricsReporter` deletes.
* The proposed Iceberg OTel reporter -- ours is replaced by it, or contributes
  its instrument names.
* An Iceberg reclaim report type -- :class:`~zamboni.metrics.ReclaimReport` maps
  onto it.

In every row the seam survives and an implementation collapses, which is the
test of whether an abstraction is in the right place.

**Nothing is emitted unless a reporter is configured.** :class:`NoopReporter` is
the default, so adding telemetry is opting in, and a deployment that wants none
pays for none.

**A reporter can never fail a run.** :func:`emit` catches everything each
reporter raises and logs it. The rule is ZMBNI-133's, arrived at the same way: a
maintenance run that did its work and then failed because a metrics endpoint was
down teaches an operator to distrust the exit code, and the exit code is the one
thing here that has to stay trustworthy. Telemetry is the least important thing
in the process and must behave like it.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from typing import Any, Protocol, runtime_checkable

from .metrics import COMMIT_REPORT, CommitReport, MetricsReport

logger = logging.getLogger(__name__)


@runtime_checkable
class MetricsReporter(Protocol):
    """One method, as Iceberg's `MetricsReporter` has one method.

    An implementation should assume it is being called from inside a
    maintenance run, on the thread that just committed, and behave accordingly:
    do not block for long, and raise rather than swallow -- :func:`emit` is what
    decides that a telemetry failure is not a run failure, and a reporter that
    silently swallows its own errors makes that decision unreviewable.
    """

    def report(self, report: MetricsReport) -> None: ...


class NoopReporter:
    """The default. Exists so that "emit nothing" is a reporter like any other.

    The alternative -- ``if reporter is not None`` at every call site -- puts the
    same branch in every caller and gets one of them wrong eventually.
    """

    def report(self, report: MetricsReport) -> None:
        return None


class CollectingReporter:
    """Keeps every report in memory.

    For tests, and for an embedded integrator that wants the run's telemetry as
    data rather than shipped somewhere -- deployment model 3, where IWS already
    has a telemetry pipeline and wants Zamboni's numbers handed to it rather
    than posted independently.

    Unbounded on purpose: a maintenance run produces a report per commit, so the
    list is the size of the run. Anything long-lived should use a reporter that
    forwards instead of one that accumulates.
    """

    def __init__(self) -> None:
        self.reports: list[MetricsReport] = []

    def report(self, report: MetricsReport) -> None:
        self.reports.append(report)

    def as_dicts(self) -> list[dict[str, Any]]:
        return [r.as_dict() for r in self.reports]


class LoggingReporter:
    """Writes each report to a logger.

    The zero-infrastructure destination, and the counterpart to
    `maintenance --json`: a cron deployment that has a log shipper and nothing
    else still gets Iceberg-shaped metrics out of the box.

    JSON on one line, because the consumer is a log shipper rather than a person;
    `describe()` is what a person reads and it is already on the report.
    """

    def __init__(self, target: logging.Logger | None = None, level: int = logging.INFO) -> None:
        self._logger = target or logging.getLogger("zamboni.metrics")
        self._level = level

    def report(self, report: MetricsReport) -> None:
        self._logger.log(self._level, "%s", json.dumps(report.as_dict(), separators=(",", ":")))


class MultiReporter:
    """Fans one report out to several reporters.

    Each is isolated: one that raises does not stop the others, because a
    misconfigured catalog endpoint should not also cost you the log line. The
    isolation is here rather than only in :func:`emit` so that a caller holding a
    `MultiReporter` directly gets it too.
    """

    def __init__(self, *reporters: MetricsReporter) -> None:
        self._reporters = tuple(reporters)

    def __len__(self) -> int:
        return len(self._reporters)

    def report(self, report: MetricsReport) -> None:
        for reporter in self._reporters:
            _safely(reporter, report)


def _safely(reporter: MetricsReporter, report: MetricsReport) -> None:
    try:
        reporter.report(report)
    except Exception:
        # Deliberately bare. A reporter can raise anything -- a socket error, a
        # JSON error, an import error from a lazily-loaded SDK -- and none of
        # them is a reason for a maintenance run to have gone wrong.
        logger.warning(
            "metrics reporter %s failed; the run is unaffected",
            type(reporter).__name__,
            exc_info=True,
        )


def emit(reporter: MetricsReporter | None, reports: Iterable[MetricsReport]) -> None:
    """Send every report, and never let that fail the caller.

    ``None`` is the same as :class:`NoopReporter`, so a caller that has not
    configured telemetry needs no branch of its own.
    """
    if reporter is None:
        return
    for report in reports:
        _safely(reporter, report)


class RestMetricsReporter:
    """POSTs to the catalog's metrics endpoint, which is what Java does by default.

    ``POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`` with a
    `ReportMetricsRequest` body -- the report's own fields, flattened, plus
    `report-type`. `RESTMetricsReporter` is the default reporter when Java uses
    a REST catalog, so a Lakekeeper that already receives commit metrics from
    Java Spark jobs starts receiving them from Zamboni too, in the same shape.

    **PyIceberg has no metrics endpoint at all** -- verified against 0.12.0,
    whose `Endpoints` class does not mention metrics and whose `RestCatalog`
    never posts one (upstream `iceberg-python#847`, open). So the URL is built
    with the catalog's own public :meth:`RestCatalog.url` and sent on the
    catalog's authenticated session; when PyIceberg gains the endpoint this
    class deletes in favour of it, which is the seam's whole point.

    **A server that does not implement it is not an error.** Metrics reporting
    is optional in the REST spec and plenty of catalogs do not accept it, so a
    404, 405 or 501 disables this reporter for the rest of the process rather
    than logging a warning per commit for a fleet of five hundred tables. Any
    other failure is logged once per occurrence and swallowed by
    :func:`_safely` above.

    Reclaim reports are **not** sent. `ReclaimReport` is Zamboni's own shape;
    posting it to an endpoint whose schema is `anyOf(ScanReport, CommitReport)`
    would be sending a body the spec does not describe, and a catalog that
    happened to accept it would be storing something no other Iceberg client
    could read.
    """

    #: What the catalog says when it does not implement the endpoint.
    UNSUPPORTED = (404, 405, 501)

    def __init__(self, catalog: Any) -> None:
        self._catalog = catalog
        self._supported = True

    @property
    def supported(self) -> bool:
        """False once the catalog has told us it does not implement this."""
        return self._supported

    def report(self, report: MetricsReport) -> None:
        if not self._supported or not isinstance(report, CommitReport):
            return
        namespace, _, table = report.table_name.rpartition(".")
        if not namespace:
            logger.debug("no namespace in %r; not reporting metrics", report.table_name)
            return

        url = self._catalog.url(
            "namespaces/{namespace}/tables/{table}/metrics",
            namespace=namespace,
            table=table,
        )
        body = {"report-type": COMMIT_REPORT, **report.as_dict()}
        response = self._session().post(url, json=body)
        if response.status_code in self.UNSUPPORTED:
            # Once, not once per table. A fleet run would otherwise produce a
            # warning per commit for a catalog that is simply not going to
            # implement this.
            logger.info(
                "catalog does not implement the metrics endpoint (HTTP %s); "
                "not reporting metrics again this run",
                response.status_code,
            )
            self._supported = False
            return
        response.raise_for_status()

    def _session(self) -> Any:
        """The catalog's authenticated session.

        Private (`RestCatalog._session`), and inventoried in
        docs/pyiceberg-private-api.md with the rest. There is no public way to
        make an authenticated request to a catalog PyIceberg is already talking
        to, and the alternative -- building a second session from the same
        credentials -- would duplicate the OAuth2 client-credentials refresh and
        get a second set of tokens for no reason.
        """
        session = getattr(self._catalog, "_session", None)
        if session is None:
            raise TypeError(
                f"{type(self._catalog).__name__} has no authenticated session; "
                "RestMetricsReporter needs a REST catalog"
            )
        return session


def reporter_for(catalog: Any, extra: Sequence[MetricsReporter] = ()) -> MetricsReporter:
    """A reporter that suits this catalog, plus whatever else was asked for.

    A REST catalog gets :class:`RestMetricsReporter`; anything else -- the local
    SQL catalog, Hive -- has no metrics endpoint to post to and gets only
    `extra`. Returns :class:`NoopReporter` when that leaves nothing, so the
    result is always callable.
    """
    reporters = list(extra)
    if hasattr(catalog, "url") and hasattr(catalog, "_session"):
        reporters.append(RestMetricsReporter(catalog))
    if not reporters:
        return NoopReporter()
    if len(reporters) == 1:
        return reporters[0]
    return MultiReporter(*reporters)


__all__ = [
    "CollectingReporter",
    "LoggingReporter",
    "MetricsReporter",
    "MultiReporter",
    "NoopReporter",
    "RestMetricsReporter",
    "emit",
    "reporter_for",
]
