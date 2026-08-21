# SPDX-License-Identifier: Apache-2.0
"""Trino, over ``ALTER TABLE … EXECUTE``.

Four of Zamboni's six operations map to a Trino table procedure. Argument names,
types and defaults below were read from the connector *source* -- the
``*TableProcedure.java`` classes -- and every statement this module emits has
been run against a live Trino 476 (dev-stack profile ``trino``).

**SQL generation is separated from execution on purpose.** The builders are pure
functions, so the statement for every operation is asserted exactly, without a
server; only :meth:`TrinoMaintainer.execute` needs a connection. That split is
what makes "we emit the right SQL" and "Trino accepts it" two distinct claims
rather than one untested one.

Three declarations here are the reason the interface has the shape it does:

* ``remove-dangling-deletes`` is UNSUPPORTED. Trino has no procedure for it and
  no option that achieves it, so this is the one operation missing from an
  engine entirely.
* ``compact`` is PARTIAL rather than UNSUPPORTED for ordering, because Trino
  *does* sort -- ``optimize`` carries the table's sort order through to the
  writer. An earlier draft of this file said "no sort and no Z-order", which was
  wrong on the first half and would have had ZMBNI-14 build a translation that
  discarded a capability Trino has.
* nothing previews. Trino's procedures have no ``dry_run``, which is what forces
  ``can_preview`` to be per operation and what
  :meth:`~zamboni.maintainers.Maintainer.check_consent` refuses on.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from . import (
    EngineConfigProblem,
    LayoutFeature,
    Maintainer,
    MaintainerCapabilities,
    MaintenanceRequest,
    Operation,
    OperationSupport,
    PreviewUnavailable,
    Reportable,
    Support,
    register,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ..session import CatalogSession

logger = logging.getLogger(__name__)

#: Default for the catalog properties that impose a floor. Confirmed against a
#: running Trino 476: a 1d retention is refused with "Retention specified (1.00d)
#: is shorter than the minimum retention configured in the system (7.00d)". A
#: deployment may lower it via a session or catalog property, so this is the
#: default to warn against rather than a hard limit.
DEFAULT_MIN_RETENTION_DAYS = 7

SERVER_SIDE = ("runs server-side, so none of Zamboni's client-side reclaim invariants apply",)


def quote(identifier: str) -> str:
    """Quote a Trino identifier, doubling any embedded quote.

    Not decoration. Table and namespace names reach here from a config file and
    a command line, and an unquoted one both breaks on a hyphen and is an
    injection point -- the procedure arguments are parameterised below, but an
    identifier cannot be.
    """
    return '"' + identifier.replace('"', '""') + '"'


def qualified(table: str, *, catalog: str) -> str:
    """``db.events`` -> ``"iceberg"."db"."events"``.

    Split on the *last* dot, so a nested namespace stays one schema identifier:
    Trino addresses ``a.b.c`` as schema ``a.b``, which is why
    ``iceberg.rest-catalog.nested-namespace-enabled`` is set in the dev stack.

    Confirmed against a live Trino 483 with a genuinely nested namespace rather
    than taken from the documentation: ``"iceberg"."nstest.deep"."events"``
    reads, and the multi-level spelling fails with "Too many dots in table
    name". Spark requires exactly the spelling Trino rejects -- see
    :func:`zamboni.maintainers.spark.qualified`.
    """
    namespace, _, name = table.rpartition(".")
    if not namespace:
        raise ValueError(f"{table!r} has no namespace; expected <namespace>.<table>")
    return f"{quote(catalog)}.{quote(namespace)}.{quote(name)}"


def _arguments(pairs: list[tuple[str, str]]) -> str:
    return ", ".join(f"{name} => {value}" for name, value in pairs) if pairs else ""


def _procedure(target: str, procedure: str, pairs: list[tuple[str, str]]) -> str:
    arguments = _arguments(pairs)
    call = f"{procedure}({arguments})" if arguments else procedure
    return f"ALTER TABLE {target} EXECUTE {call}"


def _days(value: int) -> str:
    """A Trino duration literal. `durationProperty` in the procedure metadata."""
    return f"'{value}d'"


@dataclass(frozen=True)
class TrinoResult:
    """What a procedure run reports.

    Deliberately thin. Trino's `ALTER TABLE EXECUTE` returns no row counts, so
    claiming "rewrote N files" here would be an invention -- the statement and
    the fact it succeeded are all the engine actually tells us.
    """

    operation: Operation
    table: str
    statement: str
    rows: tuple[Any, ...] = ()

    def describe(self) -> str:
        return "\n".join(
            [
                f"{self.table}: {self.operation.value} via trino",
                f"  {self.statement}",
                (
                    "  committed. Trino reports no counts for this procedure, so "
                    "there are none to show."
                ),
            ]
        )


@register
class TrinoMaintainer(Maintainer):
    name = "trino"

    #: Defaults chosen to match the dev stack, overridable per invocation.
    DEFAULT_PORT = 8080
    DEFAULT_CATALOG = "iceberg"
    DEFAULT_USER = "zamboni"

    #: `retain_last` on `expire_snapshots` arrived in Trino 479. Below that the
    #: argument does not exist and the statement is rejected outright.
    RETAIN_LAST_MIN_VERSION = 479

    def __init__(self, session: CatalogSession, options: Mapping[str, str] | None = None) -> None:
        super().__init__(session, options)
        settings = dict(options or {})
        self.host = settings.get("host", "localhost")
        self.port = int(settings.get("port") or self.DEFAULT_PORT)
        self.user = settings.get("user") or self.DEFAULT_USER
        self.catalog = settings.get("catalog") or self.DEFAULT_CATALOG
        version = settings.get("version")
        self.version = int(version) if version else None
        # Unknown version -> assume the argument is unavailable. Guessing the
        # other way turns a working expiry into a hard failure; guessing this
        # way loses min_snapshots_to_keep, which `validate()` then reports.
        self.supports_retain_last = (
            self.version is not None and self.version >= self.RETAIN_LAST_MIN_VERSION
        )

    @classmethod
    def capabilities(cls) -> MaintainerCapabilities:
        return MaintainerCapabilities(
            engine=cls.name,
            # No Z-order -- verified against the connector source, see COMPACT's
            # limitations below -- and no control over output file size, since
            # `file_size_threshold` selects inputs rather than sizing outputs.
            # Partition evolution has no procedure at all.
            layout=frozenset({LayoutFeature.SORT}),
            operations={
                Operation.COMPACT: OperationSupport(
                    Operation.COMPACT,
                    Support.PARTIAL,
                    can_preview=False,
                    limitations=(
                        (
                            "no Z-order. Verified against the connector source: zero "
                            "occurrences of zorder/z-order/morton/interleave in all 474 "
                            "files, and no open issue proposing it. Only the leading sort "
                            "column gets file skipping, so a filter on any other column "
                            "reads every file in every surviving partition"
                        ),
                        (
                            "sorts by identity transforms only -- IcebergMetadata."
                            "getSupportedSortFields skips non-identity sort fields, and "
                            "stamps sort_order_id=unsorted when any was skipped"
                        ),
                        (
                            "`file_size_threshold` selects which files to merge; there is "
                            "no control over output file size"
                        ),
                    ),
                ),
                Operation.EXPIRE: OperationSupport(
                    Operation.EXPIRE,
                    Support.PARTIAL,
                    can_preview=False,
                    limitations=(
                        "no `max_ref_age_days`: the spec's retention step 2 is unavailable",
                        (
                            "`retention_threshold` must be >= "
                            "`iceberg.expire-snapshots.min-retention` or the procedure "
                            f"fails (documented default {DEFAULT_MIN_RETENTION_DAYS}d)"
                        ),
                        (
                            "`retain_last` -- our min_snapshots_to_keep -- was added in "
                            "Trino 479; against an older server the argument does not "
                            "exist and the setting cannot be honoured"
                        ),
                    ),
                    invariants=SERVER_SIDE,
                ),
                Operation.REMOVE_ORPHANS: OperationSupport(
                    Operation.REMOVE_ORPHANS,
                    Support.PARTIAL,
                    can_preview=False,
                    limitations=(
                        (
                            "`retention_threshold` must be >= "
                            "`iceberg.remove-orphan-files.min-retention` or the procedure "
                            f"fails (documented default {DEFAULT_MIN_RETENTION_DAYS}d)"
                        ),
                    ),
                    invariants=(
                        *SERVER_SIDE,
                        (
                            "the server floor cannot be overridden, which is *stricter* "
                            "than our guard -- there is no Trino equivalent of --reclaim-now"
                        ),
                    ),
                ),
                Operation.REMOVE_DANGLING_DELETES: OperationSupport(
                    Operation.REMOVE_DANGLING_DELETES,
                    Support.UNSUPPORTED,
                    can_preview=False,
                    limitations=(
                        (
                            "Trino has no procedure for it and no option that achieves it "
                            "as a side effect; run this operation on another engine"
                        ),
                    ),
                ),
                Operation.REWRITE_MANIFESTS: OperationSupport(
                    Operation.REWRITE_MANIFESTS,
                    Support.PARTIAL,
                    can_preview=False,
                    limitations=(
                        (
                            "`optimize_manifests` takes no arguments; output size comes "
                            "from the `commit.manifest.target-size-bytes` table property"
                        ),
                    ),
                ),
                Operation.APPLY_PROPERTIES: OperationSupport(
                    Operation.APPLY_PROPERTIES,
                    Support.PARTIAL,
                    can_preview=False,
                    limitations=(
                        (
                            "the Iceberg property names are refused -- Trino's table "
                            "properties are an allowlist and `write.metadata.*` is in "
                            "PROTECTED_ICEBERG_NATIVE_PROPERTIES, rejected even via "
                            "`extra_properties`. Translated to Trino's own "
                            "`max_previous_versions` and `delete_after_commit_enabled`, "
                            "which are the same two settings under different names"
                        ),
                    ),
                ),
            },
        )

    def validate(self, operation: Operation, request: MaintenanceRequest) -> tuple[str, ...]:
        """Catch the floor collision at plan time.

        A ``table-config.json`` that is entirely valid is unusable here: our
        defaults are 5 days for expiry and 3 for orphan removal, and Trino's
        documented floors are 7 for both. Finding that out from a server error
        part-way through a fleet run is what this exists to prevent.
        """
        problems: list[str] = []
        if operation is Operation.EXPIRE:
            days = (
                request.max_snapshot_age_days
                if request.max_snapshot_age_days is not None
                else request.retention.expire_snapshots.max_snapshot_age_days
            )
            if days is not None and days < DEFAULT_MIN_RETENTION_DAYS:
                problems.append(
                    f"max_snapshot_age_days is {days}, below Trino's "
                    f"`iceberg.expire-snapshots.min-retention`, which defaults to "
                    f"{DEFAULT_MIN_RETENTION_DAYS}d. Trino fails the procedure rather "
                    "than clamping -- verified live. Raise the value, or lower the "
                    "floor with the `iceberg.expire_snapshots_min_retention` session "
                    "property or the catalog property of the same name."
                )
        if operation is Operation.EXPIRE and not self.supports_retain_last:
            keep = (
                request.min_snapshots_to_keep
                if request.min_snapshots_to_keep is not None
                else request.retention.expire_snapshots.min_snapshots_to_keep
            )
            if keep is not None:
                problems.append(
                    f"min_snapshots_to_keep is {keep}, but `retain_last` was added in "
                    f"Trino {self.RETAIN_LAST_MIN_VERSION} and this connection is "
                    + (f"{self.version}" if self.version else "of unknown version")
                    + ". Expiry would silently keep a different number of snapshots. "
                    "Pass --trino-version if the server is newer, or drop the setting."
                )

        if operation is Operation.REMOVE_ORPHANS:
            days = (
                request.older_than_days
                if request.older_than_days is not None
                else request.retention.remove_orphan_files.older_than_days
            )
            if days is not None and days < DEFAULT_MIN_RETENTION_DAYS:
                problems.append(
                    f"older_than_days is {days}, below Trino's "
                    f"`iceberg.remove-orphan-files.min-retention`, which defaults to "
                    f"{DEFAULT_MIN_RETENTION_DAYS}d. Trino fails the procedure rather "
                    "than clamping -- verified live. Raise the value, or lower the "
                    "floor with the `iceberg.remove_orphan_files_min_retention` session "
                    "property or the catalog property of the same name."
                )
        return tuple(problems)

    # -- statement building: pure, so every emitted statement is testable ----

    def statement_for(self, operation: Operation, table: str, request: MaintenanceRequest) -> str:
        """The exact SQL this maintainer would run. No connection needed."""
        self.check_supported(operation)
        target = qualified(table, catalog=self.catalog)
        builder = {
            Operation.COMPACT: self._compact_sql,
            Operation.EXPIRE: self._expire_sql,
            Operation.REMOVE_ORPHANS: self._remove_orphans_sql,
            Operation.REWRITE_MANIFESTS: self._rewrite_manifests_sql,
            Operation.APPLY_PROPERTIES: self._apply_properties_sql,
        }[operation]
        return builder(target, request)

    def _compact_sql(self, target: str, request: MaintenanceRequest) -> str:
        # `file_size_threshold` is a dataSizeProperty defaulting to 100MB. It
        # *selects* which files to merge -- it is not a target output size, which
        # Trino does not expose at all, so our target_file_size_bytes cannot be
        # translated and is deliberately not passed as if it could.
        pairs: list[tuple[str, str]] = []
        if request.compaction is not None:
            threshold = getattr(request.compaction, "target_file_size_bytes", None)
            if threshold:
                pairs.append(("file_size_threshold", f"'{threshold}B'"))
        return _procedure(target, "optimize", pairs)

    def _expire_sql(self, target: str, request: MaintenanceRequest) -> str:
        settings = request.retention.expire_snapshots
        days = (
            request.max_snapshot_age_days
            if request.max_snapshot_age_days is not None
            else settings.max_snapshot_age_days
        )
        keep = (
            request.min_snapshots_to_keep
            if request.min_snapshots_to_keep is not None
            else settings.min_snapshots_to_keep
        )
        pairs: list[tuple[str, str]] = []
        if days is not None:
            pairs.append(("retention_threshold", _days(days)))
        if keep is not None and self.supports_retain_last:
            # `retain_last` is the spec's min-snapshots-to-keep under another
            # name. Added in **Trino 479** -- emitting it against 476 fails with
            # "property 'retain_last' does not exist", which is how this was
            # found. Gated rather than always-emitted so an older Trino still
            # gets a working expiry, with the loss declared rather than hidden.
            pairs.append(("retain_last", str(keep)))
        return _procedure(target, "expire_snapshots", pairs)

    def _remove_orphans_sql(self, target: str, request: MaintenanceRequest) -> str:
        settings = request.retention.remove_orphan_files
        days = (
            request.older_than_days
            if request.older_than_days is not None
            else settings.older_than_days
        )
        pairs: list[tuple[str, str]] = []
        if days is not None:
            pairs.append(("retention_threshold", _days(days)))
        return _procedure(target, "remove_orphan_files", pairs)

    def _rewrite_manifests_sql(self, target: str, request: MaintenanceRequest) -> str:
        # Takes no arguments; output size comes from the table property
        # `commit.manifest.target-size-bytes`, so min_input_manifests has no
        # counterpart and is not silently dropped -- it is declared missing.
        return _procedure(target, "optimize_manifests", [])

    def _apply_properties_sql(self, target: str, request: MaintenanceRequest) -> str:
        # Trino does *not* accept the Iceberg property names. Its table
        # properties are an allowlist, and `write.metadata.*` is rejected even
        # through `extra_properties` ("Illegal keys in extra_properties") because
        # both names are in PROTECTED_ICEBERG_NATIVE_PROPERTIES. It exposes the
        # same two settings under its own names instead, so this is a genuine
        # translation -- found by running the Iceberg names and being refused.
        settings = request.retention.metadata
        pairs = []
        if settings.previous_versions_max is not None:
            pairs.append(f"max_previous_versions = {settings.previous_versions_max}")
        if settings.delete_after_commit is not None:
            value = "true" if settings.delete_after_commit else "false"
            pairs.append(f"delete_after_commit_enabled = {value}")
        if not pairs:
            raise EngineConfigProblem(
                "apply-properties has nothing to set: the config declares neither "
                "previous_versions_max nor delete_after_commit."
            )
        return f"ALTER TABLE {target} SET PROPERTIES {', '.join(pairs)}"

    # -- execution ------------------------------------------------------------

    def connect(self):
        """A DBAPI connection to Trino.

        Imported here rather than at module scope: the client is an optional
        dependency, and importing this module must not require it -- `zamboni
        engines` reports Trino's capabilities on an install that has no Trino
        client at all.
        """
        try:
            from trino.dbapi import connect
        except ImportError as exc:  # pragma: no cover - depends on the install
            raise EngineConfigProblem(
                "the trino engine needs the Trino client: install zamboni[trino]."
            ) from exc

        return connect(host=self.host, port=self.port, user=self.user, catalog=self.catalog)

    def execute(
        self,
        operation: Operation,
        table: str,
        *,
        request: MaintenanceRequest,
        dry_run: bool,
    ) -> Reportable:
        statement = self.statement_for(operation, table, request)
        if dry_run:
            # Reachable only by calling execute() directly; the CLI refuses
            # first. Belt and braces, because returning a "preview" from an
            # engine that cannot preview is the one failure this must not have.
            raise PreviewUnavailable(
                f"trino cannot preview {operation.value}. The statement would be: {statement}"
            )

        logger.info("trino: %s", statement)
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(statement)
            rows = tuple(cursor.fetchall())
        return TrinoResult(operation=operation, table=table, statement=statement, rows=rows)
