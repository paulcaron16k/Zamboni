# SPDX-License-Identifier: Apache-2.0
"""Spark, over the Iceberg stored procedures.

Argument names and defaults come from the Iceberg ``spark-procedures.md``
source, recorded in docs/engine-comparison.md. As with Trino, **statement
generation is separated from execution**: the builders are pure, so every
emitted ``CALL`` is asserted exactly without starting a JVM, and only
:meth:`SparkMaintainer.execute` needs a session.

That split matters more here than anywhere else. Spark is ~300MB and a JVM, so
"run the tests" cannot mean "start Spark" -- and the alternative to pure
builders would be a maintainer nobody exercises.

Three declarations here shaped the interface:

* ``remove-dangling-deletes`` is ``fulfilled_by`` COMPACT. Spark has no
  standalone procedure for it -- it is the ``remove-dangling-deletes`` option on
  ``rewrite_data_files``, which "will generate an additional commit for the
  removal". Without that field the interface would have to call this
  unsupported, which is wrong, or supported, which is misleading about *how*.
* ``remove-orphans`` previews and nothing else does. ``remove_orphan_files``
  takes ``dry_run``; no other Spark procedure does. This single asymmetry is why
  ``can_preview`` is per operation.
* Spark does something we cannot: it drops dangling deletes *per file*, where we
  can only drop whole manifests (ZMBNI-604). Capability flows in both
  directions, and the interface has to carry that.
"""

from __future__ import annotations

import datetime as dt
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

    from ..config import CompactionConfig
    from ..session import CatalogSession

logger = logging.getLogger(__name__)

SERVER_SIDE = ("runs server-side, so none of Zamboni's client-side reclaim invariants apply",)


def quote(identifier: str) -> str:
    """Backtick-quote a Spark identifier, doubling any embedded backtick.

    Spark quotes with backticks where Trino uses double quotes -- the one
    lexical difference between the two implementations, and reason enough not to
    share a helper between them.
    """
    return "`" + identifier.replace("`", "``") + "`"


def qualified(table: str, *, catalog: str) -> str:
    """``db.events`` -> ```iceberg`.`db`.`events```.

    **Every** dot is a separator, so ``a.b.events`` becomes four quoted parts and
    not three. That is the opposite of Trino, and the difference is not a style
    choice -- verified against both servers with a real nested namespace:

    ============================= ============================ ==========
    form                          Trino                        Spark
    ============================= ============================ ==========
    one part, dotted name         3 rows                       rejected
    one quoted part per level     "Too many dots in table name" 3 rows
    ============================= ============================ ==========

    Trino's schema is a single identifier that may *contain* a dot, which is what
    ``iceberg.rest-catalog.nested-namespace-enabled`` turns on. Spark has real
    multi-level namespaces and rejects a dot inside a part outright
    ("Namespace parts cannot contain '.'"). The same table therefore has two
    spellings that each engine refuses from the other, which is the strongest
    argument in the codebase for nested namespaces being more trouble than they
    are worth -- see docs/table-config.md.

    This split needs no help from the config because Spark forbids the ambiguous
    case: a dot can only ever be a separator.
    """
    parts = table.split(".")
    if len(parts) < 2:
        raise ValueError(f"{table!r} has no namespace; expected <namespace>.<table>")
    return ".".join(quote(part) for part in (catalog, *parts))


def _literal(value: str) -> str:
    """A Spark string literal, single-quoted with quotes doubled."""
    return "'" + value.replace("'", "''") + "'"


def _options(pairs: list[tuple[str, str]]) -> str:
    """Iceberg's procedure options are a Spark ``map(...)`` of string pairs."""
    flat = ", ".join(f"{_literal(k)}, {_literal(v)}" for k, v in pairs)
    return f"map({flat})"


def _call(procedure: str, arguments: list[tuple[str, str]]) -> str:
    rendered = ", ".join(f"{name} => {value}" for name, value in arguments)
    return f"CALL system.{procedure}({rendered})"


def _utc_now() -> dt.datetime:
    """Indirection so tests can pin the clock and assert an exact statement."""
    return dt.datetime.now(dt.UTC)


def _days_ago(days: int) -> str:
    """``older_than`` as a timestamp *string literal*.

    Two things had to be learned by running it. Spark takes a timestamp where
    Trino takes a duration -- the same setting, two vocabularies. And a ``CALL``
    argument must be a literal: `older_than => date_sub(current_timestamp(), 7)`
    is rejected with ``mismatched input '(' expecting STRING``, so the
    expression cannot be evaluated server-side.

    **The consequence is that the clock is the client's, not the engine's.** A
    maintenance host whose clock runs ahead of the metadata timestamps expires
    more than intended. Trino does not have this exposure, because a duration is
    resolved where the data is. Declared as a limitation rather than hidden.
    """
    stamp = (_utc_now() - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S+00:00")
    # A *typed* literal carrying an explicit offset. Three things had to be
    # learned by running it:
    #
    #   `date_sub(current_timestamp(), 7)` -- rejected by the parser,
    #       "mismatched input '(' expecting STRING".
    #   `'2026-08-06 12:00:00'` -- rejected by the type checker, "cannot cast
    #       StringType to TimestampType".
    #   `TIMESTAMP '2026-08-06 12:00:00'` -- accepted, and **wrong**. A bare
    #       wall-clock is read in `spark.sql.session.timeZone`, not UTC. Against
    #       a session in America/New_York that shifted our UTC value four hours
    #       later, so every expiry cut four hours deeper than asked. It is also
    #       the real reason a 1-day orphan interval was refused: 24h minus the
    #       offset falls under Spark's 24h floor. An earlier comment here blamed
    #       the JVM start-up round trip, which is backwards -- elapsed time makes
    #       an interval *longer*, not shorter.
    #
    # The offset removes the ambiguity without touching the operator's session
    # timezone, which is not ours to change.
    return f"TIMESTAMP {_literal(stamp)}"


@dataclass(frozen=True)
class SparkResult:
    """What a procedure returns.

    Unlike Trino, Iceberg's Spark procedures *do* return a result row -- e.g.
    ``rewrite_data_files`` yields rewritten/added file counts. Captured verbatim
    rather than interpreted, because the columns differ per procedure and
    inventing a common shape would mean guessing at several of them.
    """

    operation: Operation
    table: str
    statement: str
    rows: tuple[Any, ...] = ()

    def describe(self) -> str:
        lines = [f"{self.table}: {self.operation.value} via spark", f"  {self.statement}"]
        if self.rows:
            lines.extend(f"  {row}" for row in self.rows)
        else:
            lines.append("  committed; the procedure returned no rows.")
        return "\n".join(lines)


@register
class SparkMaintainer(Maintainer):
    name = "spark"

    DEFAULT_CATALOG = "iceberg"

    def __init__(self, session: CatalogSession, options: Mapping[str, str] | None = None) -> None:
        super().__init__(session, options)
        self.catalog = self._options.get("catalog") or self.DEFAULT_CATALOG

    @classmethod
    def capabilities(cls) -> MaintainerCapabilities:
        return MaintainerCapabilities(
            engine=cls.name,
            # Everything except partition evolution: `rewrite_data_files` takes
            # `output-spec-id`, which would do it, but Zamboni does not expose
            # that yet -- so it is a property of Spark rather than a capability
            # reachable from here.
            layout=frozenset(
                {
                    LayoutFeature.ZORDER,
                    LayoutFeature.SORT,
                    LayoutFeature.TARGET_FILE_SIZE,
                }
            ),
            operations={
                Operation.COMPACT: OperationSupport(
                    Operation.COMPACT,
                    Support.FULL,
                    can_preview=False,
                    invariants=(
                        (
                            "also removes dangling deletes in the same statement, unless "
                            "`remove_dangling_deletes` is disabled or "
                            "`dangling_delete_policy` is `block` -- Spark has no standalone "
                            "procedure, so the two operations share one call"
                        ),
                        (
                            "`partial-progress.enabled` defaults to false, so a run "
                            "commits once -- the same default Zamboni chose"
                        ),
                        (
                            "the procedure accepts `output-spec-id`, which would reorganise "
                            "into a different partition spec during the rewrite. Zamboni "
                            "does not expose it, so partition evolution stays a local-engine "
                            "capability"
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
                            "`older_than` is a timestamp literal computed on *this* host -- "
                            "a CALL argument cannot be an expression, so a clock ahead of "
                            "the metadata expires more than intended"
                        ),
                    ),
                    invariants=SERVER_SIDE,
                ),
                Operation.REMOVE_ORPHANS: OperationSupport(
                    Operation.REMOVE_ORPHANS,
                    Support.PARTIAL,
                    # The one operation on any non-local engine that previews.
                    can_preview=True,
                    limitations=(
                        (
                            "refuses an interval under 24 hours outright -- a third floor "
                            "behaviour: ours defaults to 3 days but allows 0 via "
                            "--reclaim-now, Trino has a configurable floor defaulting to 7 "
                            "days, Spark's 24 hours is hard-coded in the procedure. Exactly "
                            "1 day is refused too, because the timestamp is computed here "
                            "and Spark evaluates it moments later"
                        ),
                        (
                            "lists with Hadoop FileSystem, not Iceberg FileIO, so it needs "
                            "its own `spark.hadoop.fs.s3a.*` configuration and credentials "
                            "-- the catalog's vended credentials are not enough, though "
                            "they suffice for every other operation"
                        ),
                    ),
                    invariants=(
                        *SERVER_SIDE,
                        "`dry_run` previews; the only Spark procedure that can",
                        (
                            "`prefix_mismatch_mode` defaults to ERROR, which is the same "
                            "abort-on-doubt posture as ours for a different failure"
                        ),
                        (
                            "the procedure also accepts `file_list_view`, which would "
                            "replace the directory listing entirely -- the answer to a "
                            "remote-signing warehouse. Zamboni does not expose it yet, so "
                            "this is a property of Spark, not a capability you can reach "
                            "from here (ZMBNI-1602)"
                        ),
                    ),
                ),
                Operation.REMOVE_DANGLING_DELETES: OperationSupport(
                    Operation.REMOVE_DANGLING_DELETES,
                    Support.FULL,
                    can_preview=False,
                    fulfilled_by=Operation.COMPACT,
                    invariants=(
                        (
                            "the `remove-dangling-deletes` option on `rewrite_data_files`; "
                            "generates an additional commit for the removal"
                        ),
                        (
                            "drops deletes per file, where the local engine can only drop "
                            "whole manifests -- strictly better than ours here"
                        ),
                    ),
                ),
                Operation.REWRITE_MANIFESTS: OperationSupport(
                    Operation.REWRITE_MANIFESTS,
                    Support.FULL,
                    can_preview=False,
                    invariants=("`sort_by` clusters manifests by chosen partition transforms",),
                ),
                Operation.APPLY_PROPERTIES: OperationSupport(
                    Operation.APPLY_PROPERTIES,
                    Support.FULL,
                    can_preview=False,
                ),
            },
        )

    #: Hard-coded in Iceberg's `remove_orphan_files` procedure, not configurable:
    #: "Cannot remove orphan files with an interval less than 24 hours."
    MINIMUM_ORPHAN_INTERVAL_DAYS = 2

    def validate(self, operation: Operation, request: MaintenanceRequest) -> tuple[str, ...]:
        """Catch Spark's orphan floor before a session is even started.

        Worth doing here rather than letting the procedure raise: starting a
        SparkSession costs a JVM and a jar download, and failing after that for
        a reason knowable up front is a poor trade.
        """
        if operation is not Operation.REMOVE_ORPHANS:
            return ()
        days = (
            request.older_than_days
            if request.older_than_days is not None
            else request.retention.remove_orphan_files.older_than_days
        )
        if days is not None and days < self.MINIMUM_ORPHAN_INTERVAL_DAYS:
            return (
                (
                    f"older_than_days is {days}, but Spark's remove_orphan_files "
                    "refuses any interval under 24 hours -- hard-coded in the "
                    "procedure, not a configurable floor. Raise it to at least 2 -- "
                    "exactly 1 is refused because the timestamp is computed here and "
                    "evaluated moments later -- or "
                    "run this operation on the local engine, which allows a shorter "
                    "guard deliberately."
                ),
            )
        return ()

    # -- statement building: pure, so no JVM is needed to test it -----------

    def statement_for(
        self,
        operation: Operation,
        table: str,
        request: MaintenanceRequest,
        *,
        dry_run: bool = False,
    ) -> str:
        """The exact SQL this maintainer would run. No session needed.

        `dry_run` is a parameter rather than something `execute` splices in
        afterwards. The first version did the splice -- a `str.replace` of
        ``"CALL system.remove_orphan_files("`` -- which meant the preview flag
        depended on the exact spelling of a string produced elsewhere: rename
        the procedure, change the spacing in `_call`, and the replace silently
        matches nothing and the "preview" deletes files for real. Building it
        as an argument makes that class of failure impossible, and makes the
        preview statement inspectable without running anything.
        """
        self.check_supported(operation)
        target = qualified(table, catalog=self.catalog)
        plain = self._plain(table)
        if operation is Operation.REMOVE_ORPHANS:
            return self._remove_orphans_sql(target, plain, request, dry_run=dry_run)
        builder = {
            Operation.COMPACT: self._compact_sql,
            Operation.EXPIRE: self._expire_sql,
            Operation.REMOVE_ORPHANS: self._remove_orphans_sql,
            Operation.REMOVE_DANGLING_DELETES: self._compact_sql,
            Operation.REWRITE_MANIFESTS: self._rewrite_manifests_sql,
            Operation.APPLY_PROPERTIES: self._apply_properties_sql,
        }[operation]
        return builder(target, plain, request)

    def _compact_sql(self, target: str, plain: str, request: MaintenanceRequest) -> str:
        """``rewrite_data_files``, which also carries dangling-delete removal.

        Spark has no standalone procedure for dangling deletes -- it is the
        ``remove-dangling-deletes`` option here, which "will generate an
        additional commit for the removal". That is why the capability
        declaration marks the operation ``fulfilled_by`` COMPACT, and why both
        route to this builder.
        """
        arguments: list[tuple[str, str]] = [("table", _literal(plain))]

        # Honour the settings rather than hard-coding "true", which is what the
        # first version did -- so an operator who disabled dangling-delete
        # removal in table-config.json, or set `dangling_delete_policy: block`
        # specifically to make compaction refuse rather than touch delete files,
        # got them deleted anyway and silently. Iceberg's option accepts false;
        # nothing forced the hard-coding.
        drop_dangling = request.retention.remove_dangling_deletes.enabled
        if request.compaction is not None and request.compaction.dangling_delete_policy == "block":
            # `block` means "refuse rather than proceed" on the local engine.
            # Spark cannot express that mid-rewrite, so the honest translation
            # is to not remove them and say so, rather than to silently do the
            # opposite of what the policy asks.
            drop_dangling = False
        options: list[tuple[str, str]] = [
            ("remove-dangling-deletes", "true" if drop_dangling else "false")
        ]

        compaction = request.compaction
        if compaction is not None:
            # Direct attribute access, not getattr-with-default. The first draft
            # of this used `getattr(compaction, "zorder_by", None)` -- a field
            # that does not exist -- so Z-order would have been silently dropped
            # forever, which is precisely the failure this interface exists to
            # prevent. A wrong name must raise, not return None.
            if compaction.target_file_size_bytes:
                options.append(("target-file-size-bytes", str(compaction.target_file_size_bytes)))
            if compaction.min_input_files:
                options.append(("min-input-files", str(compaction.min_input_files)))
            # Iceberg's own default is false, and ZMBNI-106 chose the same for
            # the local engine. Pass it explicitly so the two agree visibly
            # rather than by coincidence.
            options.append(
                ("partial-progress.enabled", "true" if compaction.partial_progress else "false")
            )
            if (
                compaction.zorder_columns
                or compaction.sort_expression
                or (compaction.sort_by_table_order)
            ):
                arguments.append(("strategy", _literal("sort")))
                if expression := self._sort_expression(compaction):
                    arguments.append(("sort_order", _literal(expression)))

        arguments.append(("options", _options(options)))
        return _call("rewrite_data_files", arguments)

    @staticmethod
    def _sort_expression(compaction: CompactionConfig) -> str | None:
        """Spark is the only non-local engine that can Z-order.

        ``zorder(a, b)`` for multi-key; otherwise the declared sort expression.
        ``sort_by_table_order`` returns None deliberately -- it still selects the
        ``sort`` strategy, and Spark then defaults ``sort_order`` to the table's
        own, which is exactly what that setting means.
        """
        if compaction.zorder_columns:
            return f"zorder({', '.join(compaction.zorder_columns)})"
        if compaction.sort_expression:
            return str(compaction.sort_expression)
        return None

    def _expire_sql(self, target: str, plain: str, request: MaintenanceRequest) -> str:
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
        arguments: list[tuple[str, str]] = [("table", _literal(plain))]
        if days is not None:
            # A timestamp here, where Trino takes a duration. The same setting,
            # two vocabularies -- which is what MaintenanceRequest exists for.
            arguments.append(("older_than", _days_ago(days)))
        if keep is not None:
            arguments.append(("retain_last", str(keep)))
        return _call("expire_snapshots", arguments)

    def _remove_orphans_sql(
        self,
        target: str,
        plain: str,
        request: MaintenanceRequest,
        *,
        dry_run: bool = False,
    ) -> str:
        settings = request.retention.remove_orphan_files
        days = (
            request.older_than_days
            if request.older_than_days is not None
            else settings.older_than_days
        )
        arguments: list[tuple[str, str]] = [("table", _literal(plain))]
        if dry_run:
            arguments.append(("dry_run", "true"))
        if days is not None:
            arguments.append(("older_than", _days_ago(days)))
        return _call("remove_orphan_files", arguments)

    def _rewrite_manifests_sql(self, target: str, plain: str, request: MaintenanceRequest) -> str:
        return _call("rewrite_manifests", [("table", _literal(plain))])

    def _apply_properties_sql(self, target: str, plain: str, request: MaintenanceRequest) -> str:
        """Spark takes the Iceberg property names directly.

        Worth stating because Trino does not: there the same two settings need
        `max_previous_versions` and `delete_after_commit_enabled`, since its
        table properties are an allowlist. Spark passes them through.
        """
        settings = request.retention.metadata
        pairs = []
        if settings.previous_versions_max is not None:
            pairs.append(
                f"'write.metadata.previous-versions-max' = "
                f"{_literal(str(settings.previous_versions_max))}"
            )
        if settings.delete_after_commit is not None:
            value = "true" if settings.delete_after_commit else "false"
            pairs.append(f"'write.metadata.delete-after-commit.enabled' = {_literal(value)}")
        if not pairs:
            raise EngineConfigProblem(
                "apply-properties has nothing to set: the config declares neither "
                "previous_versions_max nor delete_after_commit."
            )
        return f"ALTER TABLE {target} SET TBLPROPERTIES ({', '.join(pairs)})"

    def _plain(self, table: str) -> str:
        """The identifier as a procedure argument: a plain string, not quoted SQL.

        Built from the original name, **never** by stripping backticks off the
        quoted form. That was the first implementation and it silently corrupted
        any name containing a backtick: ``quote()`` doubles an embedded backtick
        to escape it, and stripping every backtick collapsed the escape and the
        delimiters together, so ``we`ird.ta-ble`` became ``weird.ta-ble`` -- a
        *different table*, targeted with no error, by operations that delete
        files.
        """
        namespace, _, name = table.rpartition(".")
        if not namespace:
            raise ValueError(f"{table!r} has no namespace; expected <namespace>.<table>")
        return f"{self.catalog}.{namespace}.{name}"

    # -- execution ------------------------------------------------------------

    def connect(self):
        """A SparkSession: over Spark Connect if `remote` is set, else local.

        Imported here, not at module scope: PySpark is optional and importing
        this module must not require a JVM -- `zamboni engines` reports Spark's
        capabilities on an install that has never seen it.

        The two modes differ in more than a URL.

        **Connect** (`remote`, an `sc://` URL) runs no JVM here at all. The
        session belongs to a server someone else operates, which is why
        `_session_config` is *not* applied to it: `spark.sql.extensions` and
        `spark.hadoop.*` are read when that server starts its context, so
        sending them from a client is at best ignored and at worst an error on a
        server that rejects static conf. It also means the Iceberg extensions
        and the S3A credentials `remove-orphans` needs are the server operator's
        responsibility, and `zamboni doctor` cannot check them from here. The
        payoff is that `zamboni[spark]` is ~13MB against `zamboni[spark-lib]`'s
        ~472MB, and a developer needs no Java.

        **Local** starts a driver JVM in this process, so the machine's Java
        version becomes ours: Spark 3.x wants Java 8/11/17, Spark 4 wants 17 or
        21.
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover - depends on the install
            raise EngineConfigProblem(
                "the spark engine needs PySpark: install zamboni[spark] to "
                "drive a Spark Connect server with --spark-remote, or "
                "zamboni[spark-lib] for an embedded local session."
            ) from exc

        if remote := self._options.get("remote"):
            if self._options.get("master"):
                raise EngineConfigProblem(
                    "--spark-remote and --spark-master are mutually exclusive: "
                    "Connect attaches to a session someone else started, and a "
                    "master tells this process to start its own."
                )
            return SparkSession.builder.remote(remote).getOrCreate()

        builder = SparkSession.builder.appName("zamboni")
        if master := self._options.get("master"):
            builder = builder.master(master)
        for key, value in self._session_config().items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    def _session_config(self) -> dict[str, str]:
        """Only what Iceberg needs; everything else is the operator's business.

        Deliberately does not invent a catalog configuration. A deployment that
        runs Spark already has one, and a maintenance tool silently overriding
        `spark.sql.catalog.*` would be changing where the data is.

        Local mode only -- see :meth:`connect` for why Connect cannot take these.
        """
        config = {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
        }
        config.update(
            {k[len("conf.") :]: v for k, v in self._options.items() if k.startswith("conf.")}
        )
        return config

    def execute(
        self,
        operation: Operation,
        table: str,
        *,
        request: MaintenanceRequest,
        dry_run: bool,
    ) -> Reportable:
        if dry_run and not self.capabilities().can_preview(operation):
            raise PreviewUnavailable(
                f"spark cannot preview {operation.value}. The statement would be: "
                f"{self.statement_for(operation, table, request)}"
            )
        statement = self.statement_for(operation, table, request, dry_run=dry_run)

        logger.info("spark: %s", statement)
        session = self.connect()
        rows = tuple(row.asDict() for row in session.sql(statement).collect())
        return SparkResult(operation=operation, table=table, statement=statement, rows=rows)
