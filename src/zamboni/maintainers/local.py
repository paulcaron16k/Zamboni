"""The PyIceberg + DuckDB maintainer: what Zamboni was before it had a seam.

Extracted with no behaviour change. The 331 tests that already covered these
operations through the CLI are the regression net for that claim, which is why
this file translates and delegates rather than reimplementing anything.

Its distinguishing property is not a capability but a *guarantee*: everything
here previews, and the two reclaim operations abort rather than delete when
their own view of the world looks wrong. No procedure-based engine can offer
that, because the checks live in the caller.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..capabilities import detect
from . import (
    EngineConfigProblem,
    Maintainer,
    MaintainerCapabilities,
    MaintenanceRequest,
    Operation,
    OperationSupport,
    Reportable,
    Support,
    register,
)

if TYPE_CHECKING:
    pass


PREVIEWS_EVERYTHING = ("previews without committing; without --yes nothing is written",)

RECLAIM_INVARIANTS = (
    *PREVIEWS_EVERYTHING,
    "aborts rather than deletes when a referenced file is missing from the listing",
    "aborts when another table shares this table's location",
    "never deletes the current metadata pointer",
    "lists storage before computing reachability, so a concurrent commit survives",
)


@register
class LocalMaintainer(Maintainer):
    name = "local"

    @classmethod
    def capabilities(cls) -> MaintainerCapabilities:
        """Derived from the installed PyIceberg, not hardcoded.

        Three of these claims are properties of the *library*, not of this
        package: whether a delete manifest can be written, whether equality
        deletes can be read, whether the writer bin-packs a stream. They were
        written out as constants here, which made ``zamboni engines`` a static
        assertion about a dynamic thing -- wrong on any install whose probes
        differ, in the one place whose purpose is refusing to overstate
        capability (ZMBNI-1107).

        This is also the reason there is no ``local-0.12`` maintainer. The probes
        are the version mechanism; a version-named engine would be the version
        comparison ``capabilities.py`` exists to avoid. See docs/roadmap.md RM-1.
        """
        probes = detect()
        return MaintainerCapabilities(
            engine=cls.name,
            operations={
                Operation.COMPACT: cls._compact_support(probes),
                Operation.EXPIRE: OperationSupport(
                    Operation.EXPIRE,
                    Support.FULL,
                    can_preview=True,
                    invariants=(
                        *PREVIEWS_EVERYTHING,
                        "implements the spec's five-step retention, including max-ref-age-ms",
                        "deletes the set difference, so it never lists storage",
                    ),
                ),
                Operation.REMOVE_ORPHANS: OperationSupport(
                    Operation.REMOVE_ORPHANS,
                    Support.PARTIAL,
                    can_preview=True,
                    # Not probe-derived: this is a property of the *warehouse*,
                    # not the library. A remote-signing Lakekeeper refuses the
                    # listing however new PyIceberg is.
                    limitations=(
                        (
                            "needs a bucket listing, which a remote-signing warehouse "
                            "refuses outright -- no inventory-report path yet (ZMBNI-1602)"
                        ),
                    ),
                    invariants=RECLAIM_INVARIANTS,
                ),
                Operation.REMOVE_DANGLING_DELETES: cls._dangling_support(probes),
                Operation.REWRITE_MANIFESTS: OperationSupport(
                    Operation.REWRITE_MANIFESTS,
                    Support.FULL,
                    can_preview=True,
                    invariants=(
                        *PREVIEWS_EVERYTHING,
                        "preserves sequence numbers and snapshot ids exactly",
                    ),
                ),
                Operation.APPLY_PROPERTIES: OperationSupport(
                    Operation.APPLY_PROPERTIES,
                    Support.FULL,
                    can_preview=True,
                    invariants=(
                        *PREVIEWS_EVERYTHING,
                        "leaves an undeclared property alone rather than resetting it",
                    ),
                ),
            },
        )

    @staticmethod
    def _compact_support(probes) -> OperationSupport:
        """Compaction is the operation the probes actually gate.

        An unusable build is UNSUPPORTED rather than PARTIAL: ``unsupported_reason``
        is the check that stops a rewrite which would double-count rows, and
        declaring that as a caveat on a working operation would understate it.
        """
        invariants = [
            *PREVIEWS_EVERYTHING,
            "commits the whole run in one snapshot unless --partial-progress",
            "aborts on a row-count mismatch before committing",
            "refuses a PyIceberg build that would corrupt the table",
        ]
        if probes.streaming_write_supported:
            invariants.append("delegates bin-packing to PyIceberg's streaming writer")

        if reason := probes.unsupported_reason():
            return OperationSupport(
                Operation.COMPACT,
                Support.UNSUPPORTED,
                can_preview=False,
                limitations=(f"this PyIceberg build cannot be used: {reason}",),
            )
        if not probes.equality_deletes_readable:
            return OperationSupport(
                Operation.COMPACT,
                Support.PARTIAL,
                can_preview=True,
                limitations=(
                    (
                        f"PyIceberg {probes.version} cannot read equality deletes, so a "
                        "table carrying them is refused rather than compacted without "
                        "them -- doing otherwise would resurrect deleted rows"
                    ),
                ),
                invariants=tuple(invariants),
            )
        return OperationSupport(
            Operation.COMPACT,
            Support.FULL,
            can_preview=True,
            invariants=tuple(invariants),
        )

    @staticmethod
    def _dangling_support(probes) -> OperationSupport:
        """Whole-manifest-only is a property of the installed writer.

        ``ManifestWriterV2.content()`` returning DATA unconditionally is what
        limits this to dropping entire delete manifests. When a build can write
        a delete manifest, the limitation is gone and this must stop claiming it
        (ZMBNI-604).
        """
        if probes.delete_manifests_writable:
            return OperationSupport(
                Operation.REMOVE_DANGLING_DELETES,
                Support.FULL,
                can_preview=True,
                invariants=(
                    *PREVIEWS_EVERYTHING,
                    "rewrites a partially dangling delete manifest rather than leaving it alone",
                ),
            )
        return OperationSupport(
            Operation.REMOVE_DANGLING_DELETES,
            Support.PARTIAL,
            can_preview=True,
            limitations=(
                (
                    "drops whole delete manifests only; a partially dangling manifest "
                    "is left alone because ManifestWriterV2.content() returns DATA "
                    f"unconditionally in PyIceberg {probes.version}, so it cannot write "
                    "a delete manifest (ZMBNI-604)"
                ),
            ),
            invariants=PREVIEWS_EVERYTHING,
        )

    def execute(
        self,
        operation: Operation,
        table: str,
        *,
        request: MaintenanceRequest,
        dry_run: bool,
    ) -> Reportable:
        self.check_supported(operation)
        runner = {
            Operation.COMPACT: self._compact,
            Operation.EXPIRE: self._expire,
            Operation.REMOVE_ORPHANS: self._remove_orphans,
            Operation.REMOVE_DANGLING_DELETES: self._remove_dangling_deletes,
            Operation.REWRITE_MANIFESTS: self._rewrite_manifests,
            Operation.APPLY_PROPERTIES: self._apply_properties,
        }[operation]
        return runner(table, request, dry_run)

    # -- one method per operation, holding what the CLI handler used to ------

    def _compact(self, table: str, request: MaintenanceRequest, dry_run: bool) -> Reportable:
        from ..compactor import TableCompactor

        if request.compaction is None:
            # An assert here vanished under -O and, worse, surfaced as a
            # traceback rather than a message a cron log could act on.
            raise EngineConfigProblem(
                "compact needs a compaction config, and the request carried none. "
                "This is a caller bug: build the request with _request_for()."
            )
        if request.table_config is None:
            compactor = TableCompactor(self._session, table, request.compaction)
        else:
            # The file owns layout; the operational config still owns how the
            # run executes. Same split as the CLI had before the extraction.
            compactor = TableCompactor.from_table_config(
                self._session, table, request.table_config, base=request.compaction
            )
        return compactor.execute(dry_run=dry_run)

    def _expire(self, table: str, request: MaintenanceRequest, dry_run: bool) -> Reportable:
        from ..expire import RetentionPolicy, SnapshotExpirer

        settings = request.retention.expire_snapshots
        tbl = self._session.table(table)
        policy = RetentionPolicy.resolve(
            dict(tbl.properties),
            max_snapshot_age_days=(
                request.max_snapshot_age_days
                if request.max_snapshot_age_days is not None
                else settings.max_snapshot_age_days
            ),
            min_snapshots_to_keep=(
                request.min_snapshots_to_keep
                if request.min_snapshots_to_keep is not None
                else settings.min_snapshots_to_keep
            ),
            max_ref_age_days=settings.max_ref_age_days,
        )
        return SnapshotExpirer(policy, dry_run=dry_run).run(tbl)

    def _remove_orphans(self, table: str, request: MaintenanceRequest, dry_run: bool) -> Reportable:
        from ..orphans import OrphanCleaner

        settings = request.retention.remove_orphan_files
        older_than = (
            request.older_than_days
            if request.older_than_days is not None
            else settings.older_than_days
        )
        cleaner = OrphanCleaner(older_than_days=older_than, dry_run=dry_run)
        return cleaner.run(self._session.table(table))

    def _remove_dangling_deletes(
        self, table: str, request: MaintenanceRequest, dry_run: bool
    ) -> Reportable:
        from ..deletes import DanglingDeleteCleaner

        return DanglingDeleteCleaner(dry_run=dry_run).run(self._session.table(table))

    def _rewrite_manifests(
        self, table: str, request: MaintenanceRequest, dry_run: bool
    ) -> Reportable:
        from ..manifests import ManifestRewriter

        settings = request.retention.rewrite_manifests
        minimum = (
            request.min_input_manifests
            if request.min_input_manifests is not None
            else settings.min_input_manifests
        )
        return ManifestRewriter(min_input_manifests=minimum, dry_run=dry_run).run(
            self._session.table(table)
        )

    def _apply_properties(
        self, table: str, request: MaintenanceRequest, dry_run: bool
    ) -> Reportable:
        from ..properties import apply_metadata_properties

        return apply_metadata_properties(
            self._session.table(table), request.retention.metadata, dry_run=dry_run
        )
