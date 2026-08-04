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

from . import (
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
    from ..session import CatalogSession


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

    def __init__(self, session: CatalogSession) -> None:
        super().__init__(session)

    @classmethod
    def capabilities(cls) -> MaintainerCapabilities:
        return MaintainerCapabilities(
            engine=cls.name,
            operations={
                Operation.COMPACT: OperationSupport(
                    Operation.COMPACT,
                    Support.FULL,
                    can_preview=True,
                    invariants=(
                        *PREVIEWS_EVERYTHING,
                        "commits the whole run in one snapshot unless --partial-progress",
                        "aborts on a row-count mismatch before committing",
                        "refuses a PyIceberg build that would corrupt the table",
                    ),
                ),
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
                    limitations=(
                        (
                            "needs a bucket listing, which a remote-signing warehouse "
                            "refuses outright -- no inventory-report path yet (ZMBNI-1602)"
                        ),
                    ),
                    invariants=RECLAIM_INVARIANTS,
                ),
                Operation.REMOVE_DANGLING_DELETES: OperationSupport(
                    Operation.REMOVE_DANGLING_DELETES,
                    Support.PARTIAL,
                    can_preview=True,
                    limitations=(
                        (
                            "drops whole delete manifests only; a partially dangling "
                            "manifest is left alone because ManifestWriterV2.content() "
                            "returns DATA unconditionally, so PyIceberg cannot write a "
                            "delete manifest (ZMBNI-604, still true on 0.12)"
                        ),
                    ),
                    invariants=PREVIEWS_EVERYTHING,
                ),
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

        assert request.compaction is not None, "compaction config is required to compact"
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
