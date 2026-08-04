"""Trino, over ``ALTER TABLE … EXECUTE``. Not implemented -- ZMBNI-14.

The capability declaration below is *not* a placeholder. It is the output of
docs/engine-comparison.md, read from the Trino connector documentation, and it
is what makes this stub worth having: an operator can ask what Trino would and
would not do before any of it is written, and ZMBNI-14 has an executable
specification rather than a prose one.

Two declarations here are the reason the interface has the shape it does:

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

#: Documented defaults for the catalog properties that impose a floor. A
#: deployment may raise these; it cannot be assumed to have lowered them.
DEFAULT_MIN_RETENTION_DAYS = 7

SERVER_SIDE = ("runs server-side, so none of Zamboni's client-side reclaim invariants apply",)


@register
class TrinoMaintainer(Maintainer):
    name = "trino"

    def __init__(self, session: CatalogSession) -> None:
        super().__init__(session)

    @classmethod
    def capabilities(cls) -> MaintainerCapabilities:
        return MaintainerCapabilities(
            engine=cls.name,
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
                    Support.FULL,
                    can_preview=False,
                    limitations=(),
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
                    f"max_snapshot_age_days is {days}, below Trino's documented "
                    f"`iceberg.expire-snapshots.min-retention` default of "
                    f"{DEFAULT_MIN_RETENTION_DAYS}d. Trino fails the procedure rather "
                    "than clamping. Raise the value, or lower the server setting."
                )
        if operation is Operation.REMOVE_ORPHANS:
            days = (
                request.older_than_days
                if request.older_than_days is not None
                else request.retention.remove_orphan_files.older_than_days
            )
            if days is not None and days < DEFAULT_MIN_RETENTION_DAYS:
                problems.append(
                    f"older_than_days is {days}, below Trino's documented "
                    f"`iceberg.remove-orphan-files.min-retention` default of "
                    f"{DEFAULT_MIN_RETENTION_DAYS}d. Trino fails the procedure rather "
                    "than clamping. Raise the value, or lower the server setting."
                )
        return tuple(problems)

    def execute(
        self,
        operation: Operation,
        table: str,
        *,
        request: MaintenanceRequest,
        dry_run: bool,
    ) -> Reportable:
        self.check_supported(operation)
        raise NotImplementedError(
            f"the trino maintainer is not implemented yet (ZMBNI-14). Its declared "
            f"support for {operation.value} is already accurate -- run "
            "`zamboni engines` to see it."
        )
