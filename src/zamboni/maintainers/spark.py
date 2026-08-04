"""Spark, over the Iceberg stored procedures. Not implemented -- ZMBNI-15.

As with the Trino stub, the capability declaration is real: it comes from
docs/engine-comparison.md, read from the Iceberg ``spark-procedures.md`` source.

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
    pass

SERVER_SIDE = ("runs server-side, so none of Zamboni's client-side reclaim invariants apply",)


@register
class SparkMaintainer(Maintainer):
    name = "spark"

    @classmethod
    def capabilities(cls) -> MaintainerCapabilities:
        return MaintainerCapabilities(
            engine=cls.name,
            operations={
                Operation.COMPACT: OperationSupport(
                    Operation.COMPACT,
                    Support.FULL,
                    can_preview=False,
                    invariants=(
                        (
                            "`partial-progress.enabled` defaults to false, so a run "
                            "commits once -- the same default Zamboni chose"
                        ),
                        (
                            "can reorganise into a different partition spec during the "
                            "rewrite via `output-spec-id`"
                        ),
                    ),
                ),
                Operation.EXPIRE: OperationSupport(
                    Operation.EXPIRE,
                    Support.PARTIAL,
                    can_preview=False,
                    limitations=(
                        "no `max_ref_age_days`: the spec's retention step 2 is unavailable",
                    ),
                    invariants=SERVER_SIDE,
                ),
                Operation.REMOVE_ORPHANS: OperationSupport(
                    Operation.REMOVE_ORPHANS,
                    Support.FULL,
                    # The one operation on any non-local engine that previews.
                    can_preview=True,
                    invariants=(
                        *SERVER_SIDE,
                        "`dry_run` previews; the only Spark procedure that can",
                        (
                            "`prefix_mismatch_mode` defaults to ERROR, which is the same "
                            "abort-on-doubt posture as ours for a different failure"
                        ),
                        (
                            "`file_list_view` can replace the directory listing entirely, "
                            "which is the answer to a remote-signing warehouse"
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
            f"the spark maintainer is not implemented yet (ZMBNI-15). Its declared "
            f"support for {operation.value} is already accurate -- run "
            "`zamboni engines` to see it."
        )
