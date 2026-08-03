"""Commit a compaction as an Iceberg ``replace`` snapshot.

``replace`` is the spec's operation for "data and delete files were added and
removed without changing table data; i.e., compaction" (see the
:class:`~pyiceberg.table.snapshots.Operation` docstring). It is not cosmetic:
incremental and CDC readers use the operation to decide whether a snapshot
changed any rows, so labelling a compaction ``overwrite`` makes them re-process
data that did not change.

PyIceberg cannot emit one on its own. ``UpdateSnapshot.overwrite()`` hardcodes
``Operation.OVERWRITE``, and -- more awkwardly -- ``update_snapshot_summaries``
rejects anything outside ``{APPEND, OVERWRITE, DELETE}``, in 0.11.1 and in
current main alike. So :class:`_ReplaceFiles` lets PyIceberg compute the summary
totals as an overwrite and relabels the finished summary. ``_operation`` is read
in exactly one place (summary construction) and the snapshot's operation is
taken from the summary, so the relabel is the whole change.

Set ``CompactionConfig.snapshot_operation = "overwrite"`` to avoid the subclass
entirely at the cost of the less precise label.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyiceberg.manifest import DataFile
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.table.update.snapshot import _OverwriteFiles
from pyiceberg.typedef import EMPTY_DICT

logger = logging.getLogger(__name__)


class _ReplaceFiles(_OverwriteFiles):
    """An overwrite producer that labels its snapshot ``replace``."""

    def _summary(self, snapshot_properties: dict[str, str] = EMPTY_DICT) -> Summary:
        self._operation = Operation.OVERWRITE
        try:
            summary = super()._summary(snapshot_properties)
        finally:
            self._operation = Operation.REPLACE
        # Summary is a frozen pydantic model, so rebuild rather than mutate.
        return Summary(operation=Operation.REPLACE, **summary.additional_properties)


class ConcurrentModification(RuntimeError):
    """The table moved on between planning and committing."""


class UnsupportedPyIceberg(RuntimeError):
    """The installed PyIceberg build cannot be driven safely."""


@dataclass(frozen=True)
class CommitOutcome:
    snapshot_id: int | None
    added_files: int
    removed_files: int
    added_bytes: int
    removed_bytes: int


class ReplaceCommitter:
    def __init__(
        self,
        *,
        branch: str = "main",
        snapshot_operation: str = "replace",
        snapshot_properties: dict[str, str] | None = None,
        producer_cls: type[_OverwriteFiles] | None = None,
    ) -> None:
        if snapshot_operation not in ("replace", "overwrite"):
            raise ValueError(
                f"snapshot_operation must be 'replace' or 'overwrite', got {snapshot_operation!r}"
            )
        self._branch = branch
        self._snapshot_operation = snapshot_operation
        self._snapshot_properties = snapshot_properties or {}
        #: Overridden by partition evolution, whose added files span more than
        #: one partition spec and so cannot share a single added manifest.
        self._producer_cls = producer_cls

    def commit(
        self,
        tbl: Table,
        *,
        expected_snapshot_id: int | None,
        removed: list[DataFile],
        added: list[DataFile],
    ) -> CommitOutcome:
        """Swap ``removed`` for ``added`` in a single replace snapshot.

        Args:
            expected_snapshot_id: The snapshot the plan was built against. The
                table is re-read and checked against this immediately before
                committing, so a concurrent writer produces a clean failure
                instead of a lost update.
        """
        if not added and not removed:
            return CommitOutcome(tbl.metadata.current_snapshot_id, 0, 0, 0, 0)

        tbl.refresh()
        current = tbl.current_snapshot()
        current_id = current.snapshot_id if current else None
        if current_id != expected_snapshot_id:
            raise ConcurrentModification(
                f"table snapshot changed between planning ({expected_snapshot_id}) "
                f"and commit ({current_id})"
            )

        properties = {
            "zamboni.operation": "compaction",
            "zamboni.removed-data-files": str(len(removed)),
            "zamboni.added-data-files": str(len(added)),
            **self._snapshot_properties,
        }

        from .capabilities import detect

        want_replace = self._snapshot_operation == "replace"
        # If a future PyIceberg accepts REPLACE in update_snapshot_summaries,
        # drop the subclass and use the stock producer.
        needs_relabel = want_replace and not detect().replace_summary_supported
        producer_cls = self._producer_cls or (_ReplaceFiles if needs_relabel else _OverwriteFiles)
        operation = Operation.REPLACE if want_replace else Operation.OVERWRITE

        with tbl.transaction() as txn:
            producer = producer_cls(
                operation=operation,
                transaction=txn,
                io=tbl.io,
                branch=self._branch,
                snapshot_properties=properties,
            )
            with producer as update:
                for data_file in removed:
                    update.delete_data_file(data_file)
                for data_file in added:
                    update.append_data_file(data_file)

        tbl.refresh()
        new_snapshot = tbl.current_snapshot()
        return CommitOutcome(
            snapshot_id=new_snapshot.snapshot_id if new_snapshot else None,
            added_files=len(added),
            removed_files=len(removed),
            added_bytes=sum(f.file_size_in_bytes for f in added),
            removed_bytes=sum(f.file_size_in_bytes for f in removed),
        )


def cleanup_orphans(tbl: Table, data_files: list[DataFile]) -> int:
    """Delete files written for a rewrite that was never committed.

    Best effort: a failure to delete leaves an orphan, which is a storage cost
    rather than a correctness problem.
    """
    deleted = 0
    for data_file in data_files:
        try:
            tbl.io.delete(data_file.file_path)
            deleted += 1
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning(
                "could not delete orphaned rewrite output %s: %s", data_file.file_path, exc
            )
    return deleted


def assert_supported_pyiceberg() -> None:
    """Refuse to run on a PyIceberg build whose internals would corrupt a table.

    The checks themselves live in :mod:`zamboni.capabilities`, which probes the
    installed build structurally rather than comparing version numbers.
    """
    from .capabilities import detect

    caps = detect()
    if (reason := caps.unsupported_reason()) is not None:
        raise UnsupportedPyIceberg(f"pyiceberg {caps.version}: {reason}")
