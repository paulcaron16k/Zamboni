# SPDX-License-Identifier: Apache-2.0
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

    def __init__(self, *args, **kwargs) -> None:
        """Refuse a build whose internals would corrupt this commit.

        **The guard lives here, at the producer, rather than at each verb.** It
        used to be called in exactly one place -- ``TableCompactor.execute`` --
        while five of the six mutating operations reached
        ``_OverwriteFiles._existing_manifests`` without ever consulting it. Two
        of those five, ``rewrite-manifests`` and ``remove-dangling-deletes``,
        commit through subclasses of *this class*: on a build that prunes
        manifests by predicate without deriving the predicate correctly, the
        manifest holding a replaced file is kept verbatim and its rows are
        counted twice. Compaction refused and they proceeded (ZMBNI-37).

        Guarding the constructor covers every operation that commits through a
        producer *subclass*, and ``test_every_replace_producer_consults_the_guard``
        walks the subclass tree recursively so a grandchild circumventing
        ``super().__init__`` fails rather than slipping through.

        **It is not sufficient on its own**, which the first version of this fix
        got wrong: :meth:`ReplaceCommitter.commit` chooses its producer at
        runtime and two of the three choices are the stock ``_OverwriteFiles``.
        That method carries its own call; see it for which paths.

        This line cannot refuse a *preview*: every caller builds its producer
        after its own ``if dry_run: return``, so nothing that merely reports
        reaches here. ``TableCompactor.execute`` keeps a separate, earlier call
        which *does* precede its dry-run check -- so ``execute(dry_run=True)``
        refuses on an unusable build, deliberately: there is no point previewing
        a rewrite that cannot be committed, and refusing before the work beats
        refusing after it.
        """
        assert_supported_pyiceberg()
        super().__init__(*args, **kwargs)

    def _summary(self, snapshot_properties: dict[str, str] = EMPTY_DICT) -> Summary:
        # Built as OVERWRITE because `update_snapshot_summaries` rejects REPLACE,
        # then relabelled. The label restored afterwards is **whatever was asked
        # for**, not a hardcoded REPLACE: the committer sets `_operation` to
        # OVERWRITE for `snapshot_operation="overwrite"`, the escape hatch for
        # anyone unwilling to depend on a private producer subclass, and forcing
        # REPLACE here silently ignored it.
        #
        # `MultiSpecReplaceFiles._summary` used to correct this from the
        # subclass. That override is gone -- its per-spec half now lives in the
        # library -- so the fix belongs where the hardcoding was.
        wanted = self._operation
        self._operation = Operation.OVERWRITE
        try:
            summary = super()._summary(snapshot_properties)
        finally:
            self._operation = wanted
        # Summary is a frozen pydantic model, so rebuild rather than mutate.
        return Summary(operation=wanted, **summary.additional_properties)


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
        removed: list[DataFile],
        added: list[DataFile],
    ) -> CommitOutcome:
        """Swap ``removed`` for ``added`` in a single replace snapshot.

        Guarded here as well as in :meth:`_ReplaceFiles.__init__`, because the
        producer class is chosen at **runtime** and two of the three choices are
        not ``_ReplaceFiles``: ``snapshot_operation="overwrite"``, a documented
        option, selects the stock ``_OverwriteFiles``, and so does a build whose
        ``replace_summary_supported`` is true -- a *future* PyIceberg, which is
        exactly the kind most likely to carry the pruning defect the guard exists
        for. ``ReplaceCommitter`` is exported from :mod:`zamboni`, so a library
        caller reaches this without passing through
        :meth:`~zamboni.compactor.TableCompactor.execute` either.

        Guarding the producer alone made coverage a property of a class that is
        itself a runtime choice; the property has to be "every commit path".

        The guard runs **after** the empty-commit early return, deliberately: a
        call with nothing to add or remove produces no snapshot, so there is
        nothing for an unusable build to corrupt, and refusing it would fail a
        fleet run that simply had no work for a table.

        **This no longer checks the table's snapshot id**, and did until
        ZMBNI-79. That check refused whenever the table had moved *at all*,
        which is coarser than the loss it was guarding against: compaction
        replaces named files, so a commit touching none of them cannot cause a
        lost update. PyIceberg 0.12 validates the right thing --
        ``_validate_data_files_exist`` checks that every file this producer
        deletes still exists -- and does it per file. Proven adversarially by
        ``test_a_concurrent_commit_touching_our_files_is_still_refused``: with
        the old guard gone, a writer that rewrites a file in the removal set is
        still refused, the rows are not duplicated and its update survives.

        Keeping both was rejected. The old check is not a second opinion, it is
        a broader one: on a live catalog it refused 39 of 42 compactions that
        were touching partitions the writer never went near, so it cost the
        whole rewrite every time and protected nothing extra.
        """
        if not added and not removed:
            return CommitOutcome(tbl.metadata.current_snapshot_id, 0, 0, 0, 0)

        assert_supported_pyiceberg()

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

    **Who calls this, and who deliberately does not.** It is invoked from
    :meth:`_ReplaceFiles.__init__`, so every operation that commits through the
    private snapshot producers is covered by construction, plus once at the top
    of :meth:`~zamboni.compactor.TableCompactor.execute` to refuse before an
    expensive rewrite rather than after it.

    The three mutating operations that do **not** consult it, each for a stated
    reason rather than by omission (ZMBNI-37):

    * ``expire`` commits through PyIceberg's own ``ExpireSnapshots``, which
      removes snapshot entries from metadata. It never rewrites a manifest, so
      the pruning defect this guard exists for cannot reach it.
    * ``apply-properties`` commits a plain ``Table.transaction()`` that sets
      table properties. No manifests, no data files.
    * ``remove-orphans`` produces no snapshot at all -- it calls
      ``FileIO.delete`` on files the reachable set does not contain. Its own
      safety fence is the five invariants in design.md §6.6, which are about
      what may be deleted rather than about which PyIceberg is installed.

    If any of those three gains a manifest rewrite, it needs the guard, and
    ``test_every_replace_producer_consults_the_guard`` will not notice -- it
    watches producers, not verbs.
    """
    from .capabilities import detect

    caps = detect()
    if (reason := caps.unsupported_reason()) is not None:
        raise UnsupportedPyIceberg(f"pyiceberg {caps.version}: {reason}")
