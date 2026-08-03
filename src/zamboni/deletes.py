"""Remove delete files that no longer apply to any live data file.

Compaction applies deletes as it rewrites: the surviving rows are written
without the deleted ones, and the new data files carry a higher sequence
number, so the old delete files stop applying. They stay *referenced* though --
:class:`~pyiceberg.table.update.snapshot._OverwriteFiles` cannot remove them --
and the profiler has been reporting that as ``dangling-delete-files`` ever since
merge-on-read support landed. This module is what finally clears them.

Two upstream facts shape the design, both verified against 0.11.1 and main:

1. ``_OverwriteFiles._deleted_entries`` filters entries to
   ``DataFileContent.DATA``, so passing a delete file to ``delete_data_file``
   drops it from the rewritten manifest without recording a DELETED entry.
   :class:`_RemoveDeleteFiles` overrides that.

2. ``ManifestWriterV2.content()`` returns ``ManifestContent.DATA``
   unconditionally, and the avro metadata it writes agrees. There is no way to
   write a delete manifest. So a delete manifest can be **dropped whole** or
   left alone, but never rewritten -- rewriting one would produce a manifest
   claiming to hold data files, and a reader would then treat position deletes
   as rows.

Hence the contract: a delete file is removed only when *every* live entry in
its manifest is also dangling. Anything else is reported as retained, with the
reason. Reclaiming less is the conservative error; the alternative is silent
metadata corruption.

Whether a delete file still applies is not decided here. That question is
answered by ``pyiceberg.table.delete_file_index``, which is the same code the
scan planner uses -- so a delete file this module calls dangling is, by
construction, one no scan would have applied.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
)
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation

from .committer import ConcurrentModification, _ReplaceFiles
from .units import human_bytes

logger = logging.getLogger(__name__)

DELETE_CONTENT = (DataFileContent.POSITION_DELETES, DataFileContent.EQUALITY_DELETES)


class DanglingDeleteError(RuntimeError):
    """The removal could not be performed safely."""


class _RemoveDeleteFiles(_ReplaceFiles):
    """A replace producer that can also remove *delete* files.

    The stock ``_deleted_entries`` records only ``DataFileContent.DATA``, which
    would leave the removal untracked: the file vanishes from the manifest list
    but no snapshot says it was removed, so incremental readers never learn and
    the summary totals are wrong.
    """

    def _deleted_entries(self) -> list[ManifestEntry]:
        if self._parent_snapshot_id is None:
            return []
        previous = self._transaction.table_metadata.snapshot_by_id(self._parent_snapshot_id)
        if previous is None:  # pragma: no cover - cannot remove from an empty table
            raise ValueError(f"Could not find the previous snapshot: {self._parent_snapshot_id}")

        entries = []
        for manifest in previous.manifests(self._io):
            for entry in manifest.fetch_manifest_entry(self._io, discard_deleted=True):
                # The one change from upstream: no content filter. Data files
                # and delete files alike are recorded as removed.
                if entry.data_file in self._deleted_data_files:
                    entries.append(
                        ManifestEntry.from_args(
                            status=ManifestEntryStatus.DELETED,
                            snapshot_id=entry.snapshot_id,
                            sequence_number=entry.sequence_number,
                            file_sequence_number=entry.file_sequence_number,
                            data_file=entry.data_file,
                        )
                    )
        return entries

    def _existing_manifests(self):
        """Refuse the one path that would corrupt metadata.

        Upstream rewrites a partially-emptied manifest through
        ``write_manifest``, which stamps ``content: data``. Planning only ever
        hands us whole manifests, so this should be unreachable -- it is here
        because "should be unreachable" is exactly what a silent corruption
        looks like in hindsight.
        """
        snapshot = self._transaction.table_metadata.snapshot_by_name(name=self._target_branch)
        if snapshot is not None:
            for manifest in snapshot.manifests(io=self._io):
                if manifest.content == ManifestContent.DATA:
                    continue
                live = manifest.fetch_manifest_entry(io=self._io, discard_deleted=True)
                removed = [e for e in live if e.data_file in self._deleted_data_files]
                if removed and len(removed) != len(live):
                    raise DanglingDeleteError(
                        f"{manifest.manifest_path}: refusing to rewrite a partially-emptied "
                        "delete manifest -- PyIceberg would label it a data manifest."
                    )
        return super()._existing_manifests()


@dataclass(frozen=True)
class DeleteManifest:
    """A delete manifest, and how much of it is dangling."""

    path: str
    live: tuple[DataFile, ...]
    dangling: tuple[DataFile, ...]

    @property
    def fully_dangling(self) -> bool:
        return bool(self.dangling) and len(self.dangling) == len(self.live)

    @property
    def bytes_removable(self) -> int:
        return sum(f.file_size_in_bytes for f in self.dangling) if self.fully_dangling else 0


@dataclass
class DanglingReport:
    identifier: str
    delete_files: int = 0
    dangling_files: int = 0
    removable: list[DataFile] = field(default_factory=list)
    manifests_dropped: list[str] = field(default_factory=list)
    #: Dangling, but sharing a manifest with a delete file that still applies.
    stuck: list[DataFile] = field(default_factory=list)

    @property
    def removable_bytes(self) -> int:
        return sum(f.file_size_in_bytes for f in self.removable)

    def describe(self) -> str:
        if not self.delete_files:
            return f"{self.identifier}: no delete files"
        lines = [
            f"{self.identifier}: {self.delete_files} delete file(s), {self.dangling_files} dangling"
        ]
        if self.removable:
            lines.append(
                f"  {len(self.removable)} removable ({human_bytes(self.removable_bytes)}) "
                f"in {len(self.manifests_dropped)} whole manifest(s)"
            )
        if self.stuck:
            lines.append(
                f"  {len(self.stuck)} dangling file(s) retained: their manifest also holds "
                "a delete file that still applies, and PyIceberg cannot write a delete "
                "manifest to split it"
            )
        return "\n".join(lines)


def find_dangling(tbl: Table) -> DanglingReport:
    """Delete files the current snapshot references but no live data file needs.

    Applicability is delegated to PyIceberg's own ``DeleteFileIndex`` -- the
    index the scan planner builds -- so "dangling" here means precisely "no scan
    would apply it", rather than a restatement of the rule that could drift from
    it.
    """
    from pyiceberg.table.delete_file_index import DeleteFileIndex

    identifier = ".".join(tbl.name())
    report = DanglingReport(identifier=identifier)

    snapshot = tbl.current_snapshot()
    if snapshot is None:
        return report

    index = DeleteFileIndex()
    per_manifest: dict[str, list[DataFile]] = defaultdict(list)
    data_entries: list[ManifestEntry] = []

    for manifest in snapshot.manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            if entry.data_file.content in DELETE_CONTENT:
                index.add_delete_file(entry, entry.data_file.partition)
                per_manifest[manifest.manifest_path].append(entry.data_file)
                report.delete_files += 1
            elif entry.data_file.content == DataFileContent.DATA:
                data_entries.append(entry)

    if not report.delete_files:
        return report

    # Every delete file some live data file would pull in.
    applied: set[DataFile] = set()
    for entry in data_entries:
        applied |= index.for_data_file(
            entry.sequence_number or 0, entry.data_file, entry.data_file.partition
        )

    for path, delete_files in per_manifest.items():
        dangling = tuple(f for f in delete_files if f not in applied)
        report.dangling_files += len(dangling)
        # Not `manifest`: that name is the ManifestFile from the loop above, and
        # rebinding it to a different type here reads as the same thing.
        grouped = DeleteManifest(path=path, live=tuple(delete_files), dangling=dangling)
        if grouped.fully_dangling:
            report.removable.extend(dangling)
            report.manifests_dropped.append(path)
        else:
            report.stuck.extend(dangling)

    return report


@dataclass
class DeleteCleanupResult:
    report: DanglingReport
    removed: int = 0
    removed_bytes: int = 0
    snapshot_id: int | None = None
    dry_run: bool = False

    def describe(self) -> str:
        verb = "would remove" if self.dry_run else "removed"
        lines = [self.report.describe()]
        if self.removed:
            lines.append(
                f"  {verb} {self.removed} delete file(s) ({human_bytes(self.removed_bytes)})"
            )
        return "\n".join(lines)


class DanglingDeleteCleaner:
    """Drops delete files that apply to nothing.

    Metadata only: the delete files stay on disk, still referenced by older
    snapshots, exactly as compaction's superseded data files do. Snapshot expiry
    and orphan removal are what free the bytes.
    """

    def __init__(self, *, dry_run: bool = False) -> None:
        self._dry_run = dry_run

    def run(self, tbl: Table) -> DeleteCleanupResult:
        report = find_dangling(tbl)
        result = DeleteCleanupResult(report=report, dry_run=self._dry_run)
        if not report.removable:
            return result

        result.removed = len(report.removable)
        result.removed_bytes = report.removable_bytes
        if self._dry_run:
            return result

        snapshot = tbl.current_snapshot()
        expected = snapshot.snapshot_id if snapshot else None

        tbl.refresh()
        current = tbl.current_snapshot()
        if (current.snapshot_id if current else None) != expected:
            raise ConcurrentModification(
                "table snapshot changed between planning and removing dangling deletes"
            )

        properties = {
            "zamboni.operation": "remove-dangling-deletes",
            "zamboni.removed-delete-files": str(len(report.removable)),
        }
        with tbl.transaction() as txn:
            producer = _RemoveDeleteFiles(
                operation=Operation.REPLACE,
                transaction=txn,
                io=tbl.io,
                branch="main",
                snapshot_properties=properties,
            )
            with producer as update:
                for data_file in report.removable:
                    update.delete_data_file(data_file)

        tbl.refresh()
        new_snapshot = tbl.current_snapshot()
        result.snapshot_id = new_snapshot.snapshot_id if new_snapshot else None

        # The point of the operation, asserted rather than assumed: none of the
        # removed files may still be referenced by the new snapshot.
        still_there = {f.file_path for f in find_dangling(tbl).removable}
        removed_paths = {f.file_path for f in report.removable}
        if still_there & removed_paths:
            raise DanglingDeleteError(
                f"{report.identifier}: {len(still_there & removed_paths)} delete file(s) are "
                "still referenced after the commit"
            )
        return result
