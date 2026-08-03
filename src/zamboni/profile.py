"""Read-only inspection of a table's physical layout and compaction eligibility.

Everything here reads manifests directly rather than going through
``Table.inspect``. That is deliberate: the committer needs the *actual*
:class:`~pyiceberg.manifest.DataFile` objects, because
``_OverwriteFiles._existing_manifests`` matches files with ``entry.data_file in
self._deleted_data_files``. Reconstructing equivalent objects from an inspect
table risks a mismatch that silently leaves stale entries live.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table import Table
from pyiceberg.typedef import Record

from .capabilities import detect


class Severity:
    BLOCKER = "blocker"
    WARNING = "warning"


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    message: str

    def __str__(self) -> str:
        return f"[{self.severity}] {self.code}: {self.message}"


@dataclass(frozen=True)
class LiveFile:
    """A live data file plus the manifest context needed to rewrite it."""

    data_file: DataFile
    sequence_number: int | None
    spec_id: int
    partition: Record

    @property
    def path(self) -> str:
        return self.data_file.file_path

    @property
    def size_bytes(self) -> int:
        return self.data_file.file_size_in_bytes

    @property
    def record_count(self) -> int:
        return self.data_file.record_count


@dataclass
class TableProfile:
    identifier: str
    format_version: int
    snapshot_id: int | None
    live_files: list[LiveFile]
    position_delete_files: int
    equality_delete_files: int
    spec_ids: set[int]
    default_spec_id: int
    sort_order_id: int | None
    findings: list[Finding] = field(default_factory=list)

    @property
    def blockers(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == Severity.BLOCKER]

    @property
    def warnings(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == Severity.WARNING]

    @property
    def total_bytes(self) -> int:
        return sum(f.size_bytes for f in self.live_files)

    @property
    def total_records(self) -> int:
        return sum(f.record_count for f in self.live_files)

    def size_histogram(
        self, buckets: Iterable[int] = (1 << 20, 8 << 20, 32 << 20, 128 << 20)
    ) -> dict[str, int]:
        """Count live files by size band. The small-file tail is the thing you
        are usually trying to see."""
        edges = sorted(buckets)
        labels = [f"<{_human(e)}" for e in edges] + [f">={_human(edges[-1])}"]
        counts = dict.fromkeys(labels, 0)
        for f in self.live_files:
            for edge, label in zip(edges, labels, strict=False):
                if f.size_bytes < edge:
                    counts[label] += 1
                    break
            else:
                counts[labels[-1]] += 1
        return counts

    def summary(self) -> str:
        lines = [
            (
                f"{self.identifier}  (format-version {self.format_version}, "
                f"snapshot {self.snapshot_id})"
            ),
            (
                f"  live data files : {len(self.live_files)}  "
                f"({_human(self.total_bytes)}, {self.total_records} rows)"
            ),
            f"  size histogram  : {self.size_histogram()}",
            f"  partition specs : {sorted(self.spec_ids)} (default {self.default_spec_id})",
            f"  sort order id   : {self.sort_order_id}",
            (
                f"  delete files    : {self.position_delete_files} position, "
                f"{self.equality_delete_files} equality"
            ),
        ]
        lines += [f"  {f}" for f in self.findings]
        return "\n".join(lines)


def profile_table(tbl: Table) -> TableProfile:
    """Walk the current snapshot's manifests and assess compaction eligibility."""
    snapshot = tbl.current_snapshot()
    live: list[LiveFile] = []
    position_deletes = 0
    equality_deletes = 0
    spec_ids: set[int] = set()

    if snapshot is not None:
        for manifest in snapshot.manifests(io=tbl.io):
            for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
                data_file = entry.data_file
                if data_file.content == DataFileContent.DATA:
                    spec_ids.add(data_file.spec_id)
                    live.append(
                        LiveFile(
                            data_file=data_file,
                            sequence_number=entry.sequence_number,
                            spec_id=data_file.spec_id,
                            partition=data_file.partition,
                        )
                    )
                elif data_file.content == DataFileContent.POSITION_DELETES:
                    position_deletes += 1
                elif data_file.content == DataFileContent.EQUALITY_DELETES:
                    equality_deletes += 1

    profile = TableProfile(
        identifier=".".join(tbl.name()),
        format_version=tbl.format_version,
        snapshot_id=snapshot.snapshot_id if snapshot else None,
        live_files=live,
        position_delete_files=position_deletes,
        equality_delete_files=equality_deletes,
        spec_ids=spec_ids,
        default_spec_id=tbl.metadata.default_spec_id,
        sort_order_id=tbl.metadata.default_sort_order_id,
    )
    profile.findings = _assess(tbl, profile)
    return profile


def _assess(tbl: Table, profile: TableProfile) -> list[Finding]:
    findings: list[Finding] = []

    if profile.format_version < 2:
        findings.append(
            Finding(
                Severity.BLOCKER,
                "format-version-1",
                "Format version 1 is not supported. V1 has no sequence numbers or "
                "row-level deletes, and DuckDB refuses to write V1 at all. Upgrade "
                "the table to V2 first.",
            )
        )

    if profile.format_version >= 3:
        findings.append(
            Finding(
                Severity.BLOCKER,
                "format-version-3",
                "Format version 3 tables carry row lineage: a rewritten row must keep "
                "its original _row_id and _last_updated_sequence_number. Compaction "
                "reads rows through the scan and writes them with PyIceberg's writer, "
                "neither of which carries those fields, so it would silently reassign "
                "them and break lineage for every downstream consumer. "
                "PyIceberg 0.11.1 also cannot serialise V3 metadata at all "
                "(TableMetadataV3.model_dump_json raises NotImplementedError), which "
                "makes this fail loudly on a SQL catalog -- but a REST catalog builds "
                "the metadata server-side, so nothing upstream would stop it there. "
                "Metadata-only maintenance (expiry, orphan removal, manifest rewriting, "
                "dangling-delete removal) does not rewrite rows and is unaffected.",
            )
        )

    if profile.equality_delete_files and not detect().equality_deletes_readable:
        findings.append(
            Finding(
                Severity.BLOCKER,
                "equality-deletes",
                f"{profile.equality_delete_files} equality delete file(s) present, and "
                "this PyIceberg build raises on them during scan planning "
                "(DataScan._plan_files_local). The rows they remove cannot be "
                "materialised, so compaction would resurrect deleted rows.",
            )
        )

    if len(profile.spec_ids) > 1:
        findings.append(
            Finding(
                Severity.WARNING,
                "partition-spec-evolution",
                f"Live data files span partition specs {sorted(profile.spec_ids)}. "
                "Each spec is planned and committed separately; files are never "
                "mixed across specs within one output file.",
            )
        )

    non_linear = _non_order_preserving_fields(tbl.spec())
    if non_linear:
        findings.append(
            Finding(
                Severity.WARNING,
                "non-linear-partition-transform",
                f"Partition field(s) {non_linear} use a non-order-preserving "
                "transform. Rewrites carry the source group's partition tuple "
                "through explicitly rather than inferring it from column "
                "statistics, so this is handled -- but PyIceberg's add_files "
                "path could not do it.",
            )
        )

    if profile.position_delete_files:
        findings.append(
            Finding(
                Severity.WARNING,
                "dangling-delete-files",
                f"{profile.position_delete_files} position delete file(s) will be "
                "left referenced after compaction. Rewritten rows already have the "
                "deletes applied and the new files carry a higher sequence number, "
                "so this is correct but adds metadata. Clear it with "
                "`zamboni remove-dangling-deletes`, which the maintenance run "
                "does immediately after compacting.",
            )
        )

    if profile.snapshot_id is None:
        findings.append(
            Finding(Severity.WARNING, "empty-table", "Table has no snapshot; nothing to compact.")
        )

    return findings


def _non_order_preserving_fields(spec: PartitionSpec) -> list[str]:
    return [f.name for f in spec.fields if not f.transform.preserves_order]


def _human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n}B"  # pragma: no cover
