"""Regroup manifest entries so partition predicates can prune them.

A query prunes in two stages: first manifests, using the partition bounds each
manifest records, then data files. The first stage only works if a manifest's
entries share a partition. Streaming ingest guarantees they do not -- every
commit writes one manifest holding whatever that batch touched -- so after a few
hundred appends every manifest spans every partition and the planner reads all
of them to find anything.

Compaction does not fix this. It rewrites *data* files; the manifest set it
leaves behind still has one manifest per commit. The demo's merge-on-read run
reached 57 manifests for 35 data files.

PyIceberg has no ``rewrite_manifests``. Its ``_ManifestMergeManager`` merges by
size at append time and groups only by spec, never by partition, so it makes
manifests bigger without making them prunable.

What this does, per spec: read every live entry, group by partition, bin-pack
those groups to a target size, and write the result as a ``replace`` snapshot
that adds and removes no files at all.

Two things are preserved exactly, because losing either changes what a reader
sees rather than merely how fast it gets there:

- **Sequence numbers** (``sequence_number``, ``file_sequence_number``). A delete
  file applies to a data file with a lower sequence number. Rewriting an entry
  with a fresh sequence number would silently change which deletes apply.
- **``snapshot_id``**, so an incremental consumer can still tell which snapshot
  first added a file.

Delete manifests are left untouched: ``ManifestWriterV2`` cannot write one (see
:mod:`zamboni.deletes`). That costs little here -- dangling-delete removal is
what shrinks the delete-manifest count, and it runs first.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

from pyiceberg.manifest import (
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    write_manifest,
)
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation
from pyiceberg.typedef import Record

from .committer import ConcurrentModification, _ReplaceFiles

logger = logging.getLogger(__name__)

#: Iceberg's own default for ``write.manifest.target-size-bytes``.
DEFAULT_TARGET_MANIFEST_BYTES = 8 * 1024 * 1024
PROP_TARGET_MANIFEST_SIZE = "write.manifest.target-size-bytes"

#: Below this there is nothing to regroup.
DEFAULT_MIN_INPUT_MANIFESTS = 2

#: Used only when the source manifests report no length to estimate from.
_FALLBACK_ENTRY_BYTES = 512


class ManifestRewriteError(RuntimeError):
    """The rewrite could not be performed safely."""


@dataclass(frozen=True)
class ManifestBin:
    """One output manifest: the entries it will hold, under one spec."""

    spec_id: int
    entries: tuple[ManifestEntry, ...]

    @property
    def partitions(self) -> set:
        return {_partition_key(e) for e in self.entries}


def _partition_key(entry: ManifestEntry) -> tuple:
    partition = entry.data_file.partition
    if partition is None:
        return ()
    return tuple(partition)


@dataclass
class RewritePlan:
    identifier: str
    #: Data manifests that will be replaced.
    replaced: list[ManifestFile] = field(default_factory=list)
    #: Manifests carried over untouched -- delete manifests, and data manifests
    #: already holding a single partition at a reasonable size.
    kept: list[ManifestFile] = field(default_factory=list)
    bins: list[ManifestBin] = field(default_factory=list)
    entries: int = 0
    #: Distinct partitions per input data manifest, summed. A manifest spanning
    #: many partitions has bounds spanning them all and prunes nothing, so this
    #: -- not the manifest count -- is what the operation exists to reduce.
    spread_before: int = 0
    skipped_reason: str | None = None

    @property
    def spread_after(self) -> int:
        return sum(len(m.partitions) for m in self.bins)

    @property
    def worth_doing(self) -> bool:
        """Fewer manifests, or better-clustered ones.

        Counting manifests alone is the wrong test: four manifests each spanning
        four partitions become four manifests each holding one, which is the
        whole point of the operation and is not a reduction in count. Judging on
        count alone silently skipped exactly the case worth doing.
        """
        if not self.bins:
            return False
        return len(self.bins) < len(self.replaced) or self.spread_after < self.spread_before

    def describe(self) -> str:
        if self.skipped_reason:
            return f"{self.identifier}: skipped -- {self.skipped_reason}"
        lines = [
            (
                f"{self.identifier}: {len(self.replaced)} data manifest(s) holding "
                f"{self.entries} entries -> {len(self.bins)}"
            )
        ]
        if self.kept:
            lines.append(f"  {len(self.kept)} manifest(s) kept untouched")
        spanning = sum(1 for m in self.bins if len(m.partitions) > 1)
        lines.append(
            f"  {len(self.bins) - spanning} of {len(self.bins)} output manifest(s) "
            f"hold a single partition"
        )
        lines.append(f"  partition spread {self.spread_before} -> {self.spread_after}")
        return "\n".join(lines)


def _in_partition_order(groups: list[tuple[tuple, list[ManifestEntry]]]):
    """Sort partition groups by value, so packed manifests get tight bounds.

    Sorting by ``str`` would order integer partitions as 1, 10, 2 -- contiguous
    by name, scattered by value, which is the opposite of what the bounds need.
    Partition tuples are not always mutually comparable (mixed types, nulls), so
    fall back to the string form rather than failing.
    """
    try:
        return sorted(groups, key=lambda kv: kv[0])
    except TypeError:
        return sorted(groups, key=lambda kv: str(kv[0]))


def _target_bytes(tbl: Table) -> int:
    raw = tbl.properties.get(PROP_TARGET_MANIFEST_SIZE)
    if raw is None:
        return DEFAULT_TARGET_MANIFEST_BYTES
    try:
        return int(raw)
    except ValueError:
        raise ManifestRewriteError(
            f"table property {PROP_TARGET_MANIFEST_SIZE!r} is not an integer: {raw!r}"
        ) from None


def plan_rewrite(
    tbl: Table, *, min_input_manifests: int = DEFAULT_MIN_INPUT_MANIFESTS
) -> RewritePlan:
    """Which manifests to regroup, and into what."""
    identifier = ".".join(tbl.name())
    plan = RewritePlan(identifier=identifier)

    snapshot = tbl.current_snapshot()
    if snapshot is None:
        plan.skipped_reason = "table has no snapshot"
        return plan

    data_manifests, delete_manifests = [], []
    for manifest in snapshot.manifests(io=tbl.io):
        (data_manifests if manifest.content == ManifestContent.DATA else delete_manifests).append(
            manifest
        )
    plan.kept.extend(delete_manifests)

    if len(data_manifests) < min_input_manifests:
        plan.skipped_reason = (
            f"{len(data_manifests)} data manifest(s) < min_input_manifests={min_input_manifests}"
        )
        return plan

    # Group live entries by (spec, partition). Sequence numbers ride along on
    # the entry, untouched.
    by_group: dict[tuple[int, tuple], list[ManifestEntry]] = defaultdict(list)
    total_bytes = 0
    for manifest in data_manifests:
        total_bytes += manifest.manifest_length or 0
        partitions_here: set[tuple] = set()
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            key = _partition_key(entry)
            by_group[(manifest.partition_spec_id, key)].append(entry)
            partitions_here.add(key)
            plan.entries += 1
        plan.spread_before += len(partitions_here)

    if not plan.entries:
        plan.skipped_reason = "no live entries"
        return plan

    plan.replaced.extend(data_manifests)
    per_entry = max(1, total_bytes // plan.entries) if total_bytes else _FALLBACK_ENTRY_BYTES
    target = _target_bytes(tbl)

    # Pack whole partition groups in partition order. Contiguity is the point:
    # a manifest records lower/upper partition bounds, so a manifest holding a
    # contiguous *range* of partitions still prunes, while one holding a random
    # scattering of them has bounds spanning the table and prunes nothing.
    # Splitting a single partition across manifests is what must be avoided.
    by_spec: dict[int, list[tuple[tuple, list[ManifestEntry]]]] = defaultdict(list)
    for (spec_id, partition), entries in by_group.items():
        by_spec[spec_id].append((partition, entries))

    for spec_id in sorted(by_spec):
        groups = _in_partition_order(by_spec[spec_id])
        current: list[ManifestEntry] = []
        for _partition, entries in groups:
            if current and (len(current) + len(entries)) * per_entry > target:
                plan.bins.append(ManifestBin(spec_id, tuple(current)))
                current = []
            current.extend(entries)
        if current:
            plan.bins.append(ManifestBin(spec_id, tuple(current)))

    return plan


class _RewriteManifests(_ReplaceFiles):
    """A replace producer whose only change is the manifest grouping.

    No data file is added or removed, so ``_manifests`` is overridden wholesale:
    the stock implementation would compute added and deleted manifests from
    empty sets and then hand back the *original* existing manifests, which is
    the one thing this operation must not do.
    """

    def __init__(self, *, bins: list[ManifestBin], kept: list[ManifestFile], **kwargs) -> None:
        super().__init__(**kwargs)
        self._bins = bins
        self._kept = kept

    def _deleted_entries(self) -> list[ManifestEntry]:
        return []

    def _manifests(self) -> list[ManifestFile]:
        written: list[ManifestFile] = []
        for manifest_bin in self._bins:
            with write_manifest(
                format_version=self._transaction.table_metadata.format_version,
                spec=self.spec(manifest_bin.spec_id),
                schema=self._transaction.table_metadata.schema(),
                output_file=self.new_manifest_output(),
                snapshot_id=self._snapshot_id,
                avro_compression=self._compression,
            ) as writer:
                for entry in manifest_bin.entries:
                    writer.add_entry(
                        ManifestEntry.from_args(
                            status=ManifestEntryStatus.EXISTING,
                            # Preserved, not reassigned. See the module docstring:
                            # a fresh sequence number changes which deletes apply.
                            snapshot_id=entry.snapshot_id,
                            sequence_number=entry.sequence_number,
                            file_sequence_number=entry.file_sequence_number,
                            data_file=entry.data_file,
                        )
                    )
            written.append(writer.to_manifest_file())
        return written + self._kept


@dataclass
class RewriteResult:
    plan: RewritePlan
    manifests_before: int = 0
    manifests_after: int = 0
    snapshot_id: int | None = None
    dry_run: bool = False

    def describe(self) -> str:
        lines = [self.plan.describe()]
        if self.manifests_before:
            verb = "would be" if self.dry_run else ""
            lines.append(
                f"  manifests {self.manifests_before} -> {self.manifests_after} {verb}".rstrip()
            )
        return "\n".join(lines)


class ManifestRewriter:
    """Regroups manifests by partition. Metadata only; no data file moves."""

    def __init__(
        self,
        *,
        min_input_manifests: int = DEFAULT_MIN_INPUT_MANIFESTS,
        dry_run: bool = False,
    ) -> None:
        self._min_input_manifests = min_input_manifests
        self._dry_run = dry_run

    def run(self, tbl: Table) -> RewriteResult:
        plan = plan_rewrite(tbl, min_input_manifests=self._min_input_manifests)
        result = RewriteResult(plan=plan, dry_run=self._dry_run)

        if not plan.worth_doing:
            if not plan.skipped_reason:
                plan.skipped_reason = (
                    f"{len(plan.bins)} output manifest(s) from {len(plan.replaced)} input, "
                    f"partition spread {plan.spread_before} -> {plan.spread_after} -- "
                    "neither improves"
                )
            return result

        snapshot = tbl.current_snapshot()
        result.manifests_before = len(snapshot.manifests(io=tbl.io))
        result.manifests_after = len(plan.bins) + len(plan.kept)
        if self._dry_run:
            return result

        expected = snapshot.snapshot_id
        tbl.refresh()
        current = tbl.current_snapshot()
        if (current.snapshot_id if current else None) != expected:
            raise ConcurrentModification(
                "table snapshot changed between planning and rewriting manifests"
            )

        properties = {
            "zamboni.operation": "rewrite-manifests",
            "zamboni.manifests-replaced": str(len(plan.replaced)),
            "zamboni.manifests-created": str(len(plan.bins)),
        }
        with tbl.transaction() as txn:
            producer = _RewriteManifests(
                bins=plan.bins,
                kept=plan.kept,
                operation=Operation.REPLACE,
                transaction=txn,
                io=tbl.io,
                branch="main",
                snapshot_properties=properties,
            )
            with producer:
                pass  # every change is in the manifest grouping itself

        tbl.refresh()
        new_snapshot = tbl.current_snapshot()
        result.snapshot_id = new_snapshot.snapshot_id if new_snapshot else None
        result.manifests_after = len(new_snapshot.manifests(io=tbl.io)) if new_snapshot else 0
        return result


def manifest_partition_spread(tbl: Table) -> dict[str, int]:
    """How many partitions each data manifest touches.

    The metric the operation exists to move: a manifest spanning many partitions
    cannot be pruned by a partition predicate, so the planner opens it every
    time.
    """
    snapshot = tbl.current_snapshot()
    if snapshot is None:
        return {}
    spread = {}
    for manifest in snapshot.manifests(io=tbl.io):
        if manifest.content != ManifestContent.DATA:
            continue
        partitions: set[Record | tuple] = set()
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            partitions.add(_partition_key(entry))
        spread[manifest.manifest_path] = len(partitions)
    return spread
