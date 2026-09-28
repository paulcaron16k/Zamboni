# SPDX-License-Identifier: Apache-2.0
"""Which partitions have changed since maintenance last ran.

Compaction's candidate set is every partition holding files below the target.
On a large table that is mostly partitions nothing has written to since the last
run, and rewriting them changes nothing but the paths. This derives the set that
*did* change, from metadata alone, so the planner can leave the rest.

**Two paths, and which one a table gets is the table's choice.** Iceberg's
`write.summary.partition-limit` puts the changed partition *paths* directly into
each snapshot summary, up to a bound. Measured against PyIceberg 0.12.0 on
2026-09-27: the property works and writes `partitions.<path>` keys beside
`partition-summaries-included: true` -- but **its default is 0**, so a table
that has not set it carries `changed-partition-count` and nothing else. The
cheap path is therefore real and opt-in, not something to assume.

    summary    every snapshot since the watermark carries its partition paths.
               No manifest is opened.
    manifests  read the manifests those snapshots added. Still metadata, never
               a data file, and only the manifests of the snapshots that
               landed since the last run -- not the whole table, which is what
               `profile_table` does at ~400 ms.

**Degrades honestly.** Where the set cannot be derived -- an unpartitioned
table, a table never maintained, a watermark that has aged out -- the answer is
:data:`UNKNOWN`, meaning *consider every candidate*, which is what compaction
did before this existed. Guessing a subset would silently stop compacting
partitions that needed it, and not compacting is itself a harm (the same
reasoning as `_still_open`'s scope in `planner.py`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyiceberg.manifest import DataFileContent

from .health import maintenance_watermark

if TYPE_CHECKING:  # pragma: no cover
    from pyiceberg.table import Table
    from pyiceberg.typedef import Record

logger = logging.getLogger(__name__)

#: Snapshot-summary keys PyIceberg writes when `write.summary.partition-limit`
#: is set. `PARTITION_PREFIX` carries the partition path in the key itself,
#: which is the whole reason this path is cheap -- the value is per-partition
#: counters we do not need.
PARTITION_PREFIX = "partitions."
PARTITION_SUMMARIES_INCLUDED = "partition-summaries-included"
CHANGED_PARTITION_COUNT = "changed-partition-count"

#: The table property that turns the cheap path on. Default 0 in PyIceberg,
#: i.e. off.
PARTITION_LIMIT_PROPERTY = "write.summary.partition-limit"


@dataclass(frozen=True)
class ChangedPartitions:
    """Partition paths written to since the watermark, or "I do not know".

    ``paths is None`` is not "nothing changed" -- it is **unknown**, and the two
    must never be confused: an empty set says "compact nothing", `None` says
    "compact whatever you would have compacted anyway". Conflating them is the
    one way this feature can lose data's worth of work rather than save it.
    """

    paths: frozenset[str] | None = None
    #: ``summary``, ``manifests``, or why it could not be derived.
    source: str = "unavailable"
    #: How many snapshots were examined.
    snapshots: int = 0

    @property
    def known(self) -> bool:
        return self.paths is not None

    def covers(self, path: str) -> bool:
        """Should this partition be considered? Unknown means yes."""
        return self.paths is None or path in self.paths

    def describe(self) -> str:
        if self.paths is None:
            return f"changed partitions unknown ({self.source})"
        return (
            f"{len(self.paths)} partition(s) changed in {self.snapshots} snapshot(s) "
            f"(from {self.source})"
        )


#: "Consider every candidate", the answer whenever the set cannot be derived.
UNKNOWN = ChangedPartitions()


def _snapshots_since_watermark(tbl: Table) -> list | None:
    """The snapshots committed after maintenance last ran, or ``None``.

    ``None`` where the question has no answer: a table never maintained, or one
    whose stamp has been expired away. Both mean "everything is potentially
    stale", and the caller must widen rather than narrow.

    Ordered by **position** in `metadata.snapshots`, matching
    `maintenance_watermark`, because that list is append order while timestamps
    come from whichever client committed.
    """
    mark = maintenance_watermark(tbl)
    if not mark.maintained or mark.snapshot_id is None:
        return None

    snapshots = list(tbl.metadata.snapshots)
    for position, snapshot in enumerate(snapshots):
        if snapshot.snapshot_id == mark.snapshot_id:
            return snapshots[position + 1 :]
    return None  # pragma: no cover - the watermark came from this list


def _from_summaries(snapshots: list) -> frozenset[str] | None:
    """Partition paths straight out of the summaries, or ``None``.

    All or nothing across the snapshots: one that did not record its partitions
    means the union is incomplete, and an incomplete set is worse than no set
    because it reads as authoritative. `partition-summaries-included` is
    PyIceberg's own flag for "I recorded them", so this asks rather than infers.

    A snapshot that changed *no* partitions contributes nothing and does not
    disqualify the path -- `changed-partition-count` absent means there was
    nothing to record, not that recording was skipped.
    """
    paths: set[str] = set()
    for snapshot in snapshots:
        summary = snapshot.summary
        properties = dict(getattr(summary, "additional_properties", {}) or {})
        if properties.get(CHANGED_PARTITION_COUNT) is None:
            continue
        if properties.get(PARTITION_SUMMARIES_INCLUDED) != "true":
            return None
        paths |= {
            key[len(PARTITION_PREFIX) :] for key in properties if key.startswith(PARTITION_PREFIX)
        }
    return frozenset(paths)


def _from_manifests(tbl: Table, snapshots: list) -> frozenset[str] | None:
    """Partition paths from the manifests those snapshots added.

    Only the manifests a snapshot in the window *added*: `added_snapshot_id`
    names the snapshot a manifest was written by, so an append's new manifest is
    read and the table's existing ones are not. That is what keeps this closer
    to a few manifest reads than to `profile_table`'s whole-table walk.

    Data files are never opened -- a manifest entry carries the partition
    `Record`, which is all this needs.
    """
    wanted = {snapshot.snapshot_id for snapshot in snapshots}
    schema = tbl.schema()
    specs = tbl.specs()
    paths: set[str] = set()

    for snapshot in snapshots:
        for manifest in snapshot.manifests(io=tbl.io):
            if getattr(manifest, "added_snapshot_id", None) not in wanted:
                continue
            for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
                data_file = entry.data_file
                if data_file.content != DataFileContent.DATA:
                    continue
                spec = specs.get(data_file.spec_id)
                if spec is None:  # pragma: no cover - defensive
                    return None
                paths.add(partition_path(spec, data_file.partition, schema))
    return frozenset(paths)


def partition_path(spec, partition: Record, schema) -> str:
    """The path string Iceberg uses for a partition, e.g. ``category=a``.

    The join between the two worlds this module straddles: the planner groups by
    a partition `Record`, and the snapshot summary names partitions by path.
    Both sides go through `PartitionSpec.partition_to_path`, so they agree by
    construction rather than by a formatting rule written twice.
    """
    return spec.partition_to_path(partition, schema)


def changed_partitions(tbl: Table) -> ChangedPartitions:
    """Which partitions were written to since maintenance last ran.

    Never raises. Anything unexpected answers :data:`UNKNOWN`, because this
    decides whether to *skip* work: wrong in that direction means a partition
    goes uncompacted, which is the failure this tool exists to prevent.
    """
    if not tbl.spec().fields:
        # Unpartitioned: there is one bucket and targeting has nothing to say.
        return ChangedPartitions(source="table is not partitioned")

    try:
        snapshots = _snapshots_since_watermark(tbl)
    except Exception:  # pragma: no cover - any metadata trouble means "widen"
        logger.debug("could not read the watermark; considering every partition", exc_info=True)
        return UNKNOWN

    if snapshots is None:
        return ChangedPartitions(source="never maintained, or the watermark aged out")
    if not snapshots:
        return ChangedPartitions(paths=frozenset(), source="summary", snapshots=0)

    try:
        paths = _from_summaries(snapshots)
        source = "summary"
        if paths is None:
            paths = _from_manifests(tbl, snapshots)
            source = "manifests"
    except Exception:  # pragma: no cover - metadata we could not read
        logger.debug("could not derive the changed partitions", exc_info=True)
        return UNKNOWN

    if paths is None:  # pragma: no cover - defensive
        return UNKNOWN
    return ChangedPartitions(paths=paths, source=source, snapshots=len(snapshots))


__all__ = [
    "PARTITION_LIMIT_PROPERTY",
    "UNKNOWN",
    "ChangedPartitions",
    "changed_partitions",
    "partition_path",
]
