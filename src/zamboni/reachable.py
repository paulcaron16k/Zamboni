# SPDX-License-Identifier: Apache-2.0
"""Every file a table currently references.

This is the most safety-critical module in the package. Snapshot expiry and
orphan removal both decide what to **delete** by subtracting this set from
something else, so a category omitted here is live data destroyed.

Six categories, because Iceberg metadata points at files from six places:

============================  ==========================================
data and delete files         ``inspect.all_files()``
manifests                     ``inspect.all_manifests()``
manifest lists                ``snapshot.manifest_list``, per snapshot
table metadata JSON           ``metadata_location`` + ``metadata.metadata_log``
statistics (puffin)           ``metadata.statistics``
partition statistics          ``metadata.partition_statistics``
============================  ==========================================

The last one is easy to miss -- it is a separate metadata field from
``statistics``, and an early draft of this module did miss it.

``all_files()`` walks manifests rather than planning a scan, so unlike the
compaction path it does not raise on equality deletes. Both reclaim operations
therefore work on tables that compaction currently refuses.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table


class Category(StrEnum):
    """Why a file is referenced. Carried through so reports can say what a
    file *is*, not merely that it is unreferenced."""

    DATA = "data and delete files"
    MANIFEST = "manifests"
    MANIFEST_LIST = "manifest lists"
    METADATA = "table metadata"
    STATISTICS = "statistics"
    PARTITION_STATISTICS = "partition statistics"


def canonical(path: str) -> str:
    """A comparison key that matches what a filesystem listing returns.

    Metadata records locations with a scheme (``file:///x``, ``s3://bucket/k``)
    while ``pyarrow.fs`` lists them without one (``/x``, ``bucket/k``).
    ``parse_location``'s third element is exactly the listing form for both, and
    it collapses scheme aliases such as ``s3`` and ``s3a`` onto one key.
    """
    return PyArrowFileIO.parse_location(path)[2]


@dataclass(frozen=True)
class ReachableSet:
    """Files referenced by a table, grouped by why."""

    by_category: dict[Category, frozenset[str]]

    @property
    def paths(self) -> frozenset[str]:
        return frozenset().union(*self.by_category.values()) if self.by_category else frozenset()

    def __len__(self) -> int:
        return len(self.paths)

    def __contains__(self, path: str) -> bool:
        return canonical(path) in self.paths

    def category_of(self, path: str) -> Category | None:
        key = canonical(path)
        for category, paths in self.by_category.items():
            if key in paths:
                return category
        return None

    def describe(self) -> str:
        lines = [
            f"  {category.value:<24}{len(paths):>7}"
            for category, paths in sorted(self.by_category.items(), key=lambda kv: kv[0].value)
        ]
        lines.append(f"  {'total':<24}{len(self):>7}")
        return "\n".join(lines)


def reachable_files(tbl: Table) -> ReachableSet:
    """Every file ``tbl`` references, across every live snapshot.

    "Live snapshot" means present in table metadata. Snapshots already expired
    are gone from metadata, so files only they referenced are absent here --
    which is precisely what makes them reclaimable.
    """
    data: set[str] = set()
    manifests: set[str] = set()
    manifest_lists: set[str] = set()

    if tbl.metadata.snapshots:
        # all_files / all_manifests read manifests directly and would raise on
        # a table with no snapshots at all.
        data = {canonical(p) for p in tbl.inspect.all_files()["file_path"].to_pylist()}
        manifests = {canonical(p) for p in tbl.inspect.all_manifests()["path"].to_pylist()}
        manifest_lists = {canonical(s.manifest_list) for s in tbl.metadata.snapshots}

    metadata = {canonical(tbl.metadata_location)}
    metadata |= {canonical(entry.metadata_file) for entry in tbl.metadata.metadata_log}

    statistics = {canonical(s.statistics_path) for s in (tbl.metadata.statistics or [])}
    partition_statistics = {
        canonical(s.statistics_path) for s in (tbl.metadata.partition_statistics or [])
    }

    return ReachableSet(
        by_category={
            Category.DATA: frozenset(data),
            Category.MANIFEST: frozenset(manifests),
            Category.MANIFEST_LIST: frozenset(manifest_lists),
            Category.METADATA: frozenset(metadata),
            Category.STATISTICS: frozenset(statistics),
            Category.PARTITION_STATISTICS: frozenset(partition_statistics),
        }
    )
