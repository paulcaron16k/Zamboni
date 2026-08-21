# SPDX-License-Identifier: Apache-2.0
"""Group live data files into independently committable rewrite units.

The grouping rules mirror duckdb-iceberg's native planner
(``src/maintenance/rewrite_data_files_planner.cpp``): skip files that already
meet the target size, bucket the rest by partition, and drop partitions with too
few candidates. Keeping the rules aligned means a plan produced here can be
compared directly against ``iceberg_rewrite_data_files`` output if you ever build
that extension as an oracle.

One deliberate divergence: the native planner *refuses* a table whose files span
multiple partition specs. We split by spec instead, so each group holds a single
``(spec_id, partition tuple)`` and can be rewritten without mixing specs.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyiceberg.table import Table
from pyiceberg.typedef import Record

from .config import CompactionConfig, resolve_target_file_size
from .profile import LiveFile, TableProfile


@dataclass(frozen=True)
class FileGroup:
    """Files sharing one ``(spec_id, partition)`` that will become new files."""

    spec_id: int
    partition: Record
    files: list[LiveFile]
    target_file_size_bytes: int

    @property
    def input_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)

    @property
    def input_records(self) -> int:
        return sum(f.record_count for f in self.files)

    @property
    def paths(self) -> list[str]:
        return [f.path for f in self.files]

    def describe(self) -> str:
        part = _partition_label(self.partition)
        return (
            f"spec={self.spec_id} partition={part} "
            f"files={len(self.files)} bytes={self.input_bytes} rows={self.input_records}"
        )


@dataclass
class CompactionPlan:
    identifier: str
    snapshot_id: int | None
    target_file_size_bytes: int
    groups: list[FileGroup] = field(default_factory=list)
    #: Partitions examined but not selected, with the reason.
    skipped: list[tuple[str, str]] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.groups

    @property
    def total_input_files(self) -> int:
        return sum(len(g.files) for g in self.groups)

    @property
    def total_input_bytes(self) -> int:
        return sum(g.input_bytes for g in self.groups)

    def describe(self) -> str:
        lines = [
            (
                f"{self.identifier}: {len(self.groups)} group(s), "
                f"{self.total_input_files} file(s), target "
                f"{self.target_file_size_bytes} bytes, snapshot {self.snapshot_id}"
            )
        ]
        lines += [f"  group {i}: {g.describe()}" for i, g in enumerate(self.groups)]
        # Never let a bounded plan look like full coverage.
        lines += [f"  skipped {part}: {reason}" for part, reason in self.skipped]
        return "\n".join(lines)


class CompactionPlanner:
    def __init__(self, config: CompactionConfig) -> None:
        self._config = config

    def plan(self, tbl: Table, profile: TableProfile) -> CompactionPlan:
        target = resolve_target_file_size(self._config, dict(tbl.properties))
        plan = CompactionPlan(
            identifier=profile.identifier,
            snapshot_id=profile.snapshot_id,
            target_file_size_bytes=target,
        )
        if profile.snapshot_id is None:
            return plan

        buckets: dict[tuple[int, Record], list[LiveFile]] = {}
        for live in profile.live_files:
            if not self._config.rewrite_all and live.size_bytes >= target:
                continue
            buckets.setdefault((live.spec_id, live.partition), []).append(live)

        for (spec_id, partition), files in sorted(buckets.items(), key=lambda kv: str(kv[0])):
            label = f"spec={spec_id} partition={_partition_label(partition)}"
            if not self._config.rewrite_all and len(files) < self._config.min_input_files:
                reason = (
                    f"{len(files)} candidate file(s) < "
                    f"min_input_files={self._config.min_input_files}"
                )
                plan.skipped.append((label, reason))
                continue
            if len(files) == 1 and not self._config.rewrite_all:
                # Rewriting one file into one file changes nothing but the path.
                plan.skipped.append((label, "single candidate file"))
                continue
            plan.groups.append(
                FileGroup(
                    spec_id=spec_id,
                    partition=partition,
                    files=sorted(files, key=lambda f: f.path),
                    target_file_size_bytes=target,
                )
            )

        return plan


def _partition_label(partition: Record) -> str:
    try:
        # Record is iterable at runtime -- PyIceberg's own code does this --
        # but its type does not declare __iter__, hence the narrow ignore.
        values = list(partition)  # type: ignore[call-overload]
    except TypeError:  # pragma: no cover - defensive
        return repr(partition)
    return "()" if not values else "(" + ", ".join(repr(v) for v in values) + ")"
