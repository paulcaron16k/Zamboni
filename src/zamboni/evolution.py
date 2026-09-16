# SPDX-License-Identifier: Apache-2.0
"""Partition evolution: condense aged fine-grained partitions into coarse ones.

The motivating case is a table partitioned by ``day`` on an event timestamp.
Recent days want day-level pruning; a month from last year is a hundred small
files that a query almost never prunes into. Rewriting those old days as one
month-partitioned file cuts file count and manifest size without touching the
hot end of the table.

Why this cannot be done by rewriting alone
------------------------------------------

Under a ``day`` spec every data file has exactly one day partition value, so
thirty days cannot become one file while that spec applies. The month files have
to be written under a *month* spec. Iceberg allows that: a table carries a set of
specs, each file records which one it was written under, and only new writes use
the default. PyIceberg exposes ``AddPartitionSpecUpdate`` separately from
``SetDefaultSpecUpdate``, so a month spec can be added **without** becoming the
default -- new data keeps landing daily.

The part that needs care
------------------------

``_SnapshotProducer._manifests`` writes the added manifest with
``table_metadata.spec()`` -- the table default -- hardcoded, while grouping
*deleted* entries by each file's own ``spec_id``. Committing month-partitioned
files through it unchanged produces a manifest that declares the day spec while
its entries carry month partition values: silently corrupt metadata that reads
fine until the first predicate tries to prune on it.

:class:`MultiSpecReplaceFiles` fixes the asymmetry by grouping added files by
``spec_id`` too, mirroring what upstream already does on the delete side.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass

from pyiceberg.partitioning import PARTITION_FIELD_ID_START, PartitionSpec
from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.table import Table
from pyiceberg.table.update import AddPartitionSpecUpdate, AssertTableUUID
from pyiceberg.transforms import DayTransform, HourTransform, MonthTransform, YearTransform

from .committer import _ReplaceFiles
from .profile import LiveFile
from .tableconfig import EvolutionRule, TableSettings
from .windows import EPOCH
from .windows import transform_name as _transform_name
from .windows import window_end as _window_end


def _label(tbl: Table) -> str:
    return ".".join(tbl.name())


TRANSFORM_FOR = {
    "hour": HourTransform,
    "day": DayTransform,
    "month": MonthTransform,
    "year": YearTransform,
}


class MultiSpecReplaceFiles(_ReplaceFiles):
    """A producer that writes one added manifest per partition spec.

    Now a name only: the behaviour it existed for lives in the library. The
    fork's `feature/maintenance` writes added files under their own `spec_id`
    in both `_write_added_manifest` and `_summary`, mirroring what the delete
    side already did, so there is nothing left to override.

    Kept as a distinct class rather than collapsed into `_ReplaceFiles` so the
    committer's choice of producer still reads as a decision, and so the
    grouping behaviour has somewhere to come back to if the dependency ever
    moves to a library without it.
    """


@dataclass(frozen=True)
class EvolutionGroup:
    """Files sharing one coarse partition value, to be merged under ``spec_id``."""

    rule: EvolutionRule
    target_spec_id: int
    #: No target partition value here on purpose. The rewrite writes under
    #: `target_spec_id` and PyIceberg derives every partition value from the
    #: data, which is more trustworthy than our arithmetic -- so a value carried
    #: alongside would be unused and free to drift.
    files: list[LiveFile]
    label: str

    @property
    def input_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)


@dataclass
class EvolutionPlan:
    groups: list[EvolutionGroup]
    skipped: list[tuple[str, str]]
    #: Specs that must be registered on the table before the groups can commit.
    required_specs: dict[int, PartitionSpec]

    @property
    def is_empty(self) -> bool:
        return not self.groups

    def describe(self) -> str:
        lines = [f"{len(self.groups)} evolution group(s)"]
        lines += [
            f"  {g.label}: {len(g.files)} file(s), {g.input_bytes} bytes -> spec {g.target_spec_id}"
            for g in self.groups
        ]
        lines += [f"  skipped {what}: {why}" for what, why in self.skipped]
        return "\n".join(lines)


def plan_evolution(
    tbl: Table,
    settings: TableSettings,
    live_files: list[LiveFile],
    *,
    today: dt.date | None = None,
) -> EvolutionPlan:
    """Find aged partitions eligible to be condensed into a coarser granularity."""
    today = today or dt.datetime.now(dt.UTC).date()
    plan = EvolutionPlan(groups=[], skipped=[], required_specs={})

    if not settings.partition_evolution.enabled:
        plan.skipped.append((_label(tbl), "partition_evolution.enabled is false"))
        return plan
    if not settings.partition_evolution.rules:
        plan.skipped.append((_label(tbl), "no evolution rules configured"))
        return plan

    current_spec = tbl.spec()
    # Ids are allocated across the whole plan, not per rule. Recomputing them
    # from unchanged table metadata each time makes two rules that both need a
    # new spec claim the same id, and the second silently overwrites the first
    # in `required_specs`.
    allocator = _IdAllocator(tbl)
    for rule in settings.partition_evolution.rules:
        source_fields = [
            f for f in current_spec.fields if _transform_name(f.transform) == rule.from_transform
        ]
        if not source_fields:
            plan.skipped.append(
                (rule.from_transform, f"table's spec has no {rule.from_transform!r} field")
            )
            continue
        if len(source_fields) > 1:
            # Which field dates the partition? `older_than_days` is measured from
            # a window end, and two fields of the same granularity give two
            # answers. Guessing would silently age data by the wrong column.
            names = ", ".join(f.name for f in source_fields)
            reason = (
                f"{len(source_fields)} fields share this granularity ({names}); "
                "which one dates the partition is ambiguous"
            )
            plan.skipped.append((rule.from_transform, reason))
            continue

        source_field = source_fields[0]
        position = list(current_spec.fields).index(source_field)
        target_spec, spec_id = _resolve_target_spec(
            tbl, current_spec, source_field, rule, allocator
        )
        plan.required_specs[spec_id] = target_spec

        cutoff = today - dt.timedelta(days=rule.older_than_days)
        # Keyed by the whole output partition tuple, not just the coarse time
        # value: files sharing a month but sitting in different buckets of a
        # compound spec belong to different output partitions and cannot merge.
        buckets: dict[tuple, list[LiveFile]] = defaultdict(list)
        for live in live_files:
            if live.spec_id != current_spec.spec_id:
                continue
            values = _partition_values(live, len(current_spec.fields))
            if values is None or values[position] is None:
                continue
            window_end = _window_end(rule.from_transform, int(values[position]))
            if window_end > cutoff:
                continue  # still inside the retention window
            coarse = list(values)
            coarse[position] = _coarse_value(rule.to_transform, window_end)
            buckets[tuple(coarse)].append(live)

        for key, files in sorted(buckets.items(), key=lambda kv: str(kv[0])):
            label = _group_label(current_spec, target_spec, position, key, rule)
            if len(files) < 2:
                plan.skipped.append((label, "fewer than 2 files to merge"))
                continue
            plan.groups.append(
                EvolutionGroup(
                    rule=rule,
                    target_spec_id=spec_id,
                    files=sorted(files, key=lambda f: f.path),
                    label=label,
                )
            )

    return plan


def ensure_specs(tbl: Table, specs: dict[int, PartitionSpec]) -> Table:
    """Register any missing partition specs without changing the default.

    Adding the spec is a separate table update from setting the default, which
    is what lets new data keep landing under the fine-grained spec while old
    data moves to the coarse one.
    """
    existing = tbl.metadata.specs()
    missing = [spec for spec_id, spec in specs.items() if spec_id not in existing]
    if not missing:
        return tbl

    txn = tbl.transaction()
    txn._apply(
        tuple(AddPartitionSpecUpdate(spec=spec) for spec in missing),
        (AssertTableUUID(uuid=tbl.metadata.table_uuid),),
    )
    txn.commit_transaction()
    tbl.refresh()
    return tbl


class _IdAllocator:
    """Hands out spec and partition-field ids that stay unique within a plan."""

    def __init__(self, tbl: Table) -> None:
        self._next_spec_id = max(tbl.metadata.specs()) + 1
        self._next_field_id = (tbl.metadata.last_partition_id or PARTITION_FIELD_ID_START - 1) + 1

    def take(self) -> tuple[int, int]:
        spec_id, field_id = self._next_spec_id, self._next_field_id
        self._next_spec_id += 1
        self._next_field_id += 1
        return spec_id, field_id


def _resolve_target_spec(
    tbl: Table,
    current_spec: PartitionSpec,
    source_field: IcebergPartitionField,
    rule: EvolutionRule,
    allocator: _IdAllocator,
) -> tuple[PartitionSpec, int]:
    """Find or design the coarse spec this rule targets.

    A compound spec needs exactly *one* new spec, not one per combination of the
    other fields -- the combinations are partition *values*, which every file
    carries individually under the same spec. The other fields are copied through
    untouched, so only the aged field's transform changes.
    """
    transform = TRANSFORM_FOR[rule.to_transform]()
    wanted = [
        (f.source_id, rule.to_transform if f is source_field else _transform_name(f.transform))
        for f in current_spec.fields
    ]
    for spec_id, spec in tbl.metadata.specs().items():
        have = [(f.source_id, _transform_name(f.transform)) for f in spec.fields]
        if have == wanted:
            return spec, spec_id

    # A *new* partition field id, not the source field's. The spec requires new
    # ids to come from `last-partition-id`, because a manifest's partition
    # struct uses partition field ids as its struct field ids -- reusing the
    # day field's id for a month field would leave one id meaning two different
    # things, which is exactly the v1 problem v2 introduced this counter to fix.
    next_spec_id, next_field_id = allocator.take()
    fields = []
    for field in current_spec.fields:
        if field is source_field:
            fields.append(
                IcebergPartitionField(
                    source_id=field.source_id,
                    field_id=next_field_id,
                    transform=transform,
                    name=f"{field.name.rsplit('_', 1)[0]}_{rule.to_transform}",
                )
            )
        else:
            # Carried through with its existing field id: the field is unchanged,
            # so it keeps meaning what it already meant. Only the aged field is
            # new and only it needs a fresh id.
            fields.append(field)
    spec = PartitionSpec(*fields, spec_id=next_spec_id)
    return spec, next_spec_id


def _partition_values(live: LiveFile, expected: int) -> tuple | None:
    """A file's partition tuple, or None if it does not match the spec's arity.

    An arity mismatch means the file was written under a different spec than the
    one we resolved, so its values cannot be positionally interpreted.
    """
    values: tuple = tuple(live.partition)  # type: ignore[arg-type]  # Record is iterable
    return values if len(values) == expected else None


def _group_label(current_spec, target_spec, position: int, key: tuple, rule) -> str:
    """A label naming the coarse window and, for a compound spec, the rest.

    Without the other fields, every bucket of the same month would report the
    same label and the plan output would look like duplicated work.
    """
    head = f"{rule.from_transform}->{rule.to_transform} {_render(rule.to_transform, key[position])}"
    others = [
        f"{field.name}={key[i]}" for i, field in enumerate(current_spec.fields) if i != position
    ]
    return f"{head} [{', '.join(others)}]" if others else head


def _coarse_value(granularity: str, day: dt.date) -> int:
    """Iceberg's integer encoding for the coarse partition value covering ``day``.

    ``day`` here is the window end (exclusive), so step back one day first to
    land inside the window the data actually belongs to.
    """
    inside = day - dt.timedelta(days=1)
    if granularity == "day":
        return (inside - EPOCH).days
    if granularity == "month":
        return (inside.year - 1970) * 12 + (inside.month - 1)
    if granularity == "year":
        return inside.year - 1970
    raise ValueError(f"unsupported target granularity {granularity!r}")


def _render(granularity: str, value: int) -> str:
    if granularity == "month":
        year, month = divmod(value, 12)
        return f"{1970 + year:04d}-{month + 1:02d}"
    if granularity == "year":
        return f"{1970 + value:04d}"
    if granularity == "day":
        return (EPOCH + dt.timedelta(days=value)).isoformat()
    return str(value)
