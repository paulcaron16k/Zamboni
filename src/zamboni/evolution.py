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

from pyiceberg.manifest import DataFile, ManifestEntry, ManifestEntryStatus, ManifestFile
from pyiceberg.partitioning import PARTITION_FIELD_ID_START, PartitionSpec
from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.table.update import AddPartitionSpecUpdate, AssertTableUUID
from pyiceberg.transforms import DayTransform, HourTransform, MonthTransform, YearTransform
from pyiceberg.typedef import EMPTY_DICT, Record

from .committer import _ReplaceFiles
from .profile import LiveFile
from .tableconfig import EvolutionRule, TableSettings

EPOCH = dt.date(1970, 1, 1)


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

    Upstream writes every added file into a single manifest declared under the
    table's default spec. That is fine while all added files share that spec and
    wrong the moment they do not -- which is exactly what partition evolution
    produces.

    Named for its usual operation but it honours whichever the committer sets, so
    it is equally usable for an ``overwrite`` snapshot.
    """

    def _summary(self, snapshot_properties: dict[str, str] = EMPTY_DICT) -> Summary:
        """Summarise added files under *their own* spec.

        ``_SnapshotProducer._summary`` passes ``table_metadata.spec()`` -- the
        default -- for every added file, while using each file's own spec for
        removed ones: the same asymmetry ``_manifests`` has. Left alone, a
        month-partitioned file gets its partition value rendered through the
        day spec, so a table with ``write.summary.partition-limit`` set records
        a garbage label like ``ts_day=1971-11-04`` in permanent snapshot
        metadata.
        """
        from pyiceberg.table import TableProperties
        from pyiceberg.table.snapshots import (
            SnapshotSummaryCollector,
            Summary,
            update_snapshot_summaries,
        )

        table_metadata = self._transaction.table_metadata
        specs = table_metadata.specs()
        default_spec_id = table_metadata.default_spec_id

        ssc = SnapshotSummaryCollector(
            partition_summary_limit=int(
                table_metadata.properties.get(
                    TableProperties.WRITE_PARTITION_SUMMARY_LIMIT,
                    TableProperties.WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT,
                )
            )
        )
        for data_file in self._added_data_files:
            ssc.add_file(
                data_file=data_file,
                partition_spec=specs[_spec_id_of(data_file, default_spec_id)],
                schema=table_metadata.schema(),
            )
        for data_file in self._deleted_data_files:
            ssc.remove_file(
                data_file=data_file,
                partition_spec=specs[data_file.spec_id],
                schema=table_metadata.schema(),
            )

        previous_snapshot = (
            table_metadata.snapshot_by_id(self._parent_snapshot_id)
            if self._parent_snapshot_id is not None
            else None
        )
        summary = update_snapshot_summaries(
            summary=Summary(operation=Operation.OVERWRITE, **ssc.build(), **snapshot_properties),
            previous_summary=previous_snapshot.summary if previous_snapshot else None,
        )
        # Built as OVERWRITE because update_snapshot_summaries rejects REPLACE,
        # then labelled with whatever the committer actually asked for. Hardcoding
        # REPLACE here made `snapshot_operation="overwrite"` -- the escape hatch
        # for anyone unwilling to subclass PyIceberg internals -- silently produce
        # a replace snapshot on any evolved table.
        return Summary(operation=self._operation, **summary.additional_properties)

    def _manifests(self) -> list[ManifestFile]:
        from pyiceberg.manifest import write_manifest

        default_spec_id = self._transaction.table_metadata.default_spec_id
        by_spec: dict[int, list[DataFile]] = defaultdict(list)
        for data_file in self._added_data_files:
            by_spec[_spec_id_of(data_file, default_spec_id)].append(data_file)

        if len(by_spec) <= 1:
            spec_id = next(iter(by_spec), self._transaction.table_metadata.default_spec_id)
            if spec_id == self._transaction.table_metadata.default_spec_id:
                # Nothing unusual: let upstream handle it, so we inherit any
                # behaviour it gains.
                return super()._manifests()

        added_manifests: list[ManifestFile] = []
        for spec_id, data_files in by_spec.items():
            with write_manifest(
                format_version=self._transaction.table_metadata.format_version,
                spec=self._transaction.table_metadata.specs()[spec_id],
                schema=self._transaction.table_metadata.schema(),
                output_file=self.new_manifest_output(),
                snapshot_id=self._snapshot_id,
                avro_compression=self._compression,
            ) as writer:
                for data_file in data_files:
                    writer.add(
                        ManifestEntry.from_args(
                            status=ManifestEntryStatus.ADDED,
                            snapshot_id=self._snapshot_id,
                            sequence_number=None,
                            file_sequence_number=None,
                            data_file=data_file,
                        )
                    )
            added_manifests.append(writer.to_manifest_file())

        deleted = self._deleted_entries()
        delete_manifests: list[ManifestFile] = []
        if deleted:
            groups: dict[int, list[ManifestEntry]] = defaultdict(list)
            for entry in deleted:
                groups[entry.data_file.spec_id].append(entry)
            for spec_id, entries in groups.items():
                with write_manifest(
                    format_version=self._transaction.table_metadata.format_version,
                    spec=self._transaction.table_metadata.specs()[spec_id],
                    schema=self._transaction.table_metadata.schema(),
                    output_file=self.new_manifest_output(),
                    snapshot_id=self._snapshot_id,
                    avro_compression=self._compression,
                ) as writer:
                    for entry in entries:
                        writer.add_entry(entry)
                delete_manifests.append(writer.to_manifest_file())

        return self._process_manifests(
            added_manifests + delete_manifests + self._existing_manifests()
        )


def _spec_id_of(data_file: DataFile, default: int) -> int:
    """A data file's partition spec, tolerating one that has never had it set.

    ``spec_id`` is not part of the data-file struct -- PyIceberg populates it
    from the manifest when reading, and ``DataFile.from_args`` leaves the
    backing attribute unset -- so a freshly written file raises rather than
    returning a default.
    """
    try:
        return data_file.spec_id
    except AttributeError:
        return default


@dataclass(frozen=True)
class EvolutionGroup:
    """Files sharing one coarse partition value, to be merged under ``spec_id``."""

    rule: EvolutionRule
    target_spec_id: int
    target_partition: Record
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
        if len(current_spec.fields) != 1:
            # Condensing one field of a compound spec means synthesising a new
            # spec for every combination of the others; correct handling is not
            # obvious enough to guess at.
            reason = (
                f"spec has {len(current_spec.fields)} fields; only single-field "
                "specs are evolved automatically"
            )
            plan.skipped.append((rule.from_transform, reason))
            continue

        source_field = source_fields[0]
        target_spec, spec_id = _resolve_target_spec(tbl, source_field, rule, allocator)
        plan.required_specs[spec_id] = target_spec

        cutoff = today - dt.timedelta(days=rule.older_than_days)
        buckets: dict[int, list[LiveFile]] = defaultdict(list)
        for live in live_files:
            if live.spec_id != current_spec.spec_id:
                continue
            value = _single_partition_value(live)
            if value is None:
                continue
            window_end = _window_end(rule.from_transform, value)
            if window_end > cutoff:
                continue  # still inside the retention window
            buckets[_coarse_value(rule.to_transform, window_end)].append(live)

        for coarse, files in sorted(buckets.items()):
            rendered = _render(rule.to_transform, coarse)
            label = f"{rule.from_transform}->{rule.to_transform} {rendered}"
            if len(files) < 2:
                plan.skipped.append((label, "fewer than 2 files to merge"))
                continue
            plan.groups.append(
                EvolutionGroup(
                    rule=rule,
                    target_spec_id=spec_id,
                    target_partition=Record(coarse),
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
    source_field: IcebergPartitionField,
    rule: EvolutionRule,
    allocator: _IdAllocator,
) -> tuple[PartitionSpec, int]:
    """Find or design the coarse spec this rule targets."""
    transform = TRANSFORM_FOR[rule.to_transform]()
    for spec_id, spec in tbl.metadata.specs().items():
        if (
            len(spec.fields) == 1
            and spec.fields[0].source_id == source_field.source_id
            and _transform_name(spec.fields[0].transform) == rule.to_transform
        ):
            return spec, spec_id

    # A *new* partition field id, not the source field's. The spec requires new
    # ids to come from `last-partition-id`, because a manifest's partition
    # struct uses partition field ids as its struct field ids -- reusing the
    # day field's id for a month field would leave one id meaning two different
    # things, which is exactly the v1 problem v2 introduced this counter to fix.
    next_spec_id, next_field_id = allocator.take()
    spec = PartitionSpec(
        IcebergPartitionField(
            source_id=source_field.source_id,
            field_id=next_field_id,
            transform=transform,
            name=f"{source_field.name.rsplit('_', 1)[0]}_{rule.to_transform}",
        ),
        spec_id=next_spec_id,
    )
    return spec, next_spec_id


def _transform_name(transform) -> str:
    return str(transform)


def _single_partition_value(live: LiveFile) -> int | None:
    values = list(live.partition)  # type: ignore[call-overload]  # Record is iterable
    if len(values) != 1 or values[0] is None:
        return None
    return int(values[0])


def _window_end(granularity: str, value: int) -> dt.date:
    """The first date *after* the partition window, so ageing is conservative."""
    if granularity == "hour":
        return (dt.datetime(1970, 1, 1) + dt.timedelta(hours=value + 1)).date()
    if granularity == "day":
        return EPOCH + dt.timedelta(days=value + 1)
    if granularity == "month":
        year, month = divmod(value, 12)
        return _add_month(dt.date(1970 + year, month + 1, 1))
    if granularity == "year":
        return dt.date(1970 + value + 1, 1, 1)
    raise ValueError(f"unsupported granularity {granularity!r}")


def _add_month(d: dt.date) -> dt.date:
    return dt.date(d.year + 1, 1, 1) if d.month == 12 else dt.date(d.year, d.month + 1, 1)


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
