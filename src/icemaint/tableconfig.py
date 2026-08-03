"""The ``table-config.json`` specification.

This is the surface Data Engineers and Analysts author. It says how each table
should be laid out for querying -- partitioning, how partitioning ages, and how
rows are ordered within a file -- and the compaction routines read it rather
than taking flags on the command line.

Design notes worth knowing before editing:

* Everything is declarative and describes a *target*, never an action. The
  compactor decides what work that implies.
* ``defaults`` applies to every table; a table's own block overrides it
  key-by-key (a shallow merge per section, not a deep merge, so a table that
  declares ``partition_evolution`` owns it entirely and is not silently mixed
  with the default rules).
* ``ordering.mode`` is a closed choice because sort and z-order both define row
  order and cannot both apply. Making it an enum rather than "whichever field is
  present" means a config with both is rejected at load time, not silently
  resolved.
* Unknown keys are rejected. A typo like ``partiton`` would otherwise silently
  produce an unpartitioned layout, which is exactly the class of mistake this
  file exists to prevent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

SPEC_VERSION = 1

#: Iceberg partition transforms an author may name.
PARTITION_TRANSFORMS = frozenset(
    {"identity", "year", "month", "day", "hour", "bucket", "truncate", "void"}
)

#: Transforms that describe a time granularity, ordered coarse-last. Partition
#: evolution may only move a field *rightwards* along this list -- condensing
#: hours into days into months is a reduction in file count; the reverse would
#: mean splitting files, which compaction does not do.
TIME_GRANULARITIES = ("hour", "day", "month", "year")


class TableConfigError(ValueError):
    """The config is malformed or self-contradictory."""


@dataclass(frozen=True)
class PartitionField:
    column: str
    transform: str = "identity"
    num_buckets: int | None = None
    width: int | None = None
    name: str | None = None

    def iceberg_name(self) -> str:
        if self.name:
            return self.name
        if self.transform == "identity":
            return self.column
        if self.transform == "bucket":
            return f"{self.column}_bucket_{self.num_buckets}"
        if self.transform == "truncate":
            return f"{self.column}_trunc_{self.width}"
        return f"{self.column}_{self.transform}"

    def validate(self, where: str) -> None:
        if self.transform not in PARTITION_TRANSFORMS:
            raise TableConfigError(
                f"{where}: unknown partition transform {self.transform!r}; "
                f"expected one of {sorted(PARTITION_TRANSFORMS)}"
            )
        if self.transform == "bucket" and not self.num_buckets:
            raise TableConfigError(f"{where}: transform 'bucket' requires 'num_buckets'")
        if self.transform == "truncate" and not self.width:
            raise TableConfigError(f"{where}: transform 'truncate' requires 'width'")
        if self.transform != "bucket" and self.num_buckets is not None:
            raise TableConfigError(f"{where}: 'num_buckets' only applies to transform 'bucket'")
        if self.transform != "truncate" and self.width is not None:
            raise TableConfigError(f"{where}: 'width' only applies to transform 'truncate'")


@dataclass(frozen=True)
class EvolutionRule:
    """Condense one time granularity into a coarser one once data has aged.

    ``older_than_days`` is measured from the *end* of the partition window, so a
    rule with ``older_than_days: 90`` leaves a partition alone until every row it
    could contain is at least 90 days old. Ageing off the start of the window
    would rewrite partitions that are still receiving late-arriving rows.
    """

    from_transform: str
    to_transform: str
    older_than_days: int

    def validate(self, where: str) -> None:
        for name, value in (("from", self.from_transform), ("to", self.to_transform)):
            if value not in TIME_GRANULARITIES:
                raise TableConfigError(
                    f"{where}: {name}={value!r} is not a time granularity; "
                    f"expected one of {list(TIME_GRANULARITIES)}"
                )
        src = TIME_GRANULARITIES.index(self.from_transform)
        dst = TIME_GRANULARITIES.index(self.to_transform)
        if dst <= src:
            raise TableConfigError(
                f"{where}: cannot evolve {self.from_transform!r} to {self.to_transform!r}; "
                "evolution must move to a coarser granularity (compaction merges "
                "files, it never splits them)"
            )
        if self.older_than_days < 0:
            raise TableConfigError(f"{where}: older_than_days must be >= 0")


@dataclass(frozen=True)
class PartitionEvolution:
    enabled: bool = True
    rules: tuple[EvolutionRule, ...] = ()

    def validate(self, where: str) -> None:
        for i, rule in enumerate(self.rules):
            rule.validate(f"{where}.rules[{i}]")
        seen = [r.from_transform for r in self.rules]
        if len(seen) != len(set(seen)):
            raise TableConfigError(
                f"{where}: more than one rule for the same 'from' granularity; "
                "a partition field can only age one way"
            )


@dataclass(frozen=True)
class SortKey:
    column: str
    direction: Literal["asc", "desc"] = "asc"
    nulls: Literal["first", "last"] = "last"

    def validate(self, where: str) -> None:
        if self.direction not in ("asc", "desc"):
            raise TableConfigError(f"{where}: direction must be 'asc' or 'desc'")
        if self.nulls not in ("first", "last"):
            raise TableConfigError(f"{where}: nulls must be 'first' or 'last'")


@dataclass(frozen=True)
class ZOrder:
    columns: tuple[str, ...] = ()
    #: Bits of resolution kept per column before interleaving. More bits means
    #: finer locality per column but a smaller number of columns fits in the
    #: 62-bit ordering key.
    precision_bits: int = 16

    def validate(self, where: str) -> None:
        if len(self.columns) < 2:
            raise TableConfigError(
                f"{where}: z-order needs at least 2 columns; use ordering.mode "
                "'sort' for a single key"
            )
        if len(set(self.columns)) != len(self.columns):
            raise TableConfigError(f"{where}: duplicate column in z-order key")
        if not 1 <= self.precision_bits <= 32:
            raise TableConfigError(f"{where}: precision_bits must be between 1 and 32")
        budget = self.precision_bits * len(self.columns)
        if budget > 62:
            raise TableConfigError(
                f"{where}: {len(self.columns)} columns x {self.precision_bits} bits "
                f"= {budget} bits exceeds the 62-bit signed ordering key. Reduce "
                "precision_bits or the number of columns."
            )


@dataclass(frozen=True)
class Ordering:
    mode: Literal["none", "sort", "zorder"] = "none"
    sort: tuple[SortKey, ...] = ()
    zorder: ZOrder | None = None

    def validate(self, where: str) -> None:
        if self.mode not in ("none", "sort", "zorder"):
            raise TableConfigError(f"{where}.mode: must be 'none', 'sort' or 'zorder'")
        if self.mode == "sort":
            if not self.sort:
                raise TableConfigError(f"{where}: mode 'sort' requires a non-empty 'sort' list")
            for i, key in enumerate(self.sort):
                key.validate(f"{where}.sort[{i}]")
            if self.zorder:
                raise TableConfigError(f"{where}: mode is 'sort' but 'zorder' is also set")
        elif self.mode == "zorder":
            if self.zorder is None:
                raise TableConfigError(f"{where}: mode 'zorder' requires a 'zorder' block")
            self.zorder.validate(f"{where}.zorder")
            if self.sort:
                raise TableConfigError(f"{where}: mode is 'zorder' but 'sort' is also set")
        elif self.sort or self.zorder:
            raise TableConfigError(
                f"{where}: mode is 'none' but an ordering is defined; set mode to "
                "'sort' or 'zorder' to enable it"
            )


@dataclass(frozen=True)
class ExpireSnapshotsSettings:
    """How much history a table keeps.

    Every field defaults to ``None``, meaning "defer to the Iceberg table
    property, then to the spec default". That matters: a table that has already
    been tuned with ``history.expire.*`` should not have those silently
    overridden just because this file exists.
    """

    enabled: bool = True
    max_snapshot_age_days: int | None = None
    min_snapshots_to_keep: int | None = None
    max_ref_age_days: int | None = None

    def validate(self, where: str) -> None:
        for name, value in (
            ("max_snapshot_age_days", self.max_snapshot_age_days),
            ("max_ref_age_days", self.max_ref_age_days),
        ):
            if value is not None and value < 0:
                raise TableConfigError(f"{where}.{name}: must be >= 0")
        if self.min_snapshots_to_keep is not None and self.min_snapshots_to_keep < 1:
            raise TableConfigError(
                f"{where}.min_snapshots_to_keep: must be >= 1 -- a table always keeps "
                "at least its current snapshot"
            )


@dataclass(frozen=True)
class RemoveOrphanFilesSettings:
    """Deleting files under the table location that nothing references.

    ``older_than_days`` must exceed the longest write the warehouse performs,
    because a file written but not yet committed is indistinguishable from an
    orphan. Iceberg's own guidance and default is 3 days.
    """

    enabled: bool = True
    older_than_days: int = 3

    def validate(self, where: str) -> None:
        if self.older_than_days < 0:
            raise TableConfigError(f"{where}.older_than_days: must be >= 0")


@dataclass(frozen=True)
class RemoveDanglingDeletesSettings:
    """Dropping delete files that no longer apply to any live data file.

    Metadata only, and it cannot change what a scan returns -- a dangling delete
    is by definition one no scan would apply -- so it is on by default. Only
    merge-on-read tables have anything to do here.
    """

    enabled: bool = True


@dataclass(frozen=True)
class RewriteManifestsSettings:
    """Regrouping manifest entries by partition so predicates can prune them.

    Metadata only, and it neither adds nor removes a file, so it is on by
    default. ``min_input_manifests`` is the floor below which there is nothing
    to regroup.
    """

    enabled: bool = True
    min_input_manifests: int = 2

    def validate(self, where: str) -> None:
        if self.min_input_manifests < 2:
            raise TableConfigError(
                f"{where}.min_input_manifests: must be >= 2 -- one manifest cannot be regrouped"
            )


@dataclass(frozen=True)
class MetadataSettings:
    """How many ``metadata.json`` versions the table keeps, and who deletes them.

    Both default to ``None``, meaning "leave the table property alone". Setting
    ``previous_versions_max`` *without* ``delete_after_commit`` trims the log
    but deletes nothing, turning every dropped entry into an unreferenced file
    -- see :mod:`icemaint.properties` for the measured behaviour. That is a
    supported choice, because orphan removal sweeps them behind its age guard,
    and it is the safer one under concurrent writers.
    """

    previous_versions_max: int | None = None
    delete_after_commit: bool | None = None

    def validate(self, where: str) -> None:
        if self.previous_versions_max is not None and self.previous_versions_max < 1:
            raise TableConfigError(
                f"{where}.previous_versions_max: must be >= 1 -- the log always keeps "
                "at least the current metadata"
            )


@dataclass(frozen=True)
class Retention:
    expire_snapshots: ExpireSnapshotsSettings = field(default_factory=ExpireSnapshotsSettings)
    remove_orphan_files: RemoveOrphanFilesSettings = field(
        default_factory=RemoveOrphanFilesSettings
    )
    remove_dangling_deletes: RemoveDanglingDeletesSettings = field(
        default_factory=RemoveDanglingDeletesSettings
    )
    rewrite_manifests: RewriteManifestsSettings = field(default_factory=RewriteManifestsSettings)
    metadata: MetadataSettings = field(default_factory=MetadataSettings)

    def validate(self, where: str) -> None:
        self.expire_snapshots.validate(f"{where}.expire_snapshots")
        self.remove_orphan_files.validate(f"{where}.remove_orphan_files")
        self.rewrite_manifests.validate(f"{where}.rewrite_manifests")
        self.metadata.validate(f"{where}.metadata")

        # Orphan removal deletes what expiry unreferences. Running it without
        # expiry is legal but almost never intended, and silently reclaiming
        # far less than expected is the confusing outcome.
        if self.remove_orphan_files.enabled and not self.expire_snapshots.enabled:
            raise TableConfigError(
                f"{where}: remove_orphan_files is enabled but expire_snapshots is not. "
                "Snapshots hold references to superseded files, so almost nothing would "
                "be reclaimable. Enable expiry, or disable orphan removal explicitly."
            )


@dataclass(frozen=True)
class TableSettings:
    """The resolved layout target for one table."""

    partition: tuple[PartitionField, ...] = ()
    partition_evolution: PartitionEvolution = field(default_factory=PartitionEvolution)
    ordering: Ordering = field(default_factory=Ordering)
    retention: Retention = field(default_factory=Retention)
    target_file_size_bytes: int | None = None
    min_input_files: int = 2
    description: str | None = None

    def validate(self, where: str) -> None:
        for i, pf in enumerate(self.partition):
            pf.validate(f"{where}.partition[{i}]")
        names = [pf.iceberg_name() for pf in self.partition]
        if len(names) != len(set(names)):
            raise TableConfigError(f"{where}.partition: duplicate partition field name in {names}")
        self.partition_evolution.validate(f"{where}.partition_evolution")
        self.ordering.validate(f"{where}.ordering")
        self.retention.validate(f"{where}.retention")
        if self.target_file_size_bytes is not None and self.target_file_size_bytes < 100:
            raise TableConfigError(f"{where}.target_file_size_bytes: must be >= 100")
        if self.min_input_files < 1:
            raise TableConfigError(f"{where}.min_input_files: must be >= 1")

        # An evolution rule that names a granularity the table does not
        # partition by is almost always a typo, and silently doing nothing is
        # the worst possible response to it.
        if self.partition_evolution.enabled and self.partition:
            declared = {pf.transform for pf in self.partition}
            for i, rule in enumerate(self.partition_evolution.rules):
                if rule.from_transform not in declared:
                    raise TableConfigError(
                        f"{where}.partition_evolution.rules[{i}]: no partition field uses "
                        f"transform {rule.from_transform!r} (declared: {sorted(declared)})"
                    )


#: Applied to every table unless overridden. Days-to-months condensation is the
#: default behaviour asked for by the pipeline team; set ``enabled: false`` here
#: to turn it off fleet-wide, or per table to exempt one.
DEFAULT_PARTITION_EVOLUTION = PartitionEvolution(
    enabled=True,
    rules=(EvolutionRule(from_transform="day", to_transform="month", older_than_days=90),),
)

DEFAULT_SETTINGS = TableSettings(partition_evolution=DEFAULT_PARTITION_EVOLUTION)


@dataclass(frozen=True)
class TableConfig:
    """A parsed ``table-config.json``."""

    version: int = SPEC_VERSION
    defaults: TableSettings = field(default_factory=lambda: DEFAULT_SETTINGS)
    tables: dict[str, TableSettings] = field(default_factory=dict)
    source: str | None = None

    def for_table(self, identifier: str) -> TableSettings:
        """Settings for ``identifier``, with defaults filled in.

        The merge is per-section: a table that declares ``ordering`` replaces
        the default ordering wholesale rather than having individual keys
        blended, so what an author reads in one block is what applies.
        """
        override = self.tables.get(identifier)
        if override is None:
            return self.defaults
        return replace(
            self.defaults,
            partition=override.partition or self.defaults.partition,
            partition_evolution=override.partition_evolution
            if override.partition_evolution != PartitionEvolution()
            else self.defaults.partition_evolution,
            ordering=(
                override.ordering if override.ordering.mode != "none" else self.defaults.ordering
            ),
            target_file_size_bytes=(
                override.target_file_size_bytes
                if override.target_file_size_bytes is not None
                else self.defaults.target_file_size_bytes
            ),
            retention=(
                override.retention if override.retention != Retention() else self.defaults.retention
            ),
            min_input_files=override.min_input_files,
            description=override.description,
        )

    def validate(self) -> None:
        if self.version != SPEC_VERSION:
            raise TableConfigError(
                f"unsupported table-config version {self.version}; "
                f"this build understands {SPEC_VERSION}"
            )
        self.defaults.validate("defaults")
        for identifier, settings in self.tables.items():
            if identifier.count(".") < 1:
                raise TableConfigError(
                    f"tables.{identifier!r}: table keys must be 'namespace.table'"
                )
            settings.validate(f"tables.{identifier}")

    # -- serialisation ----------------------------------------------------

    @classmethod
    def load(cls, path: str | Path) -> TableConfig:
        path = Path(path)
        try:
            raw = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise TableConfigError(f"{path}: invalid JSON: {exc}") from None
        config = cls.from_dict(raw, source=str(path))
        config.validate()
        return config

    @classmethod
    def from_dict(cls, raw: dict[str, Any], source: str | None = None) -> TableConfig:
        _reject_unknown(raw, {"version", "defaults", "tables"}, "<root>")
        defaults = (
            _settings_from_dict(raw["defaults"], "defaults")
            if "defaults" in raw
            else DEFAULT_SETTINGS
        )
        tables = {
            identifier: _settings_from_dict(block, f"tables.{identifier}")
            for identifier, block in (raw.get("tables") or {}).items()
        }
        return cls(
            version=raw.get("version", SPEC_VERSION),
            defaults=defaults,
            tables=tables,
            source=source,
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "version": self.version,
            "defaults": _settings_to_dict(self.defaults),
        }
        out["tables"] = {k: _settings_to_dict(v) for k, v in sorted(self.tables.items())}
        return out

    def dump(self, path: str | Path, *, indent: int = 2) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), indent=indent) + "\n")


# -- dict <-> dataclass ---------------------------------------------------


def _reject_unknown(raw: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise TableConfigError(
            f"{where}: unknown key(s) {unknown}; allowed: {sorted(allowed)}. "
            "Keys are rejected rather than ignored so a typo cannot silently "
            "change a table's layout."
        )


def _settings_from_dict(raw: dict[str, Any], where: str) -> TableSettings:
    _reject_unknown(
        raw,
        {
            "partition",
            "partition_evolution",
            "ordering",
            "retention",
            "target_file_size_bytes",
            "min_input_files",
            "description",
        },
        where,
    )
    partition = tuple(
        _partition_field_from_dict(pf, f"{where}.partition[{i}]")
        for i, pf in enumerate(raw.get("partition") or ())
    )
    evolution = (
        _evolution_from_dict(raw["partition_evolution"], f"{where}.partition_evolution")
        if "partition_evolution" in raw
        else PartitionEvolution()
    )
    ordering = (
        _ordering_from_dict(raw["ordering"], f"{where}.ordering")
        if "ordering" in raw
        else Ordering()
    )
    retention = (
        _retention_from_dict(raw["retention"], f"{where}.retention")
        if "retention" in raw
        else Retention()
    )
    return TableSettings(
        partition=partition,
        partition_evolution=evolution,
        ordering=ordering,
        retention=retention,
        target_file_size_bytes=raw.get("target_file_size_bytes"),
        min_input_files=raw.get("min_input_files", 2),
        description=raw.get("description"),
    )


def _partition_field_from_dict(raw: dict[str, Any], where: str) -> PartitionField:
    _reject_unknown(raw, {"column", "transform", "num_buckets", "width", "name"}, where)
    if "column" not in raw:
        raise TableConfigError(f"{where}: 'column' is required")
    return PartitionField(
        column=raw["column"],
        transform=raw.get("transform", "identity"),
        num_buckets=raw.get("num_buckets"),
        width=raw.get("width"),
        name=raw.get("name"),
    )


def _evolution_from_dict(raw: dict[str, Any], where: str) -> PartitionEvolution:
    _reject_unknown(raw, {"enabled", "rules"}, where)
    rules = []
    for i, rule in enumerate(raw.get("rules") or ()):
        _reject_unknown(rule, {"from", "to", "older_than_days"}, f"{where}.rules[{i}]")
        missing = {"from", "to", "older_than_days"} - set(rule)
        if missing:
            raise TableConfigError(f"{where}.rules[{i}]: missing {sorted(missing)}")
        rules.append(
            EvolutionRule(
                from_transform=rule["from"],
                to_transform=rule["to"],
                older_than_days=rule["older_than_days"],
            )
        )
    return PartitionEvolution(enabled=raw.get("enabled", True), rules=tuple(rules))


def _ordering_from_dict(raw: dict[str, Any], where: str) -> Ordering:
    _reject_unknown(raw, {"mode", "sort", "zorder"}, where)
    sort = []
    for i, key in enumerate(raw.get("sort") or ()):
        _reject_unknown(key, {"column", "direction", "nulls"}, f"{where}.sort[{i}]")
        if "column" not in key:
            raise TableConfigError(f"{where}.sort[{i}]: 'column' is required")
        sort.append(
            SortKey(
                column=key["column"],
                direction=key.get("direction", "asc"),
                nulls=key.get("nulls", "last"),
            )
        )
    zorder = None
    if "zorder" in raw:
        _reject_unknown(raw["zorder"], {"columns", "precision_bits"}, f"{where}.zorder")
        zorder = ZOrder(
            columns=tuple(raw["zorder"].get("columns") or ()),
            precision_bits=raw["zorder"].get("precision_bits", 16),
        )
    return Ordering(mode=raw.get("mode", "none"), sort=tuple(sort), zorder=zorder)


def _retention_from_dict(raw: dict[str, Any], where: str) -> Retention:
    _reject_unknown(
        raw,
        {
            "expire_snapshots",
            "remove_orphan_files",
            "remove_dangling_deletes",
            "rewrite_manifests",
            "metadata",
        },
        where,
    )

    expire = ExpireSnapshotsSettings()
    if "expire_snapshots" in raw:
        block = raw["expire_snapshots"]
        _reject_unknown(
            block,
            {"enabled", "max_snapshot_age_days", "min_snapshots_to_keep", "max_ref_age_days"},
            f"{where}.expire_snapshots",
        )
        expire = ExpireSnapshotsSettings(
            enabled=block.get("enabled", True),
            max_snapshot_age_days=block.get("max_snapshot_age_days"),
            min_snapshots_to_keep=block.get("min_snapshots_to_keep"),
            max_ref_age_days=block.get("max_ref_age_days"),
        )

    orphans = RemoveOrphanFilesSettings()
    if "remove_orphan_files" in raw:
        block = raw["remove_orphan_files"]
        _reject_unknown(block, {"enabled", "older_than_days"}, f"{where}.remove_orphan_files")
        orphans = RemoveOrphanFilesSettings(
            enabled=block.get("enabled", True),
            older_than_days=block.get("older_than_days", 3),
        )

    dangling = RemoveDanglingDeletesSettings()
    if "remove_dangling_deletes" in raw:
        block = raw["remove_dangling_deletes"]
        _reject_unknown(block, {"enabled"}, f"{where}.remove_dangling_deletes")
        dangling = RemoveDanglingDeletesSettings(enabled=block.get("enabled", True))

    rewrite = RewriteManifestsSettings()
    if "rewrite_manifests" in raw:
        block = raw["rewrite_manifests"]
        _reject_unknown(block, {"enabled", "min_input_manifests"}, f"{where}.rewrite_manifests")
        rewrite = RewriteManifestsSettings(
            enabled=block.get("enabled", True),
            min_input_manifests=block.get("min_input_manifests", 2),
        )

    metadata = MetadataSettings()
    if "metadata" in raw:
        block = raw["metadata"]
        _reject_unknown(
            block, {"previous_versions_max", "delete_after_commit"}, f"{where}.metadata"
        )
        metadata = MetadataSettings(
            previous_versions_max=block.get("previous_versions_max"),
            delete_after_commit=block.get("delete_after_commit"),
        )

    return Retention(
        expire_snapshots=expire,
        remove_orphan_files=orphans,
        remove_dangling_deletes=dangling,
        rewrite_manifests=rewrite,
        metadata=metadata,
    )


def _settings_to_dict(s: TableSettings) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if s.description:
        out["description"] = s.description
    if s.partition:
        out["partition"] = [_partition_field_to_dict(pf) for pf in s.partition]
    out["partition_evolution"] = {
        "enabled": s.partition_evolution.enabled,
        "rules": [
            {
                "from": r.from_transform,
                "to": r.to_transform,
                "older_than_days": r.older_than_days,
            }
            for r in s.partition_evolution.rules
        ],
    }
    if s.ordering.mode != "none":
        ordering: dict[str, Any] = {"mode": s.ordering.mode}
        if s.ordering.mode == "sort":
            ordering["sort"] = [
                {"column": k.column, "direction": k.direction, "nulls": k.nulls}
                for k in s.ordering.sort
            ]
        elif s.ordering.zorder:
            ordering["zorder"] = {
                "columns": list(s.ordering.zorder.columns),
                "precision_bits": s.ordering.zorder.precision_bits,
            }
        out["ordering"] = ordering
    out["retention"] = {
        "expire_snapshots": {
            k: v
            for k, v in {
                "enabled": s.retention.expire_snapshots.enabled,
                "max_snapshot_age_days": s.retention.expire_snapshots.max_snapshot_age_days,
                "min_snapshots_to_keep": s.retention.expire_snapshots.min_snapshots_to_keep,
                "max_ref_age_days": s.retention.expire_snapshots.max_ref_age_days,
            }.items()
            if v is not None
        },
        "remove_orphan_files": {
            "enabled": s.retention.remove_orphan_files.enabled,
            "older_than_days": s.retention.remove_orphan_files.older_than_days,
        },
        "remove_dangling_deletes": {
            "enabled": s.retention.remove_dangling_deletes.enabled,
        },
        "rewrite_manifests": {
            "enabled": s.retention.rewrite_manifests.enabled,
            "min_input_manifests": s.retention.rewrite_manifests.min_input_manifests,
        },
        "metadata": {
            k: v
            for k, v in {
                "previous_versions_max": s.retention.metadata.previous_versions_max,
                "delete_after_commit": s.retention.metadata.delete_after_commit,
            }.items()
            if v is not None
        },
    }
    if s.target_file_size_bytes is not None:
        out["target_file_size_bytes"] = s.target_file_size_bytes
    if s.min_input_files != 2:
        out["min_input_files"] = s.min_input_files
    return out


def _partition_field_to_dict(pf: PartitionField) -> dict[str, Any]:
    out: dict[str, Any] = {"column": pf.column, "transform": pf.transform}
    if pf.num_buckets is not None:
        out["num_buckets"] = pf.num_buckets
    if pf.width is not None:
        out["width"] = pf.width
    if pf.name:
        out["name"] = pf.name
    return out
