# SPDX-License-Identifier: Apache-2.0
"""The ``table-config.json`` specification.

This is the surface Data Engineers and Analysts author. It says how each table
should be laid out for querying -- partitioning, how partitioning ages, and how
rows are ordered within a file -- and the compaction routines read it rather
than taking flags on the command line.

Design notes worth knowing before editing:

* Everything is declarative and describes a *target*, never an action. The
  compactor decides what work that implies.
* A table block writes only what differs from ``defaults``. Every key is
  inherited by presence, at every depth: a key the block contains wins whatever
  its value, ``null`` included; a key it leaves out comes from ``defaults``, and
  from the built-in when ``defaults`` leaves it out too. Lists are values and
  replace whole, so a table's ``partition_evolution.rules`` are never mixed
  with the default rules, and so is ``ordering`` (see :func:`_merge`).
* ``ordering.mode`` is a closed choice because sort and z-order both define row
  order and cannot both apply. Making it an enum rather than "whichever field is
  present" means a config with both is rejected at load time, not silently
  resolved.
* Unknown keys are rejected. A typo like ``partiton`` would otherwise silently
  produce an unpartitioned layout, which is exactly the class of mistake this
  file exists to prevent.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

SPEC_VERSION = 2

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
    -- see :mod:`zamboni.properties` for the measured behaviour. That is a
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
    #: On by default: hold the open window and the one before it (docs/table-config.md).
    #: ``None`` turns the hold off.
    skip_partitions_newer_than_windows: int | None = 1
    description: str | None = None
    #: The block as written in the file -- only the keys it contains -- or
    #: ``None`` for settings built in code, which then count as setting every
    #: key. Inheritance is decided on this, not on the field values: a value
    #: cannot say whether it was written or left out, and comparing it with the
    #: built-in default could not tell, so a written default and an omitted key
    #: merged alike (ZMBNI-216, ZMBNI-220).
    block: Mapping[str, Any] | None = field(default=None, compare=False, repr=False)

    @classmethod
    def from_dict(cls, block: Mapping[str, Any], where: str = "<block>") -> TableSettings:
        """Parse one block as it would appear in the file, keeping what it set.

        The way to build a table's settings in code that still inherit from
        ``defaults``: a block ``{"partition": [...]}`` sets the partition and
        nothing else. Constructing :class:`TableSettings` directly sets
        everything.
        """
        return _settings_from_dict(dict(block), where)

    def as_block(self) -> dict[str, Any]:
        """What this block sets, as the file would spell it."""
        return copy.deepcopy(dict(self.block)) if self.block is not None else _full_block(self)

    def sets(self, *path: str) -> bool:
        """Does this block set the key at ``path``, e.g. ``("retention",
        "expire_snapshots", "enabled")``? Settings built in code set every key.

        A key under a block that replaces whole (`ordering`) is set when the
        block is: the merge takes all of it or none of it.
        """
        if self.block is None:
            return True
        node: Any = self.block
        for key in path:
            if not isinstance(node, Mapping) or key not in node:
                return False
            if key in _REPLACED_WHOLE:
                return True
            node = node[key]
        return True

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
        if (
            self.skip_partitions_newer_than_windows is not None
            and self.skip_partitions_newer_than_windows < 0
        ):
            raise TableConfigError(f"{where}.skip_partitions_newer_than_windows: must be >= 0")

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


#: Objects that are one value rather than a set of settings; see :func:`_merge`.
_REPLACED_WHOLE = frozenset({"ordering"})


def _merge(base: Mapping[str, Any], over: Mapping[str, Any]) -> dict[str, Any]:
    """``over`` on top of ``base``, by presence, at every depth.

    A key ``over`` contains wins whatever its value -- ``null``, ``false``,
    ``[]``, or a value equal to the built-in. Objects merge key by key; lists
    and scalars replace.

    **``ordering`` replaces whole.** It is one choice, a mode and that mode's
    keys, not a set of independent settings: merged key by key, a table choosing
    ``zorder`` under a ``defaults`` that sorts would inherit the sort keys as
    well, which `Ordering.validate` rightly refuses.
    """
    out = copy.deepcopy(dict(base))
    for key, value in over.items():
        if (
            isinstance(value, Mapping)
            and isinstance(out.get(key), Mapping)
            and key not in _REPLACED_WHOLE
        ):
            out[key] = _merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


@dataclass(frozen=True)
class NamespaceSettings:
    """One namespace and the tables in it.

    A wrapper around a single field today, and deliberately a wrapper rather
    than a bare dict: namespace-level defaults are the obvious next thing to
    want ("everything in `raw` keeps three days"), and adding a key beside
    ``tables`` is then not a format change.
    """

    tables: dict[str, TableSettings] = field(default_factory=dict)


@dataclass(frozen=True)
class TableConfig:
    """A parsed ``table-config.json``.

    The file is authored as **warehouse -> namespace -> table**, which is the
    database/schema/table shape every data engineer already has. Version 2 made
    that explicit; version 1 keyed tables by a dotted string and left the split
    to be guessed.

    Guessed is the right word. ``raw.telemetry.events`` was resolved with
    ``rpartition`` -- the *last* dot wins -- so the local engine read it as
    namespace ``('raw','telemetry')`` while Trino and Spark quoted it as a
    single namespace literally named ``raw.telemetry``. The same key, three
    engines, two different tables, no error anywhere. Stating the namespace and
    forbidding dots in the table name removes the guess rather than fixing it.

    ``tables`` stays available as the flat ``namespace.table`` mapping the rest
    of the package works in, computed once here. That form is now *built* from
    a stated namespace and a dotless name instead of parsed back out of a
    string, so it cannot disagree with the file.
    """

    #: Which warehouse this file describes. Required. It does not select the
    #: warehouse -- ``--warehouse``/``--db`` or the profile does that -- it
    #: asserts, so copying acme's config into globex's directory and forgetting
    #: to edit it is an error instead of a silent maintenance run against the
    #: wrong tenant.
    warehouse: str = ""
    version: int = SPEC_VERSION
    defaults: TableSettings = field(default_factory=lambda: DEFAULT_SETTINGS)
    namespaces: dict[str, NamespaceSettings] = field(default_factory=dict)
    source: str | None = None
    #: ``{"namespace.table": settings}``. Derived; never authored.
    tables: dict[str, TableSettings] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tables",
            {
                f"{namespace}.{name}": settings
                for namespace, block in self.namespaces.items()
                for name, settings in block.tables.items()
            },
        )

    def namespace_of(self, identifier: str) -> tuple[str, ...]:
        """The namespace parts of a flat identifier, as Iceberg wants them.

        Looked up rather than parsed: the file said where the namespace ends,
        so a nested one comes back as ``('raw', 'telemetry')`` on every engine.
        """
        for namespace, block in self.namespaces.items():
            for name in block.tables:
                if f"{namespace}.{name}" == identifier:
                    return tuple(namespace.split("."))
        raise KeyError(identifier)

    def for_table(self, identifier: str) -> TableSettings:
        """The settings ``identifier`` runs with: its block over ``defaults``
        over the built-in, by presence at every depth (:func:`_merge`).

        A table the file does not name gets ``defaults`` alone.
        """
        override = self.tables.get(identifier)
        merged = _merge(_BUILT_IN_BLOCK, self.defaults.as_block())
        if override is not None:
            merged = _merge(merged, override.as_block())
        # `description` describes one table, so it is the one key not inherited.
        merged["description"] = override.description if override is not None else None
        return _settings_from_dict(merged, identifier if override is not None else "defaults")

    def source_of(self, identifier: str, *path: str) -> Literal["table", "defaults", "built-in"]:
        """Which block the value at ``path`` comes from for ``identifier``."""
        override = self.tables.get(identifier)
        if override is not None and override.sets(*path):
            return "table"
        if self.defaults.sets(*path):
            return "defaults"
        return "built-in"

    def validate(self) -> None:
        if self.version != SPEC_VERSION:
            raise TableConfigError(
                f"unsupported table-config version {self.version}; "
                f"this build understands {SPEC_VERSION}"
            )
        if not self.warehouse:
            raise TableConfigError(
                "<root>: 'warehouse' is required. It names the warehouse this file "
                "describes, and is checked against the one being maintained -- so a "
                "config copied into the wrong directory is an error rather than a "
                "run against the wrong tenant."
            )
        self.for_table("").validate("defaults")  # no table is named ""
        for namespace, block in self.namespaces.items():
            where = f"namespaces.{namespace}"
            if not namespace:
                raise TableConfigError("namespaces: a namespace name cannot be empty")
            if any(not part for part in namespace.split(".")):
                raise TableConfigError(
                    f"{where!r}: a dot in a namespace separates nesting levels, so "
                    "no level may be empty"
                )
            if not block.tables:
                raise TableConfigError(f"{where}: declares no tables")
            for name in block.tables:
                if not name:
                    raise TableConfigError(f"{where}.tables: a table name cannot be empty")
                if "." in name:
                    raise TableConfigError(
                        f"{where}.tables.{name!r}: a table name cannot contain a dot. "
                        "Dots separate namespace levels, so putting one here is the "
                        "ambiguity this format exists to remove -- move the prefix into "
                        "the namespace."
                    )
                # The merged result, not the block: a block is a fragment, and
                # whether it makes sense depends on what it inherits -- a
                # partition_evolution rule names a transform `defaults` may
                # declare.
                self.for_table(f"{namespace}.{name}").validate(f"{where}.tables.{name}")

    # -- serialisation ----------------------------------------------------

    @classmethod
    def load(cls, path: str | Path) -> TableConfig:
        path = Path(path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise TableConfigError(f"{path}: invalid JSON: {exc}") from None
        config = cls.from_dict(raw, source=str(path))
        config.validate()
        return config

    @classmethod
    def from_dict(cls, raw: dict[str, Any], source: str | None = None) -> TableConfig:
        if "tables" in raw and "namespaces" not in raw:
            # The version 1 shape. Naming it beats "unknown key(s) ['tables']",
            # which is what the generic check would say and which does not tell
            # anyone what to do about it.
            raise TableConfigError(
                "<root>: 'tables' at the top level is the version 1 shape, where a "
                "table key was 'namespace.table' and the split had to be guessed. "
                "Version 2 states it: {'warehouse': ..., 'namespaces': {'<namespace>': "
                "{'tables': {'<name>': {...}}}}}. Regenerate with `zamboni table-config "
                "generate`, or see docs/table-config.md."
            )
        _reject_unknown(raw, {"version", "warehouse", "defaults", "namespaces"}, "<root>")
        # Parsed alone first, for its errors; then resolved against the built-in,
        # so `defaults` holds the values a table inherits, while `block` keeps
        # only what the file wrote -- an absent `defaults` writes nothing.
        written = _settings_from_dict(raw.get("defaults", {}), "defaults").as_block()
        defaults = replace(
            _settings_from_dict(_merge(_BUILT_IN_BLOCK, written), "defaults"), block=written
        )
        namespaces = {}
        for namespace, block in (
            _value(raw, "namespaces", "<root>", expect=dict, nullable=True) or {}
        ).items():
            where = f"namespaces.{namespace}"
            _reject_unknown(block, {"tables"}, where)
            namespaces[namespace] = NamespaceSettings(
                tables={
                    name: _settings_from_dict(settings, f"{where}.tables.{name}")
                    for name, settings in (
                        _value(block, "tables", where, expect=dict, nullable=True) or {}
                    ).items()
                }
            )
        return cls(
            version=_value(raw, "version", "<root>", expect=int, default=SPEC_VERSION),
            warehouse=_value(raw, "warehouse", "<root>", expect=str, default=""),
            defaults=defaults,
            namespaces=namespaces,
            source=source,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "warehouse": self.warehouse,
            "defaults": self.defaults.as_block(),
            "namespaces": {
                namespace: {
                    "tables": {
                        name: settings.as_block() for name, settings in sorted(block.tables.items())
                    }
                }
                for namespace, block in sorted(self.namespaces.items())
            },
        }

    def dump(self, path: str | Path, *, indent: int = 2) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), indent=indent) + "\n", encoding="utf-8")


# -- dict <-> dataclass ---------------------------------------------------


#: How to name what a malformed block actually held, in the file's own vocabulary
#: rather than Python's -- an author reading ``found a JSON object`` can look at
#: their file, where ``found a dict`` sends them to a language they may not use.
_JSON_TYPE_NAME = {
    type(None): "null",
    bool: "a boolean",
    int: "a number",
    float: "a number",
    str: "a string",
    list: "a list",
    dict: "a JSON object",
}


def _reject_unknown(raw: Any, allowed: set[str], where: str) -> None:
    """Refuse unknown keys, and refuse anything that is not a block at all.

    The second half is the one that had to be added. ``set(raw)`` is happy to
    iterate whatever it is handed, so ``"ordering": null`` escaped as a bare
    ``TypeError: 'NoneType' object is not iterable`` -- an unhandled crash with a
    traceback and no path, for an authoring mistake a templated file makes easily
    when it renders an absent block as ``null`` instead of omitting it. Measured
    across every nested block: thirteen of fourteen crashed (ZMBNI-53).

    ``TableConfigError`` is what a caller is told to catch -- the CLI maps it to
    exit 2 and the user guide documents it as *the* config failure -- so a
    ``TypeError`` here was a promise broken, not merely an ugly message.

    Checked for any non-mapping rather than for ``None`` alone: a list or a string
    did not crash, which is worse. ``set("abc")`` is a perfectly good set, so
    ``"ordering": "day"`` reported its own characters as unknown keys.
    """
    if not isinstance(raw, dict):
        found = _JSON_TYPE_NAME.get(type(raw), repr(raw))
        raise TableConfigError(
            f"{where}: expected a block of settings, found {found}. "
            f"Omit the key entirely to take the default; allowed keys here are "
            f"{sorted(allowed)}."
        )
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise TableConfigError(
            f"{where}: unknown key(s) {unknown}; allowed: {sorted(allowed)}. "
            "Keys are rejected rather than ignored so a typo cannot silently "
            "change a table's layout."
        )


def _value(
    raw: dict[str, Any],
    key: str,
    where: str,
    *,
    expect: type,
    default: Any = None,
    nullable: bool = False,
) -> Any:
    """Read ``key``, checking the JSON type of what is there.

    Every value in this file is read through here, because ``dict.get(key,
    default)`` was quietly doing three different wrong things (ZMBNI-53):

    * **A present ``null`` does not take the default.** ``get("enabled", True)``
      returns ``None`` for ``"enabled": null``, and ``None`` is falsy -- so
      ``"remove_orphan_files": {"enabled": null}`` loaded happily and *silently
      disabled reclamation*. `maintain()` then reports "disabled in the config",
      which is exactly the "silently reclaiming far less than expected" outcome
      :meth:`Retention.validate` warns about, reached by a typo.
    * **A wrong type reached arithmetic.** ``min_input_files: "day"`` and
      ``older_than_days: null`` escaped as ``TypeError: '<' not supported`` from
      inside ``validate()`` -- an unhandled crash, not the ``TableConfigError``
      the CLI maps to exit 2.
    * **A null slipped past a presence check.** ``sort[].column: null`` satisfied
      ``"column" not in key`` and produced a sort key on column ``None``.

    ``nullable`` says whether an explicit ``null`` is a legal spelling of "unset".
    It is true only where a ``None`` *means* something -- the settings documented
    as "leave the table property alone" -- and where the loader already coalesced
    a null to empty. `zamboni.tableconfig_schema` declares exactly those keys
    nullable, and a sweep over every field position asserts the two agree.
    """
    if key not in raw:
        return default
    value = raw[key]
    if value is None:
        if nullable:
            return None
        if default is None:
            # No default to fall back on, so "omit it" is the wrong advice --
            # `sort[].column` is required and omitting it is a different error.
            raise TableConfigError(
                f"{where}.{key}: null is not a value here; this key needs {_EXPECTED_NAME[expect]}."
            )
        raise TableConfigError(
            f"{where}.{key}: null is not a value here. Omit the key to take the "
            f"default ({default!r}); a null would otherwise read as "
            f"{'off' if expect is bool else 'unset'} without saying so."
        )
    # `bool` is a subclass of `int`, so an unguarded isinstance would accept
    # `min_input_files: true` as the integer 1 -- a config that means nothing
    # being read as one that means something.
    if isinstance(value, expect) and not (expect is not bool and isinstance(value, bool)):
        return value
    raise TableConfigError(
        f"{where}.{key}: expected {_EXPECTED_NAME[expect]}, found "
        f"{_JSON_TYPE_NAME.get(type(value), repr(value))}."
    )


#: What each expected Python type is called in the file's own vocabulary.
_EXPECTED_NAME = {
    bool: "true or false",
    int: "a number",
    str: "a string",
    list: "a list",
    dict: "a JSON object",
}


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
            "skip_partitions_newer_than_windows",
            "description",
        },
        where,
    )
    partition = tuple(
        _partition_field_from_dict(pf, f"{where}.partition[{i}]")
        for i, pf in enumerate(_value(raw, "partition", where, expect=list, nullable=True) or ())
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
        target_file_size_bytes=_value(
            raw, "target_file_size_bytes", where, expect=int, nullable=True
        ),
        min_input_files=_value(raw, "min_input_files", where, expect=int, default=2),
        skip_partitions_newer_than_windows=_value(
            raw,
            "skip_partitions_newer_than_windows",
            where,
            expect=int,
            nullable=True,
            default=TableSettings.skip_partitions_newer_than_windows,
        ),
        description=_value(raw, "description", where, expect=str, nullable=True),
        block=copy.deepcopy(raw),
    )


def _partition_field_from_dict(raw: dict[str, Any], where: str) -> PartitionField:
    _reject_unknown(raw, {"column", "transform", "num_buckets", "width", "name"}, where)
    if "column" not in raw:
        raise TableConfigError(f"{where}: 'column' is required")
    return PartitionField(
        column=_value(raw, "column", where, expect=str),
        transform=_value(raw, "transform", where, expect=str, default="identity"),
        num_buckets=_value(raw, "num_buckets", where, expect=int, nullable=True),
        width=_value(raw, "width", where, expect=int, nullable=True),
        name=_value(raw, "name", where, expect=str, nullable=True),
    )


def _evolution_from_dict(raw: dict[str, Any], where: str) -> PartitionEvolution:
    _reject_unknown(raw, {"enabled", "rules"}, where)
    rules = []
    for i, rule in enumerate(_value(raw, "rules", where, expect=list, nullable=True) or ()):
        _reject_unknown(rule, {"from", "to", "older_than_days"}, f"{where}.rules[{i}]")
        missing = {"from", "to", "older_than_days"} - set(rule)
        if missing:
            raise TableConfigError(f"{where}.rules[{i}]: missing {sorted(missing)}")
        rules.append(
            EvolutionRule(
                from_transform=_value(rule, "from", f"{where}.rules[{i}]", expect=str),
                to_transform=_value(rule, "to", f"{where}.rules[{i}]", expect=str),
                older_than_days=_value(rule, "older_than_days", f"{where}.rules[{i}]", expect=int),
            )
        )
    return PartitionEvolution(
        enabled=_value(raw, "enabled", where, expect=bool, default=True), rules=tuple(rules)
    )


def _ordering_from_dict(raw: dict[str, Any], where: str) -> Ordering:
    _reject_unknown(raw, {"mode", "sort", "zorder"}, where)
    sort = []
    for i, key in enumerate(_value(raw, "sort", where, expect=list, nullable=True) or ()):
        _reject_unknown(key, {"column", "direction", "nulls"}, f"{where}.sort[{i}]")
        if "column" not in key:
            raise TableConfigError(f"{where}.sort[{i}]: 'column' is required")
        sort.append(
            SortKey(
                column=_value(key, "column", f"{where}.sort[{i}]", expect=str),
                direction=_value(key, "direction", f"{where}.sort[{i}]", expect=str, default="asc"),
                nulls=_value(key, "nulls", f"{where}.sort[{i}]", expect=str, default="last"),
            )
        )
    zorder = None
    if "zorder" in raw:
        _reject_unknown(raw["zorder"], {"columns", "precision_bits"}, f"{where}.zorder")
        zorder = ZOrder(
            columns=tuple(
                _value(raw["zorder"], "columns", f"{where}.zorder", expect=list, nullable=True)
                or ()
            ),
            precision_bits=_value(
                raw["zorder"], "precision_bits", f"{where}.zorder", expect=int, default=16
            ),
        )
    return Ordering(
        mode=_value(raw, "mode", where, expect=str, default="none"),
        sort=tuple(sort),
        zorder=zorder,
    )


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
        at = f"{where}.expire_snapshots"
        expire = ExpireSnapshotsSettings(
            enabled=_value(block, "enabled", at, expect=bool, default=True),
            max_snapshot_age_days=_value(
                block, "max_snapshot_age_days", at, expect=int, nullable=True
            ),
            min_snapshots_to_keep=_value(
                block, "min_snapshots_to_keep", at, expect=int, nullable=True
            ),
            max_ref_age_days=_value(block, "max_ref_age_days", at, expect=int, nullable=True),
        )

    orphans = RemoveOrphanFilesSettings()
    if "remove_orphan_files" in raw:
        block = raw["remove_orphan_files"]
        _reject_unknown(block, {"enabled", "older_than_days"}, f"{where}.remove_orphan_files")
        at = f"{where}.remove_orphan_files"
        orphans = RemoveOrphanFilesSettings(
            enabled=_value(block, "enabled", at, expect=bool, default=True),
            older_than_days=_value(block, "older_than_days", at, expect=int, default=3),
        )

    dangling = RemoveDanglingDeletesSettings()
    if "remove_dangling_deletes" in raw:
        block = raw["remove_dangling_deletes"]
        _reject_unknown(block, {"enabled"}, f"{where}.remove_dangling_deletes")
        dangling = RemoveDanglingDeletesSettings(
            enabled=_value(
                block, "enabled", f"{where}.remove_dangling_deletes", expect=bool, default=True
            )
        )

    rewrite = RewriteManifestsSettings()
    if "rewrite_manifests" in raw:
        block = raw["rewrite_manifests"]
        _reject_unknown(block, {"enabled", "min_input_manifests"}, f"{where}.rewrite_manifests")
        at = f"{where}.rewrite_manifests"
        rewrite = RewriteManifestsSettings(
            enabled=_value(block, "enabled", at, expect=bool, default=True),
            min_input_manifests=_value(block, "min_input_manifests", at, expect=int, default=2),
        )

    metadata = MetadataSettings()
    if "metadata" in raw:
        block = raw["metadata"]
        _reject_unknown(
            block, {"previous_versions_max", "delete_after_commit"}, f"{where}.metadata"
        )
        at = f"{where}.metadata"
        metadata = MetadataSettings(
            previous_versions_max=_value(
                block, "previous_versions_max", at, expect=int, nullable=True
            ),
            delete_after_commit=_value(
                block, "delete_after_commit", at, expect=bool, nullable=True
            ),
        )

    return Retention(
        expire_snapshots=expire,
        remove_orphan_files=orphans,
        remove_dangling_deletes=dangling,
        rewrite_manifests=rewrite,
        metadata=metadata,
    )


def _full_block(s: TableSettings) -> dict[str, Any]:
    """Every key, as the file would spell it -- the block for settings built in
    code, which count as setting everything. Unset values are written as
    ``null`` rather than left out, because a key left out would inherit."""
    out: dict[str, Any] = {
        "description": s.description,
        "partition": [_partition_field_to_dict(pf) for pf in s.partition],
        "partition_evolution": {
            "enabled": s.partition_evolution.enabled,
            "rules": [
                {
                    "from": r.from_transform,
                    "to": r.to_transform,
                    "older_than_days": r.older_than_days,
                }
                for r in s.partition_evolution.rules
            ],
        },
        "ordering": _ordering_to_dict(s.ordering),
        "retention": {
            "expire_snapshots": {
                "enabled": s.retention.expire_snapshots.enabled,
                "max_snapshot_age_days": s.retention.expire_snapshots.max_snapshot_age_days,
                "min_snapshots_to_keep": s.retention.expire_snapshots.min_snapshots_to_keep,
                "max_ref_age_days": s.retention.expire_snapshots.max_ref_age_days,
            },
            "remove_orphan_files": {
                "enabled": s.retention.remove_orphan_files.enabled,
                "older_than_days": s.retention.remove_orphan_files.older_than_days,
            },
            "remove_dangling_deletes": {"enabled": s.retention.remove_dangling_deletes.enabled},
            "rewrite_manifests": {
                "enabled": s.retention.rewrite_manifests.enabled,
                "min_input_manifests": s.retention.rewrite_manifests.min_input_manifests,
            },
            "metadata": {
                "previous_versions_max": s.retention.metadata.previous_versions_max,
                "delete_after_commit": s.retention.metadata.delete_after_commit,
            },
        },
        "target_file_size_bytes": s.target_file_size_bytes,
        "min_input_files": s.min_input_files,
        "skip_partitions_newer_than_windows": s.skip_partitions_newer_than_windows,
    }
    return out


def _ordering_to_dict(o: Ordering) -> dict[str, Any]:
    out: dict[str, Any] = {"mode": o.mode}
    if o.sort:
        out["sort"] = [
            {"column": k.column, "direction": k.direction, "nulls": k.nulls} for k in o.sort
        ]
    if o.zorder is not None:
        out["zorder"] = {
            "columns": list(o.zorder.columns),
            "precision_bits": o.zorder.precision_bits,
        }
    return out


#: The built-in settings as a complete block: the base every merge starts from,
#: so a key neither the table nor ``defaults`` writes takes the built-in value
#: (`DEFAULT_SETTINGS`) rather than whatever the parser assumes for a missing key.
_BUILT_IN_BLOCK = _full_block(DEFAULT_SETTINGS)


def _partition_field_to_dict(pf: PartitionField) -> dict[str, Any]:
    out: dict[str, Any] = {"column": pf.column, "transform": pf.transform}
    if pf.num_buckets is not None:
        out["num_buckets"] = pf.num_buckets
    if pf.width is not None:
        out["width"] = pf.width
    if pf.name:
        out["name"] = pf.name
    return out
