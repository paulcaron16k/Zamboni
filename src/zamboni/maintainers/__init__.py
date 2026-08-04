"""The maintainer contract: one operation, several engines.

The six operations are Iceberg's, not Zamboni's. Trino and Spark implement most
of them already -- better in places (Spark drops dangling deletes per file; we
can only drop whole manifests) and worse in others (Trino cannot Z-order). This
module is the seam that lets an operator ask for "maintain this table" rather
than "maintain this table with PyIceberg".

**Why the seam is here and not lower.** Two alternatives were rejected in
docs/engine-comparison.md §5. Generating SQL -- ice-keeper's design -- is
unavailable to us: Zamboni manipulates metadata through PyIceberg and is the
only one of the three that is not a query engine. Driving manifest writers is
unavailable to them: Trino and Spark expose procedures. Operation-level is the
only level all three implement, so the seam is forced rather than chosen.

**The hard part is refusing to paper over the differences.** A common interface
invites accepting every option and quietly ignoring what a backend cannot do.
Everything below exists to make that impossible:

* :class:`Support` is three-valued, because "supported" is a lie for an
  operation that works with a named limitation.
* :class:`OperationSupport` *requires* a limitation string whenever support is
  not full -- an unexplained partial is rejected at construction.
* ``can_preview`` is per operation, not per engine: Spark's
  ``remove_orphan_files`` takes ``dry_run`` and no other Spark procedure does.
* ``invariants`` records what a run actually guarantees. "Both support
  remove-orphans" is true and misleading -- ours aborts on four checked
  conditions, a server-side procedure does its own thing.
* ``fulfilled_by`` exists because Spark removes dangling deletes as an option of
  compaction rather than as an operation of its own.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, ClassVar, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ..config import CompactionConfig
    from ..session import CatalogSession
    from ..tableconfig import Retention, TableConfig


class Operation(StrEnum):
    """The mutating operations. Values match the CLI verbs deliberately."""

    COMPACT = "compact"
    EXPIRE = "expire"
    REMOVE_ORPHANS = "remove-orphans"
    REMOVE_DANGLING_DELETES = "remove-dangling-deletes"
    REWRITE_MANIFESTS = "rewrite-manifests"
    APPLY_PROPERTIES = "apply-properties"


class Support(StrEnum):
    """Three-valued on purpose.

    A boolean forces every operation that works *with a caveat* to be recorded
    as either a lie or an absence. ``expire`` on Trino runs but cannot honour
    ``max_ref_age_days``; ``remove-dangling-deletes`` here runs but only drops
    whole manifests. Both are PARTIAL, and both must say why.
    """

    FULL = "full"
    PARTIAL = "partial"
    UNSUPPORTED = "unsupported"


class Reportable(Protocol):
    """What every operation returns: something that can describe itself."""

    def describe(self) -> str: ...


class UnsupportedOperation(RuntimeError):
    """The engine cannot perform this operation. Nothing was attempted."""


class PreviewUnavailable(RuntimeError):
    """The engine cannot preview this operation, and consent was not given."""


class EngineConfigProblem(ValueError):
    """The request is valid, but not for this engine. Raised at plan time."""


@dataclass(frozen=True)
class OperationSupport:
    operation: Operation
    support: Support
    #: Whether a run can be previewed without committing. Per operation, because
    #: Spark's ``remove_orphan_files`` has ``dry_run`` and its other procedures
    #: do not.
    can_preview: bool
    #: Required unless support is FULL. Refusing to construct an unexplained
    #: PARTIAL is the whole point: a caveat nobody wrote down becomes a caveat
    #: nobody knows about.
    limitations: tuple[str, ...] = ()
    #: What a run guarantees, beyond doing the thing. Ours abort rather than
    #: delete; a server-side procedure substitutes its own guarantees.
    invariants: tuple[str, ...] = ()
    #: Set when the engine achieves this operation as part of another one --
    #: Spark removes dangling deletes via an option on ``rewrite_data_files``.
    fulfilled_by: Operation | None = None

    def __post_init__(self) -> None:
        if self.support is not Support.FULL and not self.limitations:
            raise ValueError(
                f"{self.operation.value}: support is {self.support.value} but no "
                "limitation is given. An unexplained caveat is worse than none."
            )
        if self.support is Support.UNSUPPORTED and self.can_preview:
            raise ValueError(
                f"{self.operation.value}: an unsupported operation cannot be previewed."
            )

    @property
    def usable(self) -> bool:
        return self.support is not Support.UNSUPPORTED


@dataclass(frozen=True)
class MaintainerCapabilities:
    engine: str
    operations: dict[Operation, OperationSupport] = field(default_factory=dict)

    def __post_init__(self) -> None:
        missing = [op for op in Operation if op not in self.operations]
        if missing:
            # Silence is the failure mode this prevents: an operation nobody
            # declared reads as unsupported to a caller and as an oversight to
            # a reviewer, and they cannot tell which.
            raise ValueError(
                f"{self.engine}: no declared support for {', '.join(op.value for op in missing)}"
            )

    def of(self, operation: Operation) -> OperationSupport:
        return self.operations[operation]

    def can_preview(self, operation: Operation) -> bool:
        return self.of(operation).can_preview

    def describe(self) -> str:
        rows = [f"  engine: {self.engine}"]
        for operation in Operation:
            support = self.of(operation)
            preview = "previews" if support.can_preview else "cannot preview"
            line = f"  {operation.value:<24} {support.support.value:<12} {preview}"
            if support.fulfilled_by:
                line += f", via {support.fulfilled_by.value}"
            rows.append(line)
            rows.extend(f"      - {limitation}" for limitation in support.limitations)
        return "\n".join(rows)


@dataclass(frozen=True)
class MaintenanceRequest:
    """Engine-neutral inputs, translated by each maintainer.

    Deliberately the *declarative* settings rather than a
    :class:`~zamboni.expire.RetentionPolicy` or a Trino parameter list.
    Parameters translate, they do not pass through: Trino's ``retain_last`` is
    the spec's ``min-snapshots-to-keep`` under another name, and its
    ``retention_threshold`` has a server floor that can reject a value this
    object holds quite legitimately.
    """

    retention: Retention
    #: How a compaction run executes -- memory mode, workers, target size.
    compaction: CompactionConfig | None = None
    #: The declarative layout source. Kept as the parsed config rather than a
    #: built compactor, so a non-local engine can translate it to its own
    #: vocabulary instead of receiving ours.
    table_config: TableConfig | None = None
    #: CLI overrides, which beat the config file where they are set.
    max_snapshot_age_days: int | None = None
    min_snapshots_to_keep: int | None = None
    older_than_days: int | None = None
    min_input_manifests: int | None = None


class Maintainer(ABC):
    """One engine's implementation of the six operations."""

    #: Value accepted by ``--engine``.
    name: ClassVar[str]

    def __init__(self, session: CatalogSession, options: Mapping[str, str] | None = None) -> None:
        self._session = session
        #: Engine-specific connection settings from the CLI or environment.
        #: Deliberately a plain mapping: what Trino needs (host, port, catalog)
        #: and what Spark will need (a master URL, a JVM) have nothing in common,
        #: so a shared typed shape would be a union pretending to be a contract.
        self._options = dict(options or {})

    @classmethod
    @abstractmethod
    def capabilities(cls) -> MaintainerCapabilities:
        """What this engine can do. A classmethod on purpose: what an engine
        supports is a property of the engine, not of a connected instance, so
        ``zamboni engines`` can report it without a catalog."""

    @abstractmethod
    def execute(
        self,
        operation: Operation,
        table: str,
        *,
        request: MaintenanceRequest,
        dry_run: bool,
    ) -> Reportable: ...

    def validate(self, operation: Operation, request: MaintenanceRequest) -> tuple[str, ...]:
        """Problems that make this request invalid *for this engine*.

        Plan time, not commit time. A ``table-config.json`` that is perfectly
        valid can be unusable against a given engine -- Trino refuses a
        retention below its configured floor -- and finding that out from a
        server error part-way through a fleet run is the outcome this prevents.
        """
        return ()

    def check_supported(self, operation: Operation) -> OperationSupport:
        support = self.capabilities().of(operation)
        if not support.usable:
            raise UnsupportedOperation(
                f"{self.name} cannot {operation.value}: {'; '.join(support.limitations)}"
            )
        return support

    def check_consent(self, operation: Operation, *, yes: bool) -> None:
        """Uphold "without --yes, nothing is committed" on every engine.

        Where an engine cannot preview, the rule is kept by **refusing** rather
        than by executing or by printing a dry-run notice over an engine that is
        about to delete. Refusing commits nothing, so the rule survives intact
        and acquires no exception -- see docs/roadmap.md open question 2.
        """
        if yes or self.capabilities().can_preview(operation):
            return
        raise PreviewUnavailable(
            f"{self.name} cannot preview {operation.value}, so there is nothing to "
            "show without committing. Re-run with --yes to execute it, or use an "
            "engine that previews."
        )


_REGISTRY: dict[str, type[Maintainer]] = {}


def register(cls: type[Maintainer]) -> type[Maintainer]:
    _REGISTRY[cls.name] = cls
    return cls


def available() -> list[str]:
    return sorted(_REGISTRY)


def get(name: str) -> type[Maintainer]:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"unknown engine {name!r}; available: {', '.join(available())}") from None


def _load_builtins() -> None:
    """Import the built-ins so their ``@register`` decorator runs.

    Via ``importlib`` rather than ``from . import local``: each of them imports
    back from this module, so at this point the package is still initialising
    and a static attribute access on it is not something a type checker can
    honour.
    """
    import importlib

    for module in ("local", "trino", "spark"):
        importlib.import_module(f"{__name__}.{module}")


_load_builtins()

__all__ = [
    "EngineConfigProblem",
    "Maintainer",
    "MaintainerCapabilities",
    "MaintenanceRequest",
    "Operation",
    "OperationSupport",
    "PreviewUnavailable",
    "Reportable",
    "Support",
    "UnsupportedOperation",
    "available",
    "get",
    "register",
]
