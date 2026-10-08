# SPDX-License-Identifier: Apache-2.0
"""Iceberg table maintenance without Trino or Spark.

Compaction, ordering and partition evolution; snapshot expiry, orphan-file
removal, dangling-delete removal, manifest regrouping and metadata retention.
Format version 2 throughout: V1 is refused, V3 is metadata-only.

What a release of this is allowed to change is in docs/releasing.md. For a tool
whose job includes deleting files, a changed default is a breaking change even
when no signature moved -- so the contract is written down rather than implied.
"""

import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - for type checkers and editors only
    from .backends.base import RewriteBackend, RewriteContext, RewriteOutput
    from .capabilities import PyIcebergCapabilities, detect
    from .committer import ConcurrentModification, ReplaceCommitter, UnsupportedPyIceberg
    from .compactor import CompactionBlocked, CompactionResult, TableCompactor
    from .config import CompactionConfig, MemoryMode, config_from_table_settings
    from .deletes import DanglingDeleteCleaner
    from .expire import RetentionPolicy, SnapshotExpirer
    from .fleet import CronSchedule, FleetConfig, FleetConfigError, FleetWarehouse
    from .health import TableHealth, Watermark, maintenance_watermark, table_health
    from .maintainers import (
        EngineConfigProblem,
        LayoutFeature,
        Maintainer,
        MaintainerCapabilities,
        MaintenanceRequest,
        Operation,
        OperationSupport,
        PreviewUnavailable,
        Support,
        UnsupportedOperation,
        engines_lacking,
    )
    from .maintainers import available as available_engines
    from .maintainers import get as get_maintainer
    from .maintenance import (
        RUNBOOK_ORDER,
        MaintenanceReport,
        Outcome,
        RunCounters,
        maintain,
        validate_policy,
    )
    from .manifests import ManifestRewriter
    from .metrics import (
        CommitReport,
        CounterResult,
        NoCommitReport,
        TimerResult,
        commit_reports,
        no_commit_metrics,
        no_commit_report,
    )
    from .orphans import OrphanCleaner
    from .planner import CompactionPlan, CompactionPlanner, FileGroup
    from .profile import Finding, Severity, TableProfile, profile_table
    from .reachable import reachable_files
    from .reporters import (
        CollectingReporter,
        LoggingReporter,
        MetricsReporter,
        MultiReporter,
        NoopReporter,
        RestMetricsReporter,
        reporter_for,
    )
    from .runlog import FleetSummary, summarise_logs
    from .session import AzureSettings, CatalogSession, GCSSettings, S3Settings
    from .settings import Profile
    from .settings import resolve as resolve_settings
    from .tableconfig import Retention, TableConfig, TableConfigError, TableSettings
    from .tableconfig_schema import load_schema as get_table_config_spec

#: Every public name, and where it lives. Loaded on first use, not at import
#: (ZMBNI-154): importing this package used to import the whole engine --
#: PyIceberg, DuckDB, Arrow, the compaction backends -- which made every
#: `zamboni` command, `service-status --probe` included, pay ~1.2 s before
#: parsing an argument. `from zamboni import X` and `zamboni.X` work exactly as
#: before; a name is imported the first time it is asked for.
_LAZY: dict[str, tuple[str, str]] = {
    "AzureSettings": (".session", "AzureSettings"),
    "CatalogSession": (".session", "CatalogSession"),
    "CollectingReporter": (".reporters", "CollectingReporter"),
    "CommitReport": (".metrics", "CommitReport"),
    "CompactionBlocked": (".compactor", "CompactionBlocked"),
    "CompactionConfig": (".config", "CompactionConfig"),
    "CompactionPlan": (".planner", "CompactionPlan"),
    "CompactionPlanner": (".planner", "CompactionPlanner"),
    "CompactionResult": (".compactor", "CompactionResult"),
    "ConcurrentModification": (".committer", "ConcurrentModification"),
    "CounterResult": (".metrics", "CounterResult"),
    "CronSchedule": (".fleet", "CronSchedule"),
    "DanglingDeleteCleaner": (".deletes", "DanglingDeleteCleaner"),
    "EngineConfigProblem": (".maintainers", "EngineConfigProblem"),
    "FileGroup": (".planner", "FileGroup"),
    "Finding": (".profile", "Finding"),
    "FleetConfig": (".fleet", "FleetConfig"),
    "FleetConfigError": (".fleet", "FleetConfigError"),
    "FleetSummary": (".runlog", "FleetSummary"),
    "FleetWarehouse": (".fleet", "FleetWarehouse"),
    "GCSSettings": (".session", "GCSSettings"),
    "LayoutFeature": (".maintainers", "LayoutFeature"),
    "LoggingReporter": (".reporters", "LoggingReporter"),
    "Maintainer": (".maintainers", "Maintainer"),
    "MaintainerCapabilities": (".maintainers", "MaintainerCapabilities"),
    "MaintenanceReport": (".maintenance", "MaintenanceReport"),
    "MaintenanceRequest": (".maintainers", "MaintenanceRequest"),
    "ManifestRewriter": (".manifests", "ManifestRewriter"),
    "MemoryMode": (".config", "MemoryMode"),
    "MetricsReporter": (".reporters", "MetricsReporter"),
    "MultiReporter": (".reporters", "MultiReporter"),
    "NoCommitReport": (".metrics", "NoCommitReport"),
    "NoopReporter": (".reporters", "NoopReporter"),
    "Operation": (".maintainers", "Operation"),
    "OperationSupport": (".maintainers", "OperationSupport"),
    "OrphanCleaner": (".orphans", "OrphanCleaner"),
    "Outcome": (".maintenance", "Outcome"),
    "PreviewUnavailable": (".maintainers", "PreviewUnavailable"),
    "Profile": (".settings", "Profile"),
    "PyIcebergCapabilities": (".capabilities", "PyIcebergCapabilities"),
    "RUNBOOK_ORDER": (".maintenance", "RUNBOOK_ORDER"),
    "ReplaceCommitter": (".committer", "ReplaceCommitter"),
    "RestMetricsReporter": (".reporters", "RestMetricsReporter"),
    "Retention": (".tableconfig", "Retention"),
    "RetentionPolicy": (".expire", "RetentionPolicy"),
    "RewriteBackend": (".backends.base", "RewriteBackend"),
    "RewriteContext": (".backends.base", "RewriteContext"),
    "RewriteOutput": (".backends.base", "RewriteOutput"),
    "RunCounters": (".maintenance", "RunCounters"),
    "S3Settings": (".session", "S3Settings"),
    "Severity": (".profile", "Severity"),
    "SnapshotExpirer": (".expire", "SnapshotExpirer"),
    "Support": (".maintainers", "Support"),
    "TableCompactor": (".compactor", "TableCompactor"),
    "TableConfig": (".tableconfig", "TableConfig"),
    "TableConfigError": (".tableconfig", "TableConfigError"),
    "TableHealth": (".health", "TableHealth"),
    "TableProfile": (".profile", "TableProfile"),
    "TableSettings": (".tableconfig", "TableSettings"),
    "TimerResult": (".metrics", "TimerResult"),
    "UnsupportedOperation": (".maintainers", "UnsupportedOperation"),
    "UnsupportedPyIceberg": (".committer", "UnsupportedPyIceberg"),
    "Watermark": (".health", "Watermark"),
    "available_engines": (".maintainers", "available"),
    "commit_reports": (".metrics", "commit_reports"),
    "config_from_table_settings": (".config", "config_from_table_settings"),
    "detect": (".capabilities", "detect"),
    "engines_lacking": (".maintainers", "engines_lacking"),
    "get_maintainer": (".maintainers", "get"),
    "get_table_config_spec": (".tableconfig_schema", "load_schema"),
    "maintain": (".maintenance", "maintain"),
    "maintenance_watermark": (".health", "maintenance_watermark"),
    "no_commit_metrics": (".metrics", "no_commit_metrics"),
    "no_commit_report": (".metrics", "no_commit_report"),
    "profile_table": (".profile", "profile_table"),
    "reachable_files": (".reachable", "reachable_files"),
    "reporter_for": (".reporters", "reporter_for"),
    "resolve_settings": (".settings", "resolve"),
    "summarise_logs": (".runlog", "summarise_logs"),
    "table_health": (".health", "table_health"),
    "validate_policy": (".maintenance", "validate_policy"),
}


def __getattr__(name: str) -> object:
    """Load a public name, or a submodule, on first access (PEP 562)."""
    import importlib

    if name in _LAZY:
        module, attribute = _LAZY[name]
        value = getattr(importlib.import_module(module, __name__), attribute)
    else:
        # Submodules were attributes of the package as a side effect of the
        # eager imports (`import zamboni; zamboni.session`); keep that working.
        try:
            value = importlib.import_module(f".{name}", __name__)
        except ModuleNotFoundError as exc:
            if exc.name != f"{__name__}.{name}":
                raise
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY})


# Read from the installed distribution rather than repeated as a literal here.
# pyproject.toml is the single source of truth, so `zamboni --version` cannot
# disagree with the wheel it came from -- the failure mode of a hand-maintained
# __version__ is that it goes stale precisely when it matters, in a bug report.
try:
    # The *distribution* name, which is not the import name: `zamboni` on PyPI
    # belongs to an unrelated project. Getting this wrong fails soft --
    # PackageNotFoundError below reports "0+unknown" -- so it would degrade
    # `zamboni --version` silently, in precisely the situation where a version
    # number is the thing being asked for. Pinned by test_version.py.
    __version__ = _distribution_version("iceberg-zamboni")
except PackageNotFoundError:  # pragma: no cover - importable but not installed
    __version__ = "0+unknown"


def versions() -> dict[str, str]:
    """The three versions that identify a run, machine-readable.

    The same three :func:`version_banner` prints, and the reason is its
    docstring: which operations this tool will even attempt is decided by
    probing the installed PyIceberg, so "zamboni 0.5.1" alone does not identify
    the behaviour being reported -- or, in a series of run records, explain why
    a figure moved between two nights.

    Two renderings of one fact, like every ``describe()``/``as_dict()`` pair in
    this package: prose for a person, a mapping for whatever collects it.
    """
    try:
        pyiceberg = _distribution_version("pyiceberg")
    except PackageNotFoundError:  # pragma: no cover - a hard dependency
        pyiceberg = "not installed"

    return {
        "zamboni": __version__,
        "pyiceberg": pyiceberg,
        "python": ".".join(str(part) for part in sys.version_info[:3]),
    }


def version_banner() -> str:
    """Three versions, because one of them does not explain a bug report.

    Which operations this tool will even attempt is decided by probing the
    installed PyIceberg (see ``capabilities.py``), so "zamboni 0.1.0" alone does
    not identify the behaviour someone is reporting: the same zamboni refuses
    equality deletes on one PyIceberg and reads them on another.

    Python is in there because it genuinely varies. The package declares
    ``>=3.11``, CI runs the suite on 3.11 and 3.13, and the executables in
    ``bin/`` pin ``==3.13.*`` -- so "which Python" is a real question with three
    plausible answers rather than a constant worth omitting.

    ``importlib.metadata`` reads metadata without importing either package, so
    this is cheap enough for argparse to build on every invocation.
    """
    v = versions()
    return f"zamboni {v['zamboni']} (pyiceberg {v['pyiceberg']}, python {v['python']})"


#: The supported API. Everything here is covered by the compatibility promise in
#: docs/releasing.md; anything reachable but absent from this list is internal
#: and may move in a patch release.
#:
#: This was compaction-only until ZMBNI-915 -- `TableCompactor` and its
#: config, and nothing for the other five operations. An application that
#: wanted to expire snapshots had to import `zamboni.expire`, which is exactly
#: the kind of private-path dependency a public surface exists to prevent. The
#: engine-neutral entry point for an integrator is `get_maintainer`; the
#: operation classes below are the local engine's own vocabulary and are
#: exported because `--engine local` is the default and its results carry
#: detail the generic `Reportable` does not.
__all__ = [
    "RUNBOOK_ORDER",
    "AzureSettings",
    "CatalogSession",
    "CollectingReporter",
    "CommitReport",
    "CompactionBlocked",
    "CompactionConfig",
    "CompactionPlan",
    "CompactionPlanner",
    "CompactionResult",
    "ConcurrentModification",
    "CounterResult",
    "CronSchedule",
    "DanglingDeleteCleaner",
    "EngineConfigProblem",
    "FileGroup",
    "Finding",
    "FleetConfig",
    "FleetConfigError",
    "FleetSummary",
    "FleetWarehouse",
    "GCSSettings",
    "LayoutFeature",
    "LoggingReporter",
    "Maintainer",
    "MaintainerCapabilities",
    "MaintenanceReport",
    "MaintenanceRequest",
    "ManifestRewriter",
    "MemoryMode",
    "MetricsReporter",
    "MultiReporter",
    "NoCommitReport",
    "NoopReporter",
    "Operation",
    "OperationSupport",
    "OrphanCleaner",
    "Outcome",
    "PreviewUnavailable",
    "Profile",
    "PyIcebergCapabilities",
    "ReplaceCommitter",
    "RestMetricsReporter",
    "Retention",
    "RetentionPolicy",
    "RewriteBackend",
    "RewriteContext",
    "RewriteOutput",
    "RunCounters",
    "S3Settings",
    "Severity",
    "SnapshotExpirer",
    "Support",
    "TableCompactor",
    "TableConfig",
    "TableConfigError",
    "TableHealth",
    "TableProfile",
    "TableSettings",
    "TimerResult",
    "UnsupportedOperation",
    "UnsupportedPyIceberg",
    "Watermark",
    "__version__",
    "available_engines",
    "commit_reports",
    "config_from_table_settings",
    "detect",
    "engines_lacking",
    "get_maintainer",
    "get_table_config_spec",
    "maintain",
    "maintenance_watermark",
    "no_commit_metrics",
    "no_commit_report",
    "profile_table",
    "reachable_files",
    "reporter_for",
    "resolve_settings",
    "summarise_logs",
    "table_health",
    "validate_policy",
    "version_banner",
    "versions",
]
