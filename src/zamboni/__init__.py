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

from .backends.base import RewriteBackend, RewriteContext, RewriteOutput
from .capabilities import PyIcebergCapabilities, detect
from .committer import ConcurrentModification, ReplaceCommitter, UnsupportedPyIceberg
from .compactor import CompactionBlocked, CompactionResult, TableCompactor
from .config import CompactionConfig, MemoryMode, config_from_table_settings
from .deletes import DanglingDeleteCleaner
from .expire import RetentionPolicy, SnapshotExpirer
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
from .orphans import OrphanCleaner
from .planner import CompactionPlan, CompactionPlanner, FileGroup
from .profile import Finding, Severity, TableProfile, profile_table
from .reachable import reachable_files
from .runlog import FleetSummary, summarise_logs
from .session import AzureSettings, CatalogSession, GCSSettings, S3Settings
from .settings import Profile
from .settings import resolve as resolve_settings
from .tableconfig import Retention, TableConfig, TableConfigError, TableSettings
from .tableconfig_schema import load_schema as get_table_config_spec

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
    "CompactionBlocked",
    "CompactionConfig",
    "CompactionPlan",
    "CompactionPlanner",
    "CompactionResult",
    "ConcurrentModification",
    "DanglingDeleteCleaner",
    "EngineConfigProblem",
    "FileGroup",
    "Finding",
    "FleetSummary",
    "GCSSettings",
    "LayoutFeature",
    "Maintainer",
    "MaintainerCapabilities",
    "MaintenanceReport",
    "MaintenanceRequest",
    "ManifestRewriter",
    "MemoryMode",
    "Operation",
    "OperationSupport",
    "OrphanCleaner",
    "Outcome",
    "PreviewUnavailable",
    "Profile",
    "PyIcebergCapabilities",
    "ReplaceCommitter",
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
    "UnsupportedOperation",
    "UnsupportedPyIceberg",
    "Watermark",
    "__version__",
    "available_engines",
    "config_from_table_settings",
    "detect",
    "engines_lacking",
    "get_maintainer",
    "get_table_config_spec",
    "maintain",
    "maintenance_watermark",
    "profile_table",
    "reachable_files",
    "resolve_settings",
    "summarise_logs",
    "table_health",
    "validate_policy",
    "version_banner",
    "versions",
]
