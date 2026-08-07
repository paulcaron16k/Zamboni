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
from .maintainers import (
    EngineConfigProblem,
    Maintainer,
    MaintainerCapabilities,
    MaintenanceRequest,
    Operation,
    OperationSupport,
    PreviewUnavailable,
    Support,
    UnsupportedOperation,
)
from .maintainers import available as available_engines
from .maintainers import get as get_maintainer
from .manifests import ManifestRewriter
from .orphans import OrphanCleaner
from .planner import CompactionPlan, CompactionPlanner, FileGroup
from .profile import Finding, Severity, TableProfile, profile_table
from .reachable import reachable_files
from .session import CatalogSession, S3Settings
from .settings import Profile
from .settings import resolve as resolve_settings
from .tableconfig import Retention, TableConfig, TableConfigError, TableSettings

# Read from the installed distribution rather than repeated as a literal here.
# pyproject.toml is the single source of truth, so `zamboni --version` cannot
# disagree with the wheel it came from -- the failure mode of a hand-maintained
# __version__ is that it goes stale precisely when it matters, in a bug report.
try:
    __version__ = _distribution_version("zamboni")
except PackageNotFoundError:  # pragma: no cover - importable but not installed
    __version__ = "0+unknown"


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
    try:
        pyiceberg = _distribution_version("pyiceberg")
    except PackageNotFoundError:  # pragma: no cover - a hard dependency
        pyiceberg = "not installed"

    python = ".".join(str(part) for part in sys.version_info[:3])
    return f"zamboni {__version__} (pyiceberg {pyiceberg}, python {python})"


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
    "Maintainer",
    "MaintainerCapabilities",
    "MaintenanceRequest",
    "ManifestRewriter",
    "MemoryMode",
    "Operation",
    "OperationSupport",
    "OrphanCleaner",
    "PreviewUnavailable",
    "Profile",
    "PyIcebergCapabilities",
    "ReplaceCommitter",
    "Retention",
    "RetentionPolicy",
    "RewriteBackend",
    "RewriteContext",
    "RewriteOutput",
    "S3Settings",
    "Severity",
    "SnapshotExpirer",
    "Support",
    "TableCompactor",
    "TableConfig",
    "TableConfigError",
    "TableProfile",
    "TableSettings",
    "UnsupportedOperation",
    "UnsupportedPyIceberg",
    "__version__",
    "available_engines",
    "config_from_table_settings",
    "detect",
    "get_maintainer",
    "profile_table",
    "reachable_files",
    "resolve_settings",
    "version_banner",
]
