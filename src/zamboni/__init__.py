"""Iceberg table maintenance without Trino or Spark.

Phase 1 covers data-file compaction for copy-on-write tables and for
merge-on-read tables using position deletes or V3 deletion vectors.
"""

from .backends.base import RewriteBackend, RewriteContext, RewriteOutput
from .capabilities import PyIcebergCapabilities, detect
from .committer import ConcurrentModification, ReplaceCommitter, UnsupportedPyIceberg
from .compactor import CompactionBlocked, CompactionResult, TableCompactor
from .config import CompactionConfig, MemoryMode
from .planner import CompactionPlan, CompactionPlanner, FileGroup
from .profile import Finding, Severity, TableProfile, profile_table
from .session import CatalogSession, S3Settings

__all__ = [
    "CatalogSession",
    "CompactionBlocked",
    "CompactionConfig",
    "CompactionPlan",
    "CompactionPlanner",
    "CompactionResult",
    "ConcurrentModification",
    "FileGroup",
    "Finding",
    "MemoryMode",
    "PyIcebergCapabilities",
    "ReplaceCommitter",
    "RewriteBackend",
    "RewriteContext",
    "RewriteOutput",
    "S3Settings",
    "Severity",
    "TableCompactor",
    "TableProfile",
    "UnsupportedPyIceberg",
    "detect",
    "profile_table",
]
