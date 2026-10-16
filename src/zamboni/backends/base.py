# SPDX-License-Identifier: Apache-2.0
"""Rewrite backend interface.

A backend turns one :class:`~zamboni.planner.FileGroup` into a list of new
:class:`~pyiceberg.manifest.DataFile` objects. It does not commit -- the
committer owns that -- so a backend that fails leaves nothing but orphaned
Parquet files, which the committer then cleans up.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyiceberg.manifest import DataFile
from pyiceberg.table import Table

from ..config import CompactionConfig
from ..planner import FileGroup


class SpillLimitExceeded(RuntimeError):
    """A rewrite needed more spill than ``max_temp_directory_bytes`` allows.

    The cap exists so that this is what happens instead of the kubelet evicting
    the pod (ZMBNI-186), so it is a refusal for one table rather than a crash:
    `maintain()` reports it as exit 3, blocked under this host's limits, and goes
    on. Raised from inside the rewrite, before anything is committed; the
    committer's cleanup removes what the group had already written.

    Declared here, beside the seam, rather than in the DuckDB backend, so a
    caller can catch it without importing an engine.
    """


@dataclass(frozen=True)
class RewriteContext:
    table: Table
    config: CompactionConfig
    #: Partition spec the output should be written under, when it differs from
    #: the table default. Partition evolution sets this so the writer groups
    #: rows by the *coarse* partition -- otherwise it would split the group by
    #: the table's fine-grained default spec and merge nothing. It also means
    #: the partition value is computed from the data by PyIceberg rather than
    #: derived by hand.
    write_spec_id: int | None = None


@dataclass(frozen=True)
class RewriteOutput:
    """What a rewrite produced, and what it should have produced.

    ``source_live_rows`` is the number of *live* rows in the group's source
    files -- after delete files are applied. It is not
    ``sum(data_file.record_count)``: a manifest's ``record_count`` is the
    physical row count of the file, so on a merge-on-read table it overstates
    the live count by exactly the number of deleted rows. The backend computes
    it because only the backend has the scan tasks with their delete files
    attached.
    """

    data_files: list[DataFile]
    source_live_rows: int

    @property
    def written_rows(self) -> int:
        return sum(f.record_count for f in self.data_files)


class RewriteBackend(ABC):
    """Produces new data files for a rewrite group."""

    name: str = "abstract"

    @abstractmethod
    def rewrite(self, group: FileGroup, ctx: RewriteContext) -> RewriteOutput:
        """Read ``group``'s files and write replacement files.

        The returned files must together contain exactly the live rows of the
        group -- delete files applied, nothing added, nothing dropped.
        """

    def supports(self, ctx: RewriteContext) -> tuple[bool, str]:
        """Whether this backend can handle the table. Returns (ok, reason)."""
        return True, ""
