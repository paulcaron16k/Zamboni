"""The user-facing compaction entry point."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace

from pyiceberg.manifest import DataFile

from .backends.base import RewriteBackend, RewriteContext
from .backends.duckdb_arrow import DuckDBArrowBackend
from .committer import ReplaceCommitter, assert_supported_pyiceberg, cleanup_orphans
from .config import (
    DEFAULT_TARGET_FILE_SIZE_BYTES,
    CompactionConfig,
    config_from_table_settings,
)
from .evolution import EvolutionPlan, MultiSpecReplaceFiles, ensure_specs, plan_evolution
from .planner import CompactionPlan, CompactionPlanner, FileGroup
from .profile import Severity, TableProfile, profile_table
from .session import CatalogSession
from .tableconfig import TableConfig, TableSettings

logger = logging.getLogger(__name__)


class CompactionBlocked(RuntimeError):
    """The table cannot be compacted safely."""


@dataclass
class GroupResult:
    group: str
    rewritten_data_files: int
    added_data_files: int
    rewritten_bytes: int
    added_bytes: int
    snapshot_id: int | None


@dataclass
class CompactionResult:
    """Totals for a run.

    The first three counters mirror the columns returned by duckdb-iceberg's
    native ``iceberg_rewrite_data_files``, so results are directly comparable if
    you build that extension as an oracle.
    """

    identifier: str
    rewritten_data_files: int = 0
    added_data_files: int = 0
    rewritten_bytes: int = 0
    added_bytes: int = 0
    groups: list[GroupResult] = field(default_factory=list)
    evolved: list[GroupResult] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    dangling_delete_files: int = 0
    warnings: list[str] = field(default_factory=list)

    def describe(self) -> str:
        # Count both kinds of work: reporting only compaction groups reads as
        # "0 groups" on a run whose rewriting was all partition evolution.
        work = []
        if self.groups:
            work.append(f"{len(self.groups)} compaction group(s)")
        if self.evolved:
            work.append(f"{len(self.evolved)} evolution group(s)")
        across = " and ".join(work) if work else "no groups"

        lines = [
            (
                f"{self.identifier}: rewrote {self.rewritten_data_files} file(s) "
                f"({self.rewritten_bytes} bytes) into {self.added_data_files} "
                f"({self.added_bytes} bytes) across {across}"
            )
        ]
        lines += [f"  {g.group}" for g in self.evolved]
        if self.dangling_delete_files:
            lines.append(
                f"  {self.dangling_delete_files} delete file(s) now dangling; "
                "remove-dangling-deletes will drop them"
            )
        lines += [f"  skipped {part}: {reason}" for part, reason in self.skipped]
        lines += [f"  warning: {w}" for w in self.warnings]
        return "\n".join(lines)


class TableCompactor:
    """Compact one Iceberg table's data files.

    Each rewrite group is committed as its own ``replace`` snapshot. That keeps
    a partial failure meaningful -- earlier partitions stay compacted -- at the
    cost of more snapshots than a single all-or-nothing commit would produce.
    """

    def __init__(
        self,
        session: CatalogSession,
        identifier: str,
        config: CompactionConfig | None = None,
        backend: RewriteBackend | None = None,
        settings: TableSettings | None = None,
    ) -> None:
        self._session = session
        self._identifier = identifier
        self._config = config or CompactionConfig()
        self._backend = backend or DuckDBArrowBackend(session.con)
        self._settings = settings

    @classmethod
    def from_table_config(
        cls,
        session: CatalogSession,
        identifier: str,
        table_config: TableConfig,
        base: CompactionConfig | None = None,
        backend: RewriteBackend | None = None,
    ) -> TableCompactor:
        """Build a compactor from a declarative ``table-config.json`` entry.

        Layout intent comes from the config; ``base`` keeps operational choices
        (memory mode, temp directory, branch) with the operator.
        """
        settings = table_config.for_table(identifier)
        return cls(
            session,
            identifier,
            config=config_from_table_settings(settings, base),
            backend=backend,
            settings=settings,
        )

    # -- inspection ------------------------------------------------------

    def describe(self) -> TableProfile:
        """Profile the table without changing anything."""
        return profile_table(self._session.table(self._identifier))

    def plan(self, profile: TableProfile | None = None) -> CompactionPlan:
        tbl = self._session.table(self._identifier)
        profile = profile or profile_table(tbl)
        return CompactionPlanner(self._config).plan(tbl, profile)

    # -- execution -------------------------------------------------------

    def execute(self, *, dry_run: bool = False) -> CompactionResult:
        assert_supported_pyiceberg()

        tbl = self._session.table(self._identifier)
        profile = profile_table(tbl)
        result = CompactionResult(identifier=profile.identifier)
        result.warnings = [str(f) for f in profile.warnings]
        result.dangling_delete_files = profile.position_delete_files

        blockers = profile.blockers
        if profile.position_delete_files and self._config.dangling_delete_policy == "block":
            blockers = blockers + [
                f
                for f in profile.findings
                if f.code == "dangling-delete-files" and f.severity == Severity.WARNING
            ]
        if blockers:
            detail = "\n".join(f"  {b}" for b in blockers)
            raise CompactionBlocked(f"cannot compact {profile.identifier}:\n{detail}")

        ok, reason = self._backend.supports(RewriteContext(table=tbl, config=self._config))
        if not ok:
            raise CompactionBlocked(
                f"backend {self._backend.name} cannot handle this table: {reason}"
            )

        # Evolution runs first. It condenses aged partitions into coarser ones,
        # which removes those files from the ordinary plan -- running it second
        # would compact a day's files only to immediately rewrite them again as
        # part of a month.
        evolution_plan = self._plan_evolution(tbl, profile)
        plan = CompactionPlanner(self._config).plan(tbl, profile)
        # Files claimed by evolution must not also be compacted in place.
        claimed = {f.path for g in evolution_plan.groups for f in g.files}
        if claimed:
            plan = _without_files(plan, claimed)

        result.skipped = list(plan.skipped) + list(evolution_plan.skipped)
        if dry_run:
            logger.info("dry run:\n%s\n%s", evolution_plan.describe(), plan.describe())
            return result
        if plan.is_empty and evolution_plan.is_empty:
            return result

        committer = ReplaceCommitter(
            branch=self._config.branch,
            snapshot_operation=self._config.snapshot_operation,
        )

        if evolution_plan.groups:
            tbl = self._run_evolution(tbl, evolution_plan, result)

        for group in plan.groups:
            group_result = self._run_group(tbl, group, committer)
            result.groups.append(group_result)
            result.rewritten_data_files += group_result.rewritten_data_files
            result.added_data_files += group_result.added_data_files
            result.rewritten_bytes += group_result.rewritten_bytes
            result.added_bytes += group_result.added_bytes
            # Every commit moves the table forward; the next group must plan
            # against the snapshot the previous commit produced.
            tbl = self._session.table(self._identifier)

        return result

    # -- partition evolution ---------------------------------------------

    def _plan_evolution(self, tbl, profile: TableProfile) -> EvolutionPlan:
        if self._settings is None:
            return EvolutionPlan(groups=[], skipped=[], required_specs={})
        return plan_evolution(tbl, self._settings, profile.live_files)

    def _run_evolution(self, tbl, plan: EvolutionPlan, result: CompactionResult):
        """Rewrite each aged group under the coarser spec and commit it."""
        needed = {g.target_spec_id: plan.required_specs[g.target_spec_id] for g in plan.groups}
        tbl = ensure_specs(tbl, needed)
        tbl = self._session.table(self._identifier)

        for group in plan.groups:
            file_group = FileGroup(
                spec_id=group.files[0].spec_id,
                partition=group.files[0].partition,
                files=group.files,
                target_file_size_bytes=self._config.target_file_size_bytes
                or DEFAULT_TARGET_FILE_SIZE_BYTES,
            )
            # Writing under the coarse spec is what actually merges the days:
            # left on the table's day spec, the writer would split the group
            # right back into one file per day. It also has PyIceberg derive the
            # month partition value from the data rather than trusting our
            # arithmetic.
            ctx = RewriteContext(table=tbl, config=self._config, write_spec_id=group.target_spec_id)
            output = self._backend.rewrite(file_group, ctx)

            if output.written_rows != output.source_live_rows:
                cleanup_orphans(tbl, output.data_files)
                raise RuntimeError(
                    f"evolution of {group.label} produced {output.written_rows} rows but "
                    f"the source holds {output.source_live_rows}; nothing was committed"
                )

            committer = ReplaceCommitter(
                branch=self._config.branch,
                snapshot_operation=self._config.snapshot_operation,
                producer_cls=MultiSpecReplaceFiles,
                snapshot_properties={"zamboni.evolution": group.label},
            )
            try:
                outcome = committer.commit(
                    tbl,
                    expected_snapshot_id=tbl.metadata.current_snapshot_id,
                    removed=[f.data_file for f in group.files],
                    added=output.data_files,
                )
            except Exception:
                cleanup_orphans(tbl, output.data_files)
                raise

            result.evolved.append(
                GroupResult(
                    group=f"evolution {group.label}",
                    rewritten_data_files=outcome.removed_files,
                    added_data_files=outcome.added_files,
                    rewritten_bytes=outcome.removed_bytes,
                    added_bytes=outcome.added_bytes,
                    snapshot_id=outcome.snapshot_id,
                )
            )
            result.rewritten_data_files += outcome.removed_files
            result.added_data_files += outcome.added_files
            result.rewritten_bytes += outcome.removed_bytes
            result.added_bytes += outcome.added_bytes
            tbl = self._session.table(self._identifier)

        return tbl

    def _run_group(self, tbl, group: FileGroup, committer: ReplaceCommitter) -> GroupResult:
        ctx = RewriteContext(table=tbl, config=self._config)

        output = self._backend.rewrite(group, ctx)
        added: list[DataFile] = output.data_files

        # The rewrite must be row-preserving. Checking here, before the commit,
        # is what stops a bad read from ever reaching the table. The comparison
        # is against *live* rows, not the manifests' physical record_count --
        # those differ by exactly the deleted rows on a merge-on-read table.
        if output.written_rows != output.source_live_rows:
            cleanup_orphans(tbl, added)
            raise RuntimeError(
                f"rewrite of {group.describe()} produced {output.written_rows} rows but "
                f"the source files hold {output.source_live_rows} live rows; "
                "nothing was committed"
            )

        removed = [f.data_file for f in group.files]
        try:
            outcome = committer.commit(
                tbl,
                expected_snapshot_id=tbl.metadata.current_snapshot_id,
                removed=removed,
                added=added,
            )
        except Exception:
            cleanup_orphans(tbl, added)
            raise

        return GroupResult(
            group=group.describe(),
            rewritten_data_files=outcome.removed_files,
            added_data_files=outcome.added_files,
            rewritten_bytes=outcome.removed_bytes,
            added_bytes=outcome.added_bytes,
            snapshot_id=outcome.snapshot_id,
        )


def _without_files(plan: CompactionPlan, claimed: set[str]) -> CompactionPlan:
    """Drop files already claimed by partition evolution from a compaction plan."""
    groups = []
    for group in plan.groups:
        remaining = [f for f in group.files if f.path not in claimed]
        if len(remaining) >= 2:
            groups.append(replace(group, files=remaining))
    return replace(plan, groups=groups)
