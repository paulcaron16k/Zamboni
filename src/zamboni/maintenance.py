# SPDX-License-Identifier: Apache-2.0
"""One maintenance run: every operation, in order, over every table.

This is the loop `zamboni maintenance` runs, and it lives here rather than in
`cli.py` so an application gets the same one. It used to live only in the CLI,
and the user guide told integrators to write their own -- twenty-odd lines that
had to re-derive the operation order, the `fulfilled_by` skip, which exceptions
are refusals rather than failures, and when to stop. Every one of those is a
decision this package has already made and tested; asking a caller to make them
again is asking them to get one wrong.

The whole surface is :func:`maintain`::

    report = maintain(session, table_config="table-config.json", commit=True)
    print(report.describe())
    raise SystemExit(report.exit_code)

`report.exit_code` is the same number the CLI would have exited with, which is
what makes the two genuinely equivalent rather than merely similar.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyiceberg.exceptions import CommitFailedException, ValidationException

from .committer import ConcurrentModification, UnsupportedPyIceberg
from .compactor import CompactionBlocked
from .config import CompactionConfig, config_from_table_settings
from .expire import ExpiryAborted
from .maintainers import (
    EngineConfigProblem,
    Maintainer,
    MaintenanceRequest,
    Operation,
    PreviewUnavailable,
    Reportable,
    UnsupportedOperation,
)
from .maintainers import get as get_maintainer
from .orphans import OrphanCleanupAborted
from .session import CatalogSession, StorageCredentialsRequired
from .tableconfig import TableConfig, TableConfigError
from .workdir import WorkspaceUnavailable

logger = logging.getLogger(__name__)

#: Runbook order. Three of the five gaps between these are load-bearing -- see
#: docs/runbook-dev.md. Shared with `settings.DEFAULT_OPERATIONS`, which is the
#: profile's default, so the CLI and this module cannot drift apart on it.
RUNBOOK_ORDER: tuple[Operation, ...] = (
    Operation.COMPACT,
    Operation.APPLY_PROPERTIES,
    Operation.REMOVE_DANGLING_DELETES,
    Operation.REWRITE_MANIFESTS,
    Operation.EXPIRE,
    Operation.REMOVE_ORPHANS,
)

#: Which retention flag turns an operation off. Compaction has none: it is
#: driven by the layout rather than by a switch.
_ENABLED_BY = {
    Operation.EXPIRE: "expire_snapshots",
    Operation.REMOVE_ORPHANS: "remove_orphan_files",
    Operation.REMOVE_DANGLING_DELETES: "remove_dangling_deletes",
    Operation.REWRITE_MANIFESTS: "rewrite_manifests",
}


@dataclass(frozen=True)
class Outcome:
    """What happened to one operation on one table.

    ``exit_code`` carries the CLI's meaning, because an integrator wants the
    same distinctions and inventing a second vocabulary would only mean
    translating between them: 0 success, 2 a configuration or consent problem,
    3 the table is **blocked** (a refusal, not a failure), 4 a **safety check
    aborted and nothing was deleted**.
    """

    table: str
    operation: Operation
    exit_code: int
    detail: str
    #: The engine's own result object, when it produced one.
    result: Reportable | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == 0

    @property
    def skipped(self) -> bool:
        """Ran nothing, and that was the right answer.

        Disabled in the config, unsupported by this engine, or already done by
        an operation that fulfils it. Distinct from `ok` because "we did the
        work" and "there was no work to do" are different things to report.
        """
        return self.result is None and self.exit_code == 0

    def describe(self) -> str:
        return f"{self.table} {self.operation.value}: {self.detail}"

    def as_dict(self) -> dict[str, Any]:
        """The whole outcome, structurally, so a caller need not walk the type.

        `detail` is kept because it is the only place a *skip* says why -- a
        disabled operation, an unsupported one, or one fulfilled by another
        produces no result at all, and the reason exists only as prose. So the
        contract is: `result` for what changed, `detail` for why nothing did.

        `exit_code` carries the CLI's meaning, which is the point: an integrator
        streaming these through `observer=` can alert on the same numbers a cron
        line does, without a second vocabulary.
        """
        return {
            "table": self.table,
            "operation": self.operation.value,
            "exit_code": self.exit_code,
            "ok": self.ok,
            "skipped": self.skipped,
            "detail": self.detail,
            "result": self.result.as_dict() if self.result is not None else None,
        }


#: The three states of one unit of work, least to most severe. The order is the
#: precedence used when a pair produced more than one outcome.
_WORK_STATES = ("skipped", "maintained", "failed")


@dataclass(frozen=True)
class RunCounters:
    """What a run had to do, and what it found there was no point doing.

    **The unit of work is one operation on one table, not one table.** Building
    it per table would have produced a counter that is structurally always
    zero, and ZMBNI-116 is what makes it so: `expire` and `remove-orphans`
    answer to the clock and `apply-properties` to the config file, so all three
    execute on every table on every run. No table is ever entirely without work,
    a per-table `skipped` would read 0 on every run that will ever happen, and
    the gate this exists to answer would be reading an artefact of where we drew
    that line rather than a measurement.

    Per pair, an untouched table reports 3 of 6 skipped and a written one 0 of
    6 -- the share of the scheduled work that had no input, which is the
    question, and it moves when reality moves.

    :attr:`tables` is counted too, because "half the work on two hundred tables"
    and "half the work on two" are different situations and the rate alone
    cannot tell them apart.

    The buckets partition: ``considered == skipped + maintained + failed``, one
    per pair. `test_the_counters_partition_the_run` proves it rather than this
    docstring asserting it.

    * **maintained** -- the operation executed and produced a result.
    * **failed** -- a non-zero exit code: blocked, aborted, or a config refusal.
      It beats `skipped`, and that is the one that matters. A blocked table also
      ran nothing, so the naive test "no result" would file it under `skipped`,
      inflating the very number the gate rests on and arguing for event plumbing
      out of tables that were simply broken.
    * **skipped** -- ran nothing, and that was right: disabled, unsupported,
      fulfilled by another operation, or unchanged since the last maintenance.

    What this does **not** measure: a table that *was* written to, where
    compaction then reads every manifest and rewrites nothing, counts as
    `maintained`. :class:`~zamboni.maintainers.Reportable` carries no uniform
    "did anything change" signal, and inventing one is a change to six result
    contracts rather than a counter. That is not a gap in the gate, though: it
    is waste no event plumbing removes, because a write did happen and an event
    would have fired too. The gate asks about work with *no* input, which is
    exactly `skipped`.
    """

    tables: int = 0
    considered: int = 0
    skipped: int = 0
    maintained: int = 0
    failed: int = 0

    @property
    def skip_rate(self) -> float | None:
        """The gate number: the share of the work that had nothing to act on.

        ``None`` rather than a division when nothing was considered. An empty
        run has no rate, and 0.0 would read as "nothing was skipped" -- a
        different claim, and a misleading one to average across a fleet.
        """
        if not self.considered:
            return None
        return self.skipped / self.considered

    def describe(self) -> str:
        rate = self.skip_rate
        share = "" if rate is None else f" -- {rate:.0%} of the work had no input"
        return (
            f"{self.considered} operation(s) on {self.tables} table(s): "
            f"{self.maintained} maintained, {self.skipped} skipped, "
            f"{self.failed} failed{share}"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "tables": self.tables,
            "considered": self.considered,
            "skipped": self.skipped,
            "maintained": self.maintained,
            "failed": self.failed,
            "skip_rate": self.skip_rate,
        }


@dataclass(frozen=True)
class MaintenanceReport:
    outcomes: tuple[Outcome, ...] = ()
    #: The warehouse this run maintained, from the table config. Carried so that
    #: a fleet collecting :attr:`counters` from many runs can label each
    #: aggregate without having to remember which run produced it.
    warehouse: str | None = None
    #: When the run started and finished, UTC. A series of run records is not
    #: much use without them: "the skip share was 50%" only means something
    #: beside when it was measured and how long the run took.
    started_at: datetime | None = None
    ended_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at is None or self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def exit_code(self) -> int:
        """The **worst** any operation produced.

        The worst rather than the last, so a partial failure is never reported
        as success -- which matters most here, because this is what a cron line
        keys its alerting on and nobody reads the log otherwise.
        """
        return max((o.exit_code for o in self.outcomes), default=0)

    @property
    def failures(self) -> tuple[Outcome, ...]:
        return tuple(o for o in self.outcomes if o.exit_code)

    @property
    def tables(self) -> tuple[str, ...]:
        seen: dict[str, None] = {}
        for outcome in self.outcomes:
            seen.setdefault(outcome.table, None)
        return tuple(seen)

    @property
    def counters(self) -> RunCounters:
        """What the run had to do, and what it found no point doing.

        See :class:`RunCounters` for why the unit is the (table, operation) pair.

        **Derived, never stored.** The alternative -- incrementing counters in
        the run loop -- gives the report two sources of truth for the same fact,
        and the failure mode is a report whose counters disagree with its own
        outcomes, which is worse than no counters at all because a dashboard
        would believe them. Derivation costs one pass over a list.

        A pair only counts once it has produced an outcome, so a run asked for
        no operations considers nothing -- the truthful answer -- and the
        operations after a safety abort are not counted either, because they
        genuinely were not considered.

        A pair is counted **once** even where it produced two outcomes. It can:
        an abort records a follow-on note under the aborted operation's own
        name, and counting that as a second, skipped unit of work would both
        break the reconciliation and inflate the skip share with the aftermath
        of a failure. The worst of the two wins.
        """
        worst: dict[tuple[str, Operation], str] = {}
        for outcome in self.outcomes:
            key = (outcome.table, outcome.operation)
            state = _work_state(outcome)
            if _WORK_STATES.index(state) >= _WORK_STATES.index(worst.get(key, "skipped")):
                worst[key] = state

        tally = dict.fromkeys(_WORK_STATES, 0)
        for state in worst.values():
            tally[state] += 1
        return RunCounters(
            tables=len(self.tables),
            considered=len(worst),
            **tally,
        )

    def as_dict(self) -> dict[str, Any]:
        """A whole run, serialisable in one call. ZMBNI-32."""
        # Imported here rather than at module scope: `zamboni/__init__` imports
        # this module, so a top-level `from . import versions` is a circular
        # import that fails outright. Same deferral as `maintenance_watermark`
        # below, for the same reason.
        from . import versions

        return {
            "versions": versions(),
            "exit_code": self.exit_code,
            "warehouse": self.warehouse,
            "started_at": _iso(self.started_at),
            "ended_at": _iso(self.ended_at),
            "duration_seconds": self.duration_seconds,
            "tables": list(self.tables),
            "failures": len(self.failures),
            "counters": self.counters.as_dict(),
            "outcomes": [o.as_dict() for o in self.outcomes],
        }

    def describe(self) -> str:
        lines = [o.describe() for o in self.outcomes]
        lines.append(self.counters.describe())
        if self.failures:
            lines.append(f"{len(self.failures)} operation(s) failed")
        return "\n".join(lines)


def _iso(moment: datetime | None) -> str | None:
    """UTC, to the second, with an explicit ``Z``.

    Seconds because a maintenance run is minutes long and sub-second precision in
    a nightly log is noise; explicit ``Z`` because a naive timestamp in a series
    collected from several hosts is a bug waiting for the clocks to disagree.
    """
    if moment is None:
        return None
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _work_state(outcome: Outcome) -> str:
    """Which of :class:`RunCounters`\' three buckets one outcome belongs in.

    Exhaustive and exclusive by construction: `result` is only ever set on the
    one return that also carries exit code 0, so "executed" and "failed" cannot
    both be true and everything else ran nothing.
    """
    if outcome.exit_code:
        return "failed"
    if outcome.result is not None:
        return "maintained"
    return "skipped"


def maintain(
    session: CatalogSession,
    *,
    table_config: str | Path | TableConfig | None = None,
    tables: Sequence[str] | None = None,
    engine: str = "local",
    engine_options: dict[str, str] | None = None,
    operations: Iterable[Operation | str] = RUNBOOK_ORDER,
    commit: bool = False,
    base_config: CompactionConfig | None = None,
    warehouse: str | None = None,
    observer: Callable[[Outcome], None] | None = None,
) -> MaintenanceReport:
    """Run every operation, in order, over every configured table.

    Args:
        table_config: A path or a loaded :class:`TableConfig`. Supplies the
            layout and retention for each table, and -- when ``tables`` is not
            given -- which tables to maintain.
        tables: Restrict the run. Defaults to every table the config names.
        engine: ``"local"``, ``"trino"`` or ``"spark"``.
        engine_options: Connection settings for a non-local engine, e.g.
            ``{"remote": "sc://spark:15002"}``.
        operations: Defaults to :data:`RUNBOOK_ORDER`. Order is respected as
            given, because the gaps between operations are load-bearing.
        commit: **False previews.** The same default as the CLI, where the rule
            is that nothing commits without ``--yes``: a caller that has not
            thought about it gets the safe answer.
        warehouse: Checked against the config's own ``warehouse``. Pass it and a
            file describing a different warehouse stops the run.
        observer: Called with each :class:`Outcome` as it happens, for progress
            on a long run. The report is returned either way.

    The report carries :attr:`~MaintenanceReport.counters` -- how many tables
    were considered, and how many of them had nothing to do -- which is the
    number to alert or size a schedule on. See :class:`RunCounters`.

    Failures do not stop the run. Each table is attempted, and the report
    carries the worst exit code -- except after a safety abort (exit 4), where
    the rest of *that table* is skipped because everything following it reads
    the state we have just said we do not trust.
    """
    config = _resolve_config(table_config, warehouse)
    wanted = list(tables) if tables is not None else sorted(config.tables)
    order = [Operation(o) for o in operations]
    maintainer = get_maintainer(engine)(session, engine_options or {})

    started_at = datetime.now(UTC)
    outcomes: list[Outcome] = []

    def record(outcome: Outcome) -> None:
        outcomes.append(outcome)
        if observer is not None:
            observer(outcome)

    for table in wanted:
        settings = config.for_table(table)
        request = MaintenanceRequest(
            retention=settings.retention,
            compaction=config_from_table_settings(settings, base_config),
            table_config=config,
        )
        unchanged = _unchanged_since_maintenance(session, table)
        done: set[Operation] = set()
        for operation in order:
            outcome = _run(
                maintainer,
                table,
                operation,
                request,
                settings,
                done,
                commit=commit,
                unchanged=unchanged,
            )
            record(outcome)
            if outcome.result is not None:
                done.add(operation)
            if outcome.exit_code == 4:
                record(
                    Outcome(
                        table,
                        operation,
                        0,
                        f"stopping this table: {operation.value} aborted",
                    )
                )
                break

    return MaintenanceReport(
        tuple(outcomes),
        warehouse=config.warehouse,
        started_at=started_at,
        ended_at=datetime.now(UTC),
    )


#: Operations whose input is *new data*, and which therefore have nothing to do
#: when nothing has written since maintenance last ran.
#:
#: The other three are deliberately absent, and the reason is correctness rather
#: than caution. `expire` is a function of snapshots **and the clock**: a table
#: nobody writes still ages past its retention window, so skipping it on "no
#: writes" would leave low-traffic tables accumulating snapshots forever --
#: a defect that surfaces months later as unbounded metadata growth, which is
#: precisely the thing this tool exists to prevent. `remove-orphans` is the same
#: shape: its age guard makes files *eligible* with time, and a previous run may
#: have left some. `apply-properties` answers to the config file, and a config
#: change is invisible to the watermark.
WRITE_DRIVEN = frozenset(
    {Operation.COMPACT, Operation.REWRITE_MANIFESTS, Operation.REMOVE_DANGLING_DELETES}
)


def _unchanged_since_maintenance(session: CatalogSession, table: str) -> str | None:
    """Why this table is untouched since maintenance last ran, or ``None``.

    One metadata load per table, ~20-38 ms measured -- against ~400 ms to profile
    a table and ~2,035 ms for a full orphan scan, so asking is around 2% of the
    cost of the cheapest thing it can save.

    **Any doubt answers ``None``.** A catalog that will not load the table, a
    watermark that cannot be read: the run proceeds and the operations decide for
    themselves, exactly as before. A skip is an optimisation and must never be
    the thing that stops a table being maintained -- and this function sits in
    front of every operation in the run, so the safe direction is the one where
    it is wrong.
    """
    from .health import maintenance_watermark

    try:
        mark = maintenance_watermark(session.catalog.load_table(table))
    except Exception:  # pragma: no cover - any catalog trouble means "just run"
        logger.debug("could not read the watermark for %s; running anyway", table, exc_info=True)
        return None

    if mark.written_since or not mark.maintained:
        return None
    return f"nothing written since {mark.operation}"


def _run(
    maintainer: Maintainer,
    table: str,
    operation: Operation,
    request: MaintenanceRequest,
    settings,
    done: set[Operation],
    *,
    commit: bool,
    unchanged: str | None = None,
) -> Outcome:
    """One operation, with every "this is not a failure" case named.

    The four shapes of not-a-failure are easy to conflate and expensive to get
    wrong: *disabled* means the config said no, *unsupported* means the engine
    said no, *fulfilled* means another operation already did it, and *unchanged*
    means nothing has written since maintenance last ran. All four exit 0 and
    none of them ran anything.

    Args:
        unchanged: Why this table is untouched since the last maintenance, or
            ``None`` if it is not. Only :data:`WRITE_DRIVEN` operations are
            skipped for it.
    """
    flag = _ENABLED_BY.get(operation)
    if flag is not None and not getattr(request.retention, flag).enabled:
        return Outcome(table, operation, 0, f"disabled in the config ({flag})")

    if unchanged is not None and operation in WRITE_DRIVEN:
        # A fourth shape of not-a-failure, and the cheapest: nothing has written,
        # so there is nothing for this operation to act on. Reported rather than
        # passed over silently -- an operator comparing two nightly runs needs to
        # see that a table was considered and found to need nothing, which reads
        # very differently from a table that was never reached.
        return Outcome(table, operation, 0, f"nothing to do -- {unchanged}")

    support = maintainer.capabilities().of(operation)
    if support.fulfilled_by and support.fulfilled_by in done:
        # Not decoration. On Spark, dangling-delete removal *is* an option of
        # rewrite_data_files, so running both compacts the table twice -- the
        # second time to no effect.
        return Outcome(table, operation, 0, f"already done by {support.fulfilled_by.value}")

    try:
        maintainer.check_supported(operation)
        if problems := maintainer.validate(operation, request):
            raise EngineConfigProblem(
                f"this configuration cannot run {operation.value} on "
                f"{maintainer.name}:\n  - " + "\n  - ".join(problems)
            )
        result = maintainer.execute(operation, table, request=request, dry_run=not commit)
    except UnsupportedOperation as exc:
        # Declared, not a failure: Trino cannot remove dangling deletes, and a
        # nightly fleet run should skip it rather than fail every night.
        return Outcome(table, operation, 0, f"skipped -- {exc}")
    except (CompactionBlocked, UnsupportedPyIceberg) as exc:
        # Both are refusals rather than failures: the table, or the build under
        # it, is blocked. `UnsupportedPyIceberg` escaping instead would abort the
        # whole fleet run with a traceback and exit 1 -- contradicting both the
        # exit-code contract and this function's promise that each table is
        # attempted. Added with ZMBNI-37, which gave five more operations a way
        # to raise it.
        return Outcome(table, operation, 3, str(exc))
    except (ConcurrentModification, CommitFailedException, ValidationException) as exc:
        # Another writer committed to this table while the operation was running.
        # A refusal, not a failure: nothing was changed, and the answer is to run
        # again outside the load window rather than to investigate.
        #
        # Three exceptions because the conflict surfaces at three different
        # depths, and a fleet run must not care which:
        #
        #   ConcurrentModification   ours -- `ReplaceCommitter.commit` re-reads
        #                            the table and refuses when the snapshot moved
        #                            between planning and commit
        #   ValidationException      PyIceberg's `_validate_concurrency`, which
        #                            checks the commit window before the swap and
        #                            is *not* retried by its own retry loop
        #   CommitFailedException    the CAS itself, after PyIceberg exhausts
        #                            `commit.retry.num-retries` (default 4)
        #
        # Measured rather than reasoned about: against a live Lakekeeper with an
        # upsert writer committing every 50ms, 67 compaction runs produced 60
        # successes, 3 ConcurrentModification and 4 ValidationException -- and
        # before this, the first of them ended the whole run, leaving every later
        # table unmaintained. Nothing was corrupted in any of them; detection
        # worked every time. The defect was only ever in what happened next.
        return Outcome(
            table,
            operation,
            3,
            f"another writer committed to this table during {operation.value}; "
            f"nothing was changed -- retry outside the load window ({exc})",
        )
    except (ExpiryAborted, OrphanCleanupAborted) as exc:
        # A safety check refused. Nothing was deleted, and the operator response
        # is the same in both cases: stop and look.
        return Outcome(table, operation, 4, f"aborted, nothing deleted: {exc}")
    except (
        PreviewUnavailable,
        EngineConfigProblem,
        WorkspaceUnavailable,
        StorageCredentialsRequired,
    ) as exc:
        # `WorkspaceUnavailable` is a deployment misconfiguration -- no writable
        # spill directory -- and `StorageCredentialsRequired` is the same shape:
        # no usable object-store credentials, credentials for the wrong store, or
        # a cloud backend that is not installed. Both are exit 2 beside the other
        # config refusals rather than an uncaught exception. It will be raised by every table in
        # the run, which is the point: the operator sees one clear reason per
        # table instead of a traceback from the first one (ZMBNI-76's shape).
        return Outcome(table, operation, 2, str(exc))

    return Outcome(table, operation, 0, result.describe(), result=result)


def _resolve_config(
    table_config: str | Path | TableConfig | None, warehouse: str | None
) -> TableConfig:
    if table_config is None:
        raise TableConfigError(
            "maintain() needs a table_config: it supplies the retention that "
            "decides what may be deleted, and defaulting that would be guessing "
            "on the caller's behalf about deleting their data."
        )
    config = (
        table_config if isinstance(table_config, TableConfig) else TableConfig.load(table_config)
    )
    if warehouse and config.warehouse != warehouse:
        raise TableConfigError(
            f"the config declares warehouse {config.warehouse!r}, but this run is "
            f"maintaining {warehouse!r}. One of the two is wrong; the file is the "
            "one that travels between directories."
        )
    return config


__all__ = [
    "RUNBOOK_ORDER",
    "MaintenanceReport",
    "Outcome",
    "RunCounters",
    "maintain",
    "validate_policy",
]


def validate_policy(
    table_config: str | Path | TableConfig,
    *,
    engine: str,
    operations: Iterable[Operation | str] = RUNBOOK_ORDER,
    engine_options: dict[str, str] | None = None,
    tables: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Will this engine accept this policy? Answered without connecting to anything.

    The one-shot form of :meth:`~zamboni.maintainers.Maintainer.validate_request`,
    over every table a config names. It exists for the same reason
    :func:`maintain` does: the first production integrator assembled it by hand,
    and every integrator after would have re-derived the same loop -- which
    operations to ask about, how to resolve a table's retention, and how to
    attribute a problem to a table (ZMBNI-33).

    **Takes no `CatalogSession`, deliberately.** That is the whole point, and it
    is the opposite of what the original report proposed. Maintainer construction
    was already pure -- `TrinoMaintainer.__init__` reads `version` from an option
    rather than from the server, measured by blocking `socket.connect` and
    constructing one. What connects is the *catalog* session a caller builds to
    pass in: `CatalogSession.for_lakekeeper()` resolves configuration eagerly, so
    against an unreachable catalog it hangs rather than failing fast. A signature
    taking a session would therefore have kept the exact failure the report was
    trying to remove, which is why this one does not have one.

    So a caller can distinguish the two answers it could not before: a non-empty
    return means *the engine rejects this policy*, and there is no third
    possibility, because nothing here can fail to connect.

    Returns:
        One string per problem, each naming the table and the operation. Empty
        means every named engine check passed -- not that the run will succeed,
        which no amount of validation can promise.
    """
    config = _resolve_config(table_config, None)
    maintainer = get_maintainer(engine)
    wanted = list(tables) if tables is not None else sorted(config.tables)

    problems: list[str] = []
    for table in wanted:
        settings = config.for_table(table)
        request = MaintenanceRequest(retention=settings.retention, table_config=config)
        for operation in (Operation(o) for o in operations):
            support = maintainer.capabilities().of(operation)
            if not support.usable:
                # Not a problem with the policy. `maintain` skips these at exit 0
                # and reporting them here would make an unsupported operation
                # look like a misconfiguration.
                continue
            problems.extend(
                f"{table} {operation.value}: {problem}"
                for problem in maintainer.validate_request(
                    operation, request, options=engine_options or {}
                )
            )
    return tuple(problems)
