"""The maintainer contract.

The interface exists to stop an engine quietly accepting an option it cannot
honour, so most of these assert a *refusal*. The declarations themselves are
checked against docs/engine-comparison.md, because a capability table that
drifts from the analysis it came from is worse than no table: it reads as
verified.
"""

from __future__ import annotations

import pytest

from zamboni import maintainers
from zamboni.maintainers import (
    EngineConfigProblem,
    MaintainerCapabilities,
    MaintenanceRequest,
    Operation,
    OperationSupport,
    PreviewUnavailable,
    Support,
    UnsupportedOperation,
)
from zamboni.maintainers.local import LocalMaintainer
from zamboni.maintainers.spark import SparkMaintainer
from zamboni.maintainers.trino import DEFAULT_MIN_RETENTION_DAYS, TrinoMaintainer
from zamboni.tableconfig import Retention


def request(**kwargs) -> MaintenanceRequest:
    return MaintenanceRequest(retention=Retention(), **kwargs)


# -- the model itself ----------------------------------------------------


def test_every_engine_declares_every_operation():
    """Silence is the failure this prevents. An operation nobody declared reads
    as unsupported to a caller and as an oversight to a reviewer, and they
    cannot tell which."""
    for name in maintainers.available():
        caps = maintainers.get(name).capabilities()
        for operation in Operation:
            assert caps.of(operation) is not None


def test_a_partial_support_must_say_why():
    with pytest.raises(ValueError, match="no limitation is given"):
        OperationSupport(Operation.COMPACT, Support.PARTIAL, can_preview=True)


def test_an_unsupported_operation_must_say_why():
    with pytest.raises(ValueError, match="no limitation is given"):
        OperationSupport(Operation.COMPACT, Support.UNSUPPORTED, can_preview=False)


def test_an_unsupported_operation_cannot_claim_to_preview():
    with pytest.raises(ValueError, match="cannot be previewed"):
        OperationSupport(
            Operation.COMPACT, Support.UNSUPPORTED, can_preview=True, limitations=("no",)
        )


def test_capabilities_reject_a_missing_operation():
    with pytest.raises(ValueError, match="no declared support"):
        MaintainerCapabilities(engine="partial", operations={})


# -- support is not binary -----------------------------------------------


def test_support_is_three_valued_in_practice_not_just_in_theory():
    """If every declaration were FULL or UNSUPPORTED the third state would be
    dead weight. These are the real ones, from the engine comparison."""
    assert LocalMaintainer.capabilities().of(Operation.REMOVE_DANGLING_DELETES).support is (
        Support.PARTIAL
    )
    assert TrinoMaintainer.capabilities().of(Operation.EXPIRE).support is Support.PARTIAL
    assert SparkMaintainer.capabilities().of(Operation.EXPIRE).support is Support.PARTIAL


def test_trino_cannot_remove_dangling_deletes_at_all():
    """The one operation missing from an engine outright."""
    support = TrinoMaintainer.capabilities().of(Operation.REMOVE_DANGLING_DELETES)

    assert support.support is Support.UNSUPPORTED
    assert not support.usable

    with pytest.raises(UnsupportedOperation, match="remove-dangling-deletes"):
        TrinoMaintainer(None).check_supported(Operation.REMOVE_DANGLING_DELETES)


def test_spark_fulfils_dangling_deletes_through_compaction():
    """Not a standalone procedure there -- an option on rewrite_data_files."""
    support = SparkMaintainer.capabilities().of(Operation.REMOVE_DANGLING_DELETES)

    assert support.support is Support.FULL
    assert support.fulfilled_by is Operation.COMPACT


# -- preview is per operation --------------------------------------------


def test_preview_is_per_operation_not_per_engine():
    """The asymmetry that forces the whole design: Spark's remove_orphan_files
    takes dry_run and no other Spark procedure does."""
    spark = SparkMaintainer.capabilities()

    assert spark.can_preview(Operation.REMOVE_ORPHANS)
    assert not spark.can_preview(Operation.COMPACT)
    assert not spark.can_preview(Operation.EXPIRE)


def test_the_local_engine_previews_everything():
    caps = LocalMaintainer.capabilities()

    assert all(caps.can_preview(operation) for operation in Operation)


def test_trino_previews_nothing():
    caps = TrinoMaintainer.capabilities()

    assert not any(caps.can_preview(operation) for operation in Operation)


# -- the --yes rule survives an engine that cannot preview ---------------


def test_an_engine_that_cannot_preview_refuses_rather_than_running():
    """The rule is "without --yes, nothing is committed". Refusing keeps it --
    executing would break it, and printing a dry-run notice over an engine with
    no such mode would be a lie. See roadmap.md open question 2."""
    with pytest.raises(PreviewUnavailable, match="cannot preview"):
        TrinoMaintainer(None).check_consent(Operation.COMPACT, yes=False)


def test_consent_makes_a_non_previewing_engine_runnable():
    TrinoMaintainer(None).check_consent(Operation.COMPACT, yes=True)  # does not raise


def test_an_engine_that_can_preview_needs_no_consent_to_preview():
    LocalMaintainer(None).check_consent(Operation.COMPACT, yes=False)  # does not raise
    SparkMaintainer(None).check_consent(Operation.REMOVE_ORPHANS, yes=False)


# -- config translation validates, it does not just format ---------------


def test_trino_rejects_our_default_retention_at_plan_time():
    """A perfectly valid table-config.json is unusable against a default Trino:
    our defaults are 5 and 3 days, its documented floors are 7 for both. This
    must surface before anything runs, not as a server error part-way through."""
    problems = TrinoMaintainer(None).validate(Operation.EXPIRE, request(max_snapshot_age_days=5))

    assert len(problems) == 1
    assert "min-retention" in problems[0]
    assert str(DEFAULT_MIN_RETENTION_DAYS) in problems[0]


def test_trino_accepts_a_retention_at_or_above_the_floor():
    assert not TrinoMaintainer(None).validate(
        Operation.EXPIRE, request(max_snapshot_age_days=DEFAULT_MIN_RETENTION_DAYS)
    )


def test_trino_rejects_a_short_orphan_guard():
    problems = TrinoMaintainer(None).validate(Operation.REMOVE_ORPHANS, request(older_than_days=3))

    assert len(problems) == 1
    assert "remove-orphan-files.min-retention" in problems[0]


def test_the_local_engine_imposes_no_floors():
    """Ours is a default, not a floor -- --reclaim-now can set it to zero. That
    difference is the point of §4 of the engine comparison, so it is pinned."""
    local = LocalMaintainer(None)

    assert not local.validate(Operation.EXPIRE, request(max_snapshot_age_days=1))
    assert not local.validate(Operation.REMOVE_ORPHANS, request(older_than_days=0))


# -- the registry --------------------------------------------------------


def test_the_three_engines_are_registered():
    assert maintainers.available() == ["local", "spark", "trino"]


def test_an_unknown_engine_names_the_ones_that_exist():
    with pytest.raises(KeyError, match="local"):
        maintainers.get("presto")


def test_stubs_refuse_to_pretend_they_ran():
    for engine in (TrinoMaintainer, SparkMaintainer):
        with pytest.raises(NotImplementedError, match="not implemented yet"):
            engine(None).execute(Operation.COMPACT, "db.events", request=request(), dry_run=False)


def test_describe_reports_limitations_so_they_are_discoverable():
    text = TrinoMaintainer.capabilities().describe()

    assert "no Z-order" in text
    # Trino *does* sort -- an earlier declaration said it did not. The distinction
    # matters: the gap is lexicographic-only ordering, not absent ordering.
    assert "identity transforms only" in text
    assert "unsupported" in text
    assert "cannot preview" in text


def test_describe_says_when_an_operation_rides_on_another():
    assert "via compact" in SparkMaintainer.capabilities().describe()


# -- the declarations match the analysis they came from ------------------


@pytest.mark.parametrize(
    ("engine", "operation", "expected"),
    [
        # docs/engine-comparison.md §2: five of six verbs are common to all
        # three; only dangling-delete removal is missing, and only from Trino.
        (TrinoMaintainer, Operation.COMPACT, True),
        (TrinoMaintainer, Operation.EXPIRE, True),
        (TrinoMaintainer, Operation.REMOVE_ORPHANS, True),
        (TrinoMaintainer, Operation.REWRITE_MANIFESTS, True),
        (TrinoMaintainer, Operation.APPLY_PROPERTIES, True),
        (TrinoMaintainer, Operation.REMOVE_DANGLING_DELETES, False),
        (SparkMaintainer, Operation.REMOVE_DANGLING_DELETES, True),
        (LocalMaintainer, Operation.REMOVE_DANGLING_DELETES, True),
    ],
)
def test_declarations_match_the_engine_comparison(engine, operation, expected):
    assert engine.capabilities().of(operation).usable is expected


def test_an_engine_config_problem_is_distinct_from_a_refusal():
    """Different exit codes hang off these: a config problem is fixable by
    editing the invocation (2), an unsupported operation is a refusal (3)."""
    assert issubclass(EngineConfigProblem, ValueError)
    assert not issubclass(EngineConfigProblem, UnsupportedOperation)
