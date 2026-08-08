"""The maintainer contract.

The interface exists to stop an engine quietly accepting an option it cannot
honour, so most of these assert a *refusal*. The declarations themselves are
checked against docs/engine-comparison.md, because a capability table that
drifts from the analysis it came from is worse than no table: it reads as
verified.
"""

from __future__ import annotations

import datetime
from dataclasses import replace

import pytest

from zamboni import maintainers
from zamboni.capabilities import detect
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


def test_spark_refuses_to_return_a_preview_it_cannot_produce():
    """Both non-local engines are implemented now (ZMBNI-14, ZMBNI-15), so the
    stub assertion this replaces is gone. What still needs pinning is that
    `execute(dry_run=True)` on an operation Spark cannot preview raises rather
    than returning something that reads like a plan."""
    with pytest.raises(PreviewUnavailable, match="cannot preview"):
        SparkMaintainer(None).execute(
            Operation.COMPACT, "db.events", request=request(), dry_run=True
        )


def test_trino_refuses_to_return_a_preview_it_cannot_produce():
    """Belt and braces below the CLI: `execute(dry_run=True)` on an engine that
    cannot preview must raise, not return something that reads like a plan."""
    with pytest.raises(PreviewUnavailable, match="cannot preview"):
        TrinoMaintainer(None).execute(
            Operation.COMPACT, "db.events", request=request(), dry_run=True
        )


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


# -- ZMBNI-14: the statements Trino actually receives ---------------------

TRINO_479 = {"catalog": "iceberg", "version": "483"}


def trino(**overrides):
    return TrinoMaintainer(None, {**TRINO_479, **overrides})


def full_retention() -> MaintenanceRequest:
    from zamboni.tableconfig import (
        ExpireSnapshotsSettings,
        MetadataSettings,
        RemoveOrphanFilesSettings,
    )

    return MaintenanceRequest(
        retention=Retention(
            expire_snapshots=ExpireSnapshotsSettings(
                enabled=True, max_snapshot_age_days=7, min_snapshots_to_keep=2
            ),
            remove_orphan_files=RemoveOrphanFilesSettings(enabled=True, older_than_days=7),
            metadata=MetadataSettings(previous_versions_max=3, delete_after_commit=True),
        )
    )


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (Operation.COMPACT, 'ALTER TABLE "iceberg"."db"."events" EXECUTE optimize'),
        (
            Operation.EXPIRE,
            (
                'ALTER TABLE "iceberg"."db"."events" EXECUTE expire_snapshots('
                "retention_threshold => '7d', retain_last => 2)"
            ),
        ),
        (
            Operation.REMOVE_ORPHANS,
            (
                'ALTER TABLE "iceberg"."db"."events" EXECUTE remove_orphan_files('
                "retention_threshold => '7d')"
            ),
        ),
        (
            Operation.REWRITE_MANIFESTS,
            'ALTER TABLE "iceberg"."db"."events" EXECUTE optimize_manifests',
        ),
        (
            Operation.APPLY_PROPERTIES,
            (
                'ALTER TABLE "iceberg"."db"."events" SET PROPERTIES '
                "max_previous_versions = 3, delete_after_commit_enabled = true"
            ),
        ),
    ],
)
def test_the_exact_statement_for_each_operation(operation, expected):
    """Pinned exactly, because every one of these has been run against a live
    Trino 483 -- so a change here is a change to something known to work."""
    assert trino().statement_for(operation, "db.events", full_retention()) == expected


def test_apply_properties_translates_the_names_rather_than_passing_them_through():
    """Trino refuses the Iceberg names outright: its table properties are an
    allowlist, and `write.metadata.*` is rejected even through
    `extra_properties` ("Illegal keys"). Found by running it."""
    sql = trino().statement_for(Operation.APPLY_PROPERTIES, "db.events", full_retention())

    assert "write.metadata" not in sql
    assert "max_previous_versions" in sql
    assert "delete_after_commit_enabled" in sql


def test_retain_last_is_omitted_on_a_trino_that_lacks_it():
    """Added in Trino 479. Emitting it against 476 fails outright with
    "property 'retain_last' does not exist" -- verified."""
    sql = trino(version="476").statement_for(Operation.EXPIRE, "db.events", full_retention())

    assert "retain_last" not in sql
    assert "retention_threshold => '7d'" in sql


def test_an_unknown_trino_version_assumes_the_older_behaviour():
    """Guessing the other way turns a working expiry into a hard failure."""
    maintainer = TrinoMaintainer(None, {"catalog": "iceberg"})

    assert not maintainer.supports_retain_last
    assert "retain_last" not in maintainer.statement_for(
        Operation.EXPIRE, "db.events", full_retention()
    )


def test_dropping_retain_last_is_reported_not_silent():
    problems = trino(version="476").validate(Operation.EXPIRE, full_retention())

    assert len(problems) == 1
    assert "retain_last" in problems[0] and "479" in problems[0]


@pytest.mark.parametrize(
    ("identifier", "expected_target"),
    [
        ("db.events", '"iceberg"."db"."events"'),
        # A quote in the name, doubled; a hyphen, which breaks unquoted.
        ('we"ird.ta-ble', '"iceberg"."we""ird"."ta-ble"'),
        # A multi-level namespace. Not a feature Zamboni endorses -- they are
        # unevenly supported, and they do not map onto Postgres or Snowflake,
        # which stop at catalog.schema.table. It is here because *quoting* is
        # what makes such a name usable at all, and that is measured, not
        # assumed: against Trino 483, `"iceberg"."nstest.inner"."t"` returns a
        # row count, while the bare `iceberg.nstest.inner.t` fails with
        # "mismatched input 'inner'". The rule the row pins is "always quote",
        # which is the same rule that handles the hyphen above.
        ("a.b.events", '"iceberg"."a.b"."events"'),
    ],
)
def test_identifiers_are_always_quoted(identifier, expected_target):
    sql = trino().statement_for(Operation.REWRITE_MANIFESTS, identifier, full_retention())

    assert sql == f"ALTER TABLE {expected_target} EXECUTE optimize_manifests"


def test_a_table_without_a_namespace_is_rejected():
    with pytest.raises(ValueError, match="no namespace"):
        trino().statement_for(Operation.COMPACT, "events", full_retention())


def test_trino_refuses_an_operation_it_cannot_do_before_building_sql():
    with pytest.raises(UnsupportedOperation):
        trino().statement_for(Operation.REMOVE_DANGLING_DELETES, "db.events", full_retention())


# -- ZMBNI-1107: the declaration follows the installed build ---------------
#
# These are the point of the story. Asserting the *current* declaration only
# pins today's install; what was wrong before is that the declaration could not
# change at all. Each of these flips a probe and requires the claim to follow.


def probes(**overrides):
    """The real probe result with individual answers flipped."""
    return replace(detect(), **overrides)


def local_with(monkeypatch, **overrides):
    from zamboni.maintainers import local as local_module

    monkeypatch.setattr(local_module, "detect", lambda: probes(**overrides))
    return LocalMaintainer.capabilities()


def test_dangling_deletes_becomes_full_when_the_writer_can_write_one(monkeypatch):
    """ZMBNI-604's caveat is a property of ManifestWriterV2, not of us. A build
    that can write a delete manifest must stop being told it cannot."""
    caps = local_with(monkeypatch, delete_manifests_writable=True)
    support = caps.of(Operation.REMOVE_DANGLING_DELETES)

    assert support.support is Support.FULL
    assert not support.limitations
    assert any("partially dangling" in inv for inv in support.invariants)


def test_dangling_deletes_stays_partial_when_it_cannot(monkeypatch):
    support = local_with(monkeypatch, delete_manifests_writable=False).of(
        Operation.REMOVE_DANGLING_DELETES
    )

    assert support.support is Support.PARTIAL
    assert "ZMBNI-604" in support.limitations[0]


def test_compaction_becomes_full_when_equality_deletes_are_readable(monkeypatch):
    support = local_with(monkeypatch, equality_deletes_readable=True).of(Operation.COMPACT)

    assert support.support is Support.FULL
    assert not support.limitations


def test_compaction_is_partial_while_equality_deletes_are_not(monkeypatch):
    support = local_with(monkeypatch, equality_deletes_readable=False).of(Operation.COMPACT)

    assert support.support is Support.PARTIAL
    assert "equality deletes" in support.limitations[0]


def test_an_unusable_build_makes_compaction_unsupported(monkeypatch):
    """Not a caveat on a working operation. `unsupported_reason` is the check
    that stops a rewrite which would double-count rows, and PARTIAL would
    understate that."""
    support = local_with(
        monkeypatch, prunes_manifests_by_predicate=True, derives_delete_predicate=False
    ).of(Operation.COMPACT)

    assert support.support is Support.UNSUPPORTED
    assert not support.can_preview
    assert "cannot be used" in support.limitations[0]


def test_streaming_writes_are_declared_when_available(monkeypatch):
    with_streaming = local_with(monkeypatch, streaming_write_supported=True).of(Operation.COMPACT)
    without = local_with(monkeypatch, streaming_write_supported=False).of(Operation.COMPACT)

    assert any("streaming writer" in inv for inv in with_streaming.invariants)
    assert not any("streaming writer" in inv for inv in without.invariants)


def test_the_declaration_names_the_installed_version(monkeypatch):
    """So a bug report quoting `zamboni engines` carries the deciding fact."""
    caps = local_with(monkeypatch, version="9.9.9", delete_manifests_writable=False)

    assert "9.9.9" in caps.of(Operation.REMOVE_DANGLING_DELETES).limitations[0]


def test_the_warehouse_limitation_is_not_probe_derived(monkeypatch):
    """remove-orphans needs a bucket listing whatever PyIceberg is installed --
    that is a property of the warehouse. Pinned so a later refactor does not
    sweep it into the probe-driven set and make it disappear on 0.12."""
    caps = local_with(monkeypatch, delete_manifests_writable=True, equality_deletes_readable=True)
    support = caps.of(Operation.REMOVE_ORPHANS)

    assert support.support is Support.PARTIAL
    assert "remote-signing" in support.limitations[0]


# -- ZMBNI-15: the statements Spark actually receives ----------------------


FROZEN_NOW = datetime.datetime(2026, 8, 7, 12, 0, 0, tzinfo=datetime.UTC)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    """Spark takes `older_than` as a literal timestamp computed on this host, so
    an exact-statement assertion needs a fixed clock. Pinning it also documents
    the exposure: the value is ours, not the engine's."""
    from zamboni.maintainers import spark as spark_module

    monkeypatch.setattr(spark_module, "_utc_now", lambda: FROZEN_NOW)


def spark(**overrides):
    return SparkMaintainer(None, {"catalog": "iceberg", **overrides})


def spark_request(**compaction):
    """Always carries a CompactionConfig.

    An earlier version passed None when given no overrides, so the options block
    was skipped and the test asserting `partial-progress.enabled` was checking a
    statement that could never occur in practice -- `_request_for` always builds
    one for compact and maintenance.
    """
    from zamboni.config import CompactionConfig

    return MaintenanceRequest(
        retention=full_retention().retention,
        compaction=CompactionConfig(**compaction),
    )


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (
            Operation.EXPIRE,
            (
                "CALL system.expire_snapshots(table => 'iceberg.db.events', "
                "older_than => TIMESTAMP '2026-07-31 12:00:00+00:00', retain_last => 2)"
            ),
        ),
        (
            Operation.REMOVE_ORPHANS,
            (
                "CALL system.remove_orphan_files(table => 'iceberg.db.events', "
                "older_than => TIMESTAMP '2026-07-31 12:00:00+00:00')"
            ),
        ),
        (
            Operation.REWRITE_MANIFESTS,
            "CALL system.rewrite_manifests(table => 'iceberg.db.events')",
        ),
        (
            Operation.APPLY_PROPERTIES,
            (
                "ALTER TABLE `iceberg`.`db`.`events` SET TBLPROPERTIES ("
                "'write.metadata.previous-versions-max' = '3', "
                "'write.metadata.delete-after-commit.enabled' = 'true')"
            ),
        ),
    ],
)
def test_the_exact_spark_statement_for_each_operation(operation, expected):
    assert spark().statement_for(operation, "db.events", spark_request()) == expected


def test_expire_takes_a_timestamp_where_trino_takes_a_duration():
    """The same setting, two vocabularies -- what MaintenanceRequest is for.

    Trino: `retention_threshold => '7d'`. Spark: a *typed literal* timestamp,
    computed on this host. Both forms were learned by running them -- an
    expression is rejected by the parser, a bare string by the type checker.
    """
    sql = spark().statement_for(Operation.EXPIRE, "db.events", spark_request())

    assert "older_than => TIMESTAMP '2026-07-31 12:00:00+00:00'" in sql
    assert "'7d'" not in sql
    assert "current_timestamp()" not in sql, "an expression is rejected by Spark's parser"


def test_the_orphan_floor_is_two_days_not_one():
    """Spark refuses under 24 hours, and exactly 1 day is refused too: the
    timestamp is computed here and evaluated moments later, so it is short by
    the round trip. Both verified against a live Spark."""
    assert spark().validate(Operation.REMOVE_ORPHANS, spark_request_with_orphan_days(1))
    assert spark().validate(Operation.REMOVE_ORPHANS, spark_request_with_orphan_days(0))
    assert not spark().validate(Operation.REMOVE_ORPHANS, spark_request_with_orphan_days(2))


def spark_request_with_orphan_days(days: int) -> MaintenanceRequest:
    from zamboni.config import CompactionConfig
    from zamboni.tableconfig import RemoveOrphanFilesSettings

    base = full_retention().retention
    return MaintenanceRequest(
        retention=replace(
            base,
            remove_orphan_files=RemoveOrphanFilesSettings(enabled=True, older_than_days=days),
        ),
        compaction=CompactionConfig(),
    )


def test_zorder_reaches_spark():
    """Spark is the only non-local engine that can Z-order, so this is the one
    capability the Trino maintainer had to declare missing."""
    sql = spark().statement_for(
        Operation.COMPACT, "db.events", spark_request(zorder_columns=["a", "b"])
    )

    assert "strategy => 'sort'" in sql
    assert "sort_order => 'zorder(a, b)'" in sql


def test_sort_by_table_order_selects_the_strategy_without_an_expression():
    """Spark defaults `sort_order` to the table's own, which is exactly what the
    setting means -- so naming one would override the thing being asked for."""
    sql = spark().statement_for(
        Operation.COMPACT, "db.events", spark_request(sort_by_table_order=True)
    )

    assert "strategy => 'sort'" in sql
    assert "sort_order =>" not in sql


def test_compaction_always_asks_spark_to_drop_dangling_deletes():
    sql = spark().statement_for(Operation.COMPACT, "db.events", spark_request())

    assert "'remove-dangling-deletes', 'true'" in sql


def test_dangling_delete_removal_is_the_compaction_statement():
    """Not a separate procedure in Spark. Both must emit the same SQL, or the
    `fulfilled_by` declaration is a lie."""
    request = spark_request()
    compact = spark().statement_for(Operation.COMPACT, "db.events", request)
    dangling = spark().statement_for(Operation.REMOVE_DANGLING_DELETES, "db.events", request)

    assert compact == dangling


def test_partial_progress_is_passed_explicitly_not_left_to_a_default():
    """Iceberg's default is false and ZMBNI-106 chose the same locally. Sending
    it makes the two agree visibly rather than by coincidence."""
    off = spark().statement_for(Operation.COMPACT, "db.events", spark_request())
    on = spark().statement_for(Operation.COMPACT, "db.events", spark_request(partial_progress=True))

    assert "'partial-progress.enabled', 'false'" in off
    assert "'partial-progress.enabled', 'true'" in on


def test_procedure_arguments_take_a_plain_identifier_not_quoted_sql():
    """`table => 'db.events'` is a string argument. Backticks belong in
    ALTER TABLE and would be part of the name here."""
    call = spark().statement_for(Operation.REWRITE_MANIFESTS, "db.events", spark_request())
    alter = spark().statement_for(Operation.APPLY_PROPERTIES, "db.events", spark_request())

    assert "`" not in call
    assert "`iceberg`.`db`.`events`" in alter


def test_spark_identifiers_are_backtick_quoted():
    """Spark quotes with backticks where Trino uses double quotes."""
    sql = spark().statement_for(Operation.APPLY_PROPERTIES, "we`ird.ta-ble", spark_request())

    assert "ALTER TABLE `iceberg`.`we``ird`.`ta-ble`" in sql


# -- review findings (independent review of ZMBNI-15) ---------------------


def test_a_backticked_identifier_survives_into_the_procedure_argument():
    """The first implementation derived the plain identifier by stripping every
    backtick off the quoted form. `quote()` doubles an embedded backtick to
    escape it, so stripping collapsed the escape and the delimiters together and
    ``we`ird.ta-ble`` silently became ``weird.ta-ble`` -- a *different table*,
    targeted with no error, by operations that delete files."""
    sql = spark().statement_for(Operation.REWRITE_MANIFESTS, "we`ird.ta-ble", spark_request())

    assert "table => 'iceberg.we`ird.ta-ble'" in sql
    assert "weird" not in sql


def test_the_timestamp_carries_an_explicit_utc_offset():
    """A bare wall-clock is read in `spark.sql.session.timeZone`, not UTC.

    Verified against a live session in America/New_York: our UTC value was
    shifted four hours later, so every expiry cut deeper than asked, and a
    1-day orphan interval fell under Spark's 24h floor. The offset removes the
    ambiguity without touching the operator's session timezone.
    """
    sql = spark().statement_for(Operation.EXPIRE, "db.events", spark_request())

    assert "TIMESTAMP '2026-07-31 12:00:00+00:00'" in sql


@pytest.mark.parametrize(
    ("retention_enabled", "policy", "expected"),
    [
        (True, "report", "true"),
        (False, "report", "false"),
        (True, "block", "false"),
    ],
)
def test_compaction_honours_the_dangling_delete_settings(retention_enabled, policy, expected):
    """Hard-coded `true` meant an operator who disabled dangling-delete removal,
    or set `block` specifically so compaction would refuse rather than touch
    delete files, got them deleted anyway and silently. Iceberg's option accepts
    false; nothing forced the hard-coding."""
    from zamboni.config import CompactionConfig
    from zamboni.tableconfig import RemoveDanglingDeletesSettings

    request = MaintenanceRequest(
        retention=replace(
            full_retention().retention,
            remove_dangling_deletes=RemoveDanglingDeletesSettings(enabled=retention_enabled),
        ),
        compaction=CompactionConfig(dangling_delete_policy=policy),
    )

    sql = spark().statement_for(Operation.COMPACT, "db.events", request)

    assert f"'remove-dangling-deletes', '{expected}'" in sql


def test_compact_declares_the_dangling_delete_side_effect():
    """An operator reading `zamboni engines` for `compact` must be able to learn
    that it also touches delete files. Previously that coupling was recorded
    only under remove-dangling-deletes."""
    support = SparkMaintainer.capabilities().of(Operation.COMPACT)

    assert any("dangling" in invariant for invariant in support.invariants)


def test_the_preview_flag_is_an_argument_not_a_string_splice():
    """`execute` used to add the flag by `str.replace`-ing the built statement,
    so a preview that failed to match its own needle would silently delete for
    real. Asking the builder for it means there is nothing to mismatch."""
    maintainer = spark()

    preview = maintainer.statement_for(
        Operation.REMOVE_ORPHANS, "db.events", spark_request(), dry_run=True
    )
    live = maintainer.statement_for(Operation.REMOVE_ORPHANS, "db.events", spark_request())

    assert "dry_run => true" in preview
    assert "dry_run" not in live


def test_no_other_operation_can_be_asked_for_a_preview():
    """`can_preview` says only remove-orphans previews; the builder must agree,
    rather than accepting the flag and dropping it."""
    with pytest.raises(PreviewUnavailable):
        spark().execute(Operation.COMPACT, "db.events", request=spark_request(), dry_run=True)


# -- layout features (ZMBNI-1907) -----------------------------------------


def test_every_engine_declares_its_layout_features():
    """A missing declaration reads as "none of them", which would make the
    config summary warn about a Z-order that works perfectly well."""
    for name in maintainers.available():
        capabilities = maintainers.get(name).capabilities()
        assert capabilities.layout, f"{name} declares no layout features"


def test_trino_is_the_engine_without_zorder():
    """Pins the fact the whole engine-choice section of the user guide turns on."""
    from zamboni.maintainers import LayoutFeature

    assert maintainers.engines_lacking(LayoutFeature.ZORDER) == ("trino",)
    assert LocalMaintainer.capabilities().can(LayoutFeature.ZORDER)
    assert SparkMaintainer.capabilities().can(LayoutFeature.ZORDER)


def test_partition_evolution_is_local_only():
    from zamboni.maintainers import LayoutFeature

    assert set(maintainers.engines_lacking(LayoutFeature.PARTITION_EVOLUTION)) == {
        "spark",
        "trino",
    }


def test_engines_reports_the_layout_features():
    """`zamboni engines` is the generated answer, so it has to carry this too --
    otherwise the summary derives from a declaration nobody can read."""
    text = TrinoMaintainer.capabilities().describe()

    assert "layout:" in text
    assert "zorder" not in text.split("layout:")[1]
