"""Capability probing and the decisions it drives."""

from __future__ import annotations

import dataclasses
from dataclasses import replace

import pytest

from zamboni.capabilities import PyIcebergCapabilities, detect


def test_probes_the_installed_build():
    caps = detect()
    assert caps.version
    # Every probe must resolve to a real bool, not None or a truthy object --
    # a probe that silently fails open would defeat the whole mechanism.
    for field in dataclasses.fields(caps):
        # `version` is a string and `pruning_evidence` records *how* a probe was
        # settled rather than being one.
        if field.name in ("version", "pruning_evidence"):
            continue
        assert isinstance(getattr(caps, field.name), bool), field.name


def test_installed_build_is_usable():
    assert detect().unsupported_reason() is None


def test_installed_build_writes_added_files_under_their_own_spec():
    """This repository develops and tests against a build that can evolve partitions.

    Without it `pyproject.toml`'s `[tool.uv.sources]` redirect is the only thing
    pointing `pyiceberg` at the maintenance fork, and nothing notices if that
    stops working -- drop the pin, make the rev unreachable, or re-lock without
    it, and uv resolves stock PyIceberg from PyPI, the probe answers False,
    partition evolution is *silently withdrawn as a layout feature*, and the
    whole suite still passes. A capability this project ships would quietly stop
    being exercised, with nothing red.

    Deliberately asserts the **capability**, not where the code came from. If
    upstream merges the patch and a release carries the behaviour, the fork
    retires, `[tool.uv.sources]` goes, and this test keeps passing unchanged --
    which is the end state the fork exists to reach. Checking the git URL
    instead would have to be deleted on the day it finally mattered most.
    """
    from tests.conftest import library_honours_spec_expected

    expected = library_honours_spec_expected()
    actual = detect().added_files_honour_spec
    assert actual is expected, (
        (
            "the installed PyIceberg does not write an added data file under its own "
            "partition spec, but this environment expects the maintenance fork. "
            "`[tool.uv.sources]` in pyproject.toml is what redirects it; run `uv sync` "
            "and check the pinned rev is reachable. See MAINTENANCE-FORK.md on that "
            "branch and docs/pyiceberg-private-api.md."
        )
        if expected
        else (
            "this environment expects a stock PyIceberg (ZAMBONI_EXPECT_BUILD=stock) "
            "but the installed build already honours an added file's own spec. Either "
            "the leg resolved the fork by mistake -- in which case it proves nothing "
            "about what a consumer installs -- or upstream has shipped the behaviour, "
            "in which case the fork can retire and this expectation should go."
        )
    )


def _caps(**overrides) -> PyIcebergCapabilities:
    base = {
        "version": "test",
        "operation_is_injectable": True,
        "replace_summary_supported": False,
        "streaming_write_supported": False,
        "prunes_manifests_by_predicate": False,
        "added_files_honour_spec": False,
        "derives_delete_predicate": False,
        "equality_deletes_readable": False,
        "delete_manifests_writable": False,
    }
    base.update(overrides)
    return PyIcebergCapabilities(**base)


def test_pruning_without_derivation_is_refused():
    """The double-count hazard: predicate pruning with nothing deriving it."""
    caps = _caps(prunes_manifests_by_predicate=True, derives_delete_predicate=False)
    assert not caps.manifest_pruning_is_safe
    assert "counted twice" in caps.unsupported_reason()


@pytest.mark.parametrize(
    "prunes",
    [
        True,  # 0.12: prunes, and the overwrite kept the right rows
        False,  # shows no sign of pruning, and the overwrite agreed
    ],
)
def test_the_observed_outcome_decides_whichever_way_the_source_looks(prunes):
    caps = _caps(prunes_manifests_by_predicate=prunes, derives_delete_predicate=True)
    assert caps.manifest_pruning_is_safe
    assert caps.unsupported_reason() is None


def test_a_structural_no_cannot_overrule_an_observed_row_loss():
    """`or not prunes_manifests_by_predicate` used to grant safety here.

    The combination is "the overwrite lost rows, on a build whose source shows
    no sign of pruning" -- and the source is what is wrong, not the measurement.
    The needle deciding it is `manifest_evaluator`, which in 0.12 matches a
    **local variable name inside a function body**, so renaming a local was
    enough to reach this square. Rows lost is rows lost.
    """
    caps = _caps(prunes_manifests_by_predicate=False, derives_delete_predicate=False)

    assert not caps.manifest_pruning_is_safe
    assert "counted twice" in caps.unsupported_reason()


def test_missing_operation_argument_is_refused():
    caps = _caps(operation_is_injectable=False)
    assert "replace snapshot" in caps.unsupported_reason()


def test_describe_lists_every_probe():
    text = detect().describe()
    for label in ("streaming writes", "equality deletes readable", "derives delete predicate"):
        assert label in text


def test_equality_delete_blocker_follows_the_capability(session, unpartitioned, monkeypatch):
    """When a build can read equality deletes, their presence stops being a blocker."""
    import zamboni.profile as profile_module

    fake_profile = dataclasses.replace  # noqa: F841 - readability only

    def make(readable: bool):
        monkeypatch.setattr(
            profile_module, "detect", lambda: _caps(equality_deletes_readable=readable)
        )
        prof = profile_module.profile_table(unpartitioned)
        # Pretend the table has equality deletes and re-assess.
        prof.equality_delete_files = 3
        return profile_module._assess(unpartitioned, prof)

    blocked = [f.code for f in make(readable=False)]
    allowed = [f.code for f in make(readable=True)]

    assert "equality-deletes" in blocked
    assert "equality-deletes" not in allowed


# -- ZMBNI-1102: probe answers must match the installed build -------------
#
# The tests above check a probe returns *a bool*. These check it returns the
# *right* one, which is a different property and the one that failed: against
# unreleased 0.12 `equality_deletes_readable` came back True while PyIceberg was
# still refusing equality deletes one call deeper. A probe that answers
# confidently and wrongly is worse than one that cannot answer.


def test_the_equality_delete_probe_agrees_with_the_installed_source():
    """If the refusal exists anywhere in scan planning, we must not claim readable.

    Deliberately checks the *module*, not one function. The first version of
    this probe inspected `DataScan._plan_files_local` alone; 0.12 extracted the
    planner and left that method a five-line delegation, so the probe stopped
    finding a guard that was very much still there. Narrowing it back would
    reintroduce exactly that, and this fails if anyone does.
    """
    import inspect

    import pyiceberg.table as table_module

    guard_present = "does not yet support equality deletes" in inspect.getsource(table_module)

    assert detect().equality_deletes_readable is not guard_present, (
        "the probe disagrees with the installed PyIceberg: guard "
        f"{'present' if guard_present else 'absent'}, probe says "
        f"readable={detect().equality_deletes_readable}"
    )


def test_the_delete_manifest_probe_agrees_with_the_installed_writer():
    """ZMBNI-604's blocker is `ManifestWriterV2.content()` returning DATA."""
    from pyiceberg.manifest import ManifestContent, ManifestWriterV2

    writes_data_only = ManifestWriterV2.content(None) is ManifestContent.DATA

    assert detect().delete_manifests_writable is not writes_data_only


# -- the three probes that had no independent cross-check (ZMBNI-14/38) ---
#
# `test_probes_the_installed_build` asserts each answer is a real bool and
# `test_doctor_reports_...` that it reaches the output. Neither asks whether the
# value is *right*, so a structural probe that silently flipped would pass both.
# These three re-derive the answer by a different route. None compares against a
# per-version literal: a table saying "0.12 says X" is the compare-to-a-literal
# pattern CONTRIBUTING rule 2 forbids, and would keep passing after the
# behaviour it describes changed.


def test_the_operation_probe_agrees_with_what_the_producer_accepts(unpartitioned):
    """Derived by constructing one, not by reading the signature again.

    The probe asks `inspect.signature`. This hands a real `_OverwriteFiles` the
    `operation=` argument against a real transaction, which is what
    `ReplaceCommitter.commit` does -- so a build where the parameter exists but
    is rejected, or is accepted under another name, disagrees here.
    """
    from pyiceberg.table.snapshots import Operation
    from pyiceberg.table.update.snapshot import _OverwriteFiles

    table = unpartitioned
    try:
        with table.transaction() as txn:
            _OverwriteFiles(
                operation=Operation.OVERWRITE,
                transaction=txn,
                io=table.io,
                snapshot_properties={},
            )
        accepted = True
    except TypeError:
        accepted = False

    assert detect().operation_is_injectable is accepted


def test_the_replace_summary_probe_agrees_with_the_installed_summary_rules():
    """Behavioural probe, structural cross-check -- the reverse of the pair above.

    The probe calls `update_snapshot_summaries` and catches `ValueError`. This
    reads what that function is willing to accept, so the two disagree if the
    rejection ever moves to a different exception or a different place.
    """
    import inspect

    from pyiceberg.table import snapshots

    source = inspect.getsource(snapshots.update_snapshot_summaries)
    mentions_replace = "Operation.REPLACE" in source

    assert detect().replace_summary_supported is mentions_replace, (
        "the probe and the installed `update_snapshot_summaries` disagree about "
        "whether REPLACE is an accepted operation"
    )


def test_the_streaming_probe_agrees_with_what_the_writer_accepts(unpartitioned):
    """Derived by writing one, not by reading an annotation.

    The probe reads the `df` parameter's type annotation off
    `_dataframe_to_data_files`, which is the very thing that changed under us
    once before. This hands it an actual `RecordBatchReader` and looks at
    whether files come back. An annotation that says one thing while the
    implementation does another shows up here and nowhere else -- the streaming
    compaction tests skip when the probe is False, so a wrongly-False probe
    makes them pass by not running.
    """
    import pyarrow as pa
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    table = unpartitioned
    arrow = table.scan().to_arrow()
    reader = pa.RecordBatchReader.from_batches(arrow.schema, arrow.to_batches())

    try:
        written = list(
            _dataframe_to_data_files(table_metadata=table.metadata, df=reader, io=table.io)
        )
        accepted = bool(written)
    except Exception:
        accepted = False

    assert detect().streaming_write_supported is accepted


def test_the_pruning_pair_is_consistent_on_the_installed_build():
    """Pruning without derivation is the combination that double-counts rows.

    Both flipped together between 0.11.1 and 0.12, which is why the build stays
    usable across that boundary -- but nothing guarantees a future build does
    the same, and this is where that would surface.
    """
    caps = detect()

    assert caps.manifest_pruning_is_safe, (
        f"prunes={caps.prunes_manifests_by_predicate} "
        f"derives={caps.derives_delete_predicate}: this build would double-count rows"
    )


def test_doctor_reports_the_installed_version_and_every_probe():
    """The output an operator pastes into a bug report has to carry the facts."""
    caps = detect()
    text = caps.describe()

    assert caps.version in text
    for field in dataclasses.fields(caps):
        if field.name == "version":
            continue
        assert str(getattr(caps, field.name)) in text, f"{field.name} missing from doctor output"


# -- the pruning probe is behavioural, not name-based (ZMBNI-1109) --------


def test_a_structural_no_cannot_skip_the_observation(monkeypatch):
    """The short-circuit this replaced was reachable by renaming a local.

    It read: no sign of pruning -> safe, without running the probe. `prunes` is
    settled by grepping `_existing_manifests`'s body for `manifest_evaluator`,
    which matches the local `manifest_evaluators` -- so a pure refactor upstream
    would have declared a pruning build safe with nothing measured.

    Its justification was cost, on a premise that expired: "does not prune ->
    no cost, this is 0.11.1, which is every current user". The floor is >=0.12
    now and every supported build prunes, so it saved a branch no supported
    install takes -- and since ZMBNI-88 the probe is paid once per install.
    """
    from zamboni import capabilities

    observed = []
    monkeypatch.setattr(capabilities, "_pruning_behaves", lambda: observed.append(1) or False)

    safe, evidence = capabilities._derivation_is_correct(prunes=False)

    assert observed, "the structural answer short-circuited the measurement"
    assert not safe, "an observed row loss was overruled by the source not looking like pruning"
    assert evidence.startswith("observed")


def test_the_structural_answer_still_decides_when_nothing_can_be_observed(monkeypatch):
    """It is the fallback now, not the gate -- and it can only withdraw safety.

    A build that cannot be probed at all (no `sql` extra, no writable temp) gets
    the old treatment, because there is nothing better to go on.
    """
    from zamboni import capabilities

    monkeypatch.setattr(capabilities, "_pruning_behaves", lambda: None)

    safe, evidence = capabilities._derivation_is_correct(prunes=False)
    assert safe
    assert "not observed" in evidence

    safe, evidence = capabilities._derivation_is_correct(prunes=True)
    assert not safe
    assert "unknown" in evidence


def test_a_pruning_build_is_settled_by_observation(monkeypatch):
    from zamboni import capabilities

    monkeypatch.setattr(capabilities, "_pruning_behaves", lambda: True)
    safe, evidence = capabilities._derivation_is_correct(prunes=True)
    assert safe
    assert evidence.startswith("observed")

    monkeypatch.setattr(capabilities, "_pruning_behaves", lambda: False)
    safe, evidence = capabilities._derivation_is_correct(prunes=True)
    assert not safe
    assert evidence.startswith("observed")


def test_an_unanswerable_probe_is_treated_as_unsafe(monkeypatch):
    """`None` means we could not establish the answer, which is not the same as
    establishing a good one. Refusing costs a skipped run; guessing costs rows."""
    from zamboni import capabilities

    monkeypatch.setattr(capabilities, "_pruning_behaves", lambda: None)

    safe, evidence = capabilities._derivation_is_correct(prunes=True)

    assert not safe
    assert "unknown" in evidence


def test_the_behavioural_probe_agrees_with_the_installed_build():
    """The probe itself, run for real against whatever PyIceberg is installed.

    Costs a few hundred milliseconds and earns it: this is the only test that
    exercises the probe end to end, and a probe nobody has watched work is a
    probe that quietly returns None.
    """
    from zamboni.capabilities import _pruning_behaves, detect

    observed = _pruning_behaves()

    assert observed is not None, "the probe could not run; it would report 'unknown'"
    # 0.11.1 does not prune, so correctness here is trivially true; on a 0.12
    # that prunes, this is the real answer.
    assert (
        observed is detect().manifest_pruning_is_safe or not detect().prunes_manifests_by_predicate
    )


def test_no_symbol_can_override_what_was_observed(monkeypatch):
    """The point of ZMBNI-1109, pinned behaviourally.

    `_build_delete_files_partition_predicate` is present on a 0.12 candidate which
    corrupts data, *and* on the builds that fix it. Its existence was never the
    property. So removing it must not change the verdict, and adding one back
    must not either -- if either did, a name would be deciding safety again and
    the corrupting build would be declared safe.
    """
    from pyiceberg.table.update.snapshot import _SnapshotProducer

    from zamboni import capabilities

    monkeypatch.setattr(capabilities, "_pruning_behaves", lambda: False)

    monkeypatch.setattr(
        _SnapshotProducer, "_build_delete_files_partition_predicate", lambda *a: None, raising=False
    )
    with_symbol, _ = capabilities._derivation_is_correct(prunes=True)

    monkeypatch.delattr(_SnapshotProducer, "_build_delete_files_partition_predicate", raising=False)
    without_symbol, _ = capabilities._derivation_is_correct(prunes=True)

    assert with_symbol is without_symbol is False, (
        "the presence of a private symbol changed the safety verdict; that "
        "symbol exists on corrupting builds too"
    )


# -- added files and their partition spec (ZMBNI-59) ----------------------


def test_evolution_stays_available_whichever_build_is_installed(monkeypatch):
    """Partition evolution is no longer withdrawn, because it no longer has to be.

    It was, and the reasoning held at the time: evolution is definitionally the
    case where a run adds files under more than one spec, and a library that
    writes every added file under the table default turns that into an
    `IndexError` from the Avro writer -- a partition `Record` has the arity of
    the spec that produced it -- four frames down and naming nothing.

    What changed is that the capability no longer depends on the library.
    `MultiSpecReplaceFiles` carries a probe-gated fallback, so a build whose
    `added_files_honour_spec` is false gets the behaviour from the committer
    instead of losing the feature. Declaring it either way is therefore the
    truthful declaration -- and `engines` must not advertise less than a run
    will do, any more than more.
    """
    from zamboni.maintainers import LayoutFeature, Operation, Support
    from zamboni.maintainers import local as local_module
    from zamboni.maintainers.local import LocalMaintainer

    for honoured in (True, False):
        monkeypatch.setattr(
            local_module, "detect", lambda h=honoured: replace(detect(), added_files_honour_spec=h)
        )
        caps = LocalMaintainer.capabilities()

        assert LayoutFeature.PARTITION_EVOLUTION in caps.layout, (
            f"added_files_honour_spec={honoured}: evolution is available on both, "
            "via the library or via the fallback"
        )
        assert caps.layout == frozenset(LayoutFeature), "no layout feature is withdrawn"
        assert caps.of(Operation.COMPACT).support is not Support.UNSUPPORTED


def test_the_probe_answers_no_rather_than_dont_know_when_the_commit_is_refused():
    """`False` and `None` are different answers and lead somewhere different.

    A build that cannot write an added file under its own spec *raises* when the
    commit is attempted, and that refusal is the answer. `None` is reserved for
    a probe that could not run at all -- no `sql` extra, no writable temp
    directory -- which an operator resolves differently.
    """
    from tests.conftest import library_honours_spec_expected
    from zamboni import capabilities

    if not library_honours_spec_expected():
        # On a stock build the probe's *answer* is False, which is this test's
        # other half: `False` rather than `None`. Asserted below.
        assert capabilities._added_files_honour_their_spec() is False
        return
    assert capabilities._added_files_honour_their_spec() is True, (
        "the installed build should honour an added file's spec; if this fails, "
        "check that pyiceberg resolves to the maintenance fork"
    )
