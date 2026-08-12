"""Capability probing and the decisions it drives."""

from __future__ import annotations

import dataclasses

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


def _caps(**overrides) -> PyIcebergCapabilities:
    base = {
        "version": "test",
        "operation_is_injectable": True,
        "replace_summary_supported": False,
        "streaming_write_supported": False,
        "prunes_manifests_by_predicate": False,
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
    "prunes,derives",
    [
        (False, False),  # 0.11.1: scans every manifest
        (True, True),  # main/0.12: prunes, and derives the predicate
        (False, True),
    ],
)
def test_safe_pruning_combinations(prunes, derives):
    caps = _caps(prunes_manifests_by_predicate=prunes, derives_delete_predicate=derives)
    assert caps.manifest_pruning_is_safe
    assert caps.unsupported_reason() is None


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


def test_a_build_that_does_not_prune_needs_no_probe():
    """0.11.1 is every current user, and pays nothing for this.

    The expensive check exists for a hazard that only arrives with pruning, so
    a build without pruning short-circuits before touching the filesystem.
    """
    from zamboni.capabilities import _derivation_is_correct

    safe, evidence = _derivation_is_correct(prunes=False)

    assert safe
    assert "not applicable" in evidence


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

    `_build_delete_files_partition_predicate` is present on 0.12.0rc1, which
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
