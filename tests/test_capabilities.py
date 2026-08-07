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
        if field.name == "version":
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


def test_the_derivation_probe_survives_a_rename():
    """A single-name `hasattr` turned an upstream rename into "unusable".

    PyIceberg called this `_build_delete_files_partition_predicate` when manifest
    pruning first appeared, and renamed it to
    `_build_delete_files_partition_filters` in the fix for
    apache/iceberg-python#3758 -- because the predicate moved from the
    source-column domain to the partition-field domain, which is what makes
    pruning correct across transforms.

    Matching one name meant Zamboni refused to run on every build carrying that
    fix. The safe direction, but still wrong.
    """
    from zamboni.capabilities import DERIVATION_METHODS, _derives_delete_predicate

    assert len(DERIVATION_METHODS) >= 2, "a single name is what caused the false negative"

    for name in DERIVATION_METHODS:
        assert _derives_delete_predicate(type("Producer", (), {name: lambda self: None}))

    assert not _derives_delete_predicate(type("Producer", (), {}))
