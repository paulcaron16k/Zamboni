"""Capability probing and the decisions it drives."""

from __future__ import annotations

import dataclasses

import pytest

from icemaint.capabilities import PyIcebergCapabilities, detect


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
    import icemaint.profile as profile_module

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
