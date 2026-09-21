# SPDX-License-Identifier: Apache-2.0
"""The probe cache must answer identically to probing, or not answer at all.

`detect()` decides whether partition evolution is safe to attempt, whether a
compaction may proceed past equality deletes, and which of two `_ReplaceFiles`
implementations runs. A cache that serves a stale answer to any of those is
worse than no cache, so these tests are about *when it declines*, not about the
speed it buys.
"""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from zamboni import capabilities, probecache


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Keep every test off the real cache, and out of each other's."""
    monkeypatch.setenv("ZAMBONI_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.delenv("ZAMBONI_NO_PROBE_CACHE", raising=False)
    capabilities.detect.cache_clear()
    yield
    capabilities.detect.cache_clear()


class _Dist:
    def __init__(self, version="1.0", record="a,b,c\n", direct_url=None):
        self.version = version
        self._files = {"RECORD": record, "direct_url.json": direct_url}

    def read_text(self, name):
        return self._files.get(name)


def _installed(monkeypatch, **dists):
    monkeypatch.setattr(probecache, "distribution", lambda name: dists[name])


@pytest.mark.parametrize(
    ("provenance", "editable"),
    [
        ('{"dir_info": {"editable": true}}', True),
        ('{"dir_info":{"editable":true}}', True),
        ('{"dir_info": {"editable": false}}', False),
        ('{"vcs_info": {"commit_id": "abc"}}', False),
        ("}not json{", True),
    ],
)
def test_editable_detection_survives_formatting(provenance, editable):
    """Parsed, not pattern-matched -- and unparseable counts as editable.

    An earlier draft tested for the substring `"editable": true` against JSON
    with its spaces stripped, so a compact `direct_url.json` read as
    non-editable and this checkout cached against its own working tree.
    """
    assert probecache._is_editable(provenance) is editable


def test_an_editable_install_of_either_package_disables_the_cache(monkeypatch):
    """Zamboni's own editable install counts, not just PyIceberg's.

    The probes ask about PyIceberg, but the code deciding what the answers mean
    is Zamboni's, and an editable install of it changes under `git checkout`
    with no install-level signal at all.
    """
    editable = _Dist(direct_url='{"dir_info": {"editable": true}}')
    for editable_package in ("pyiceberg", "iceberg-zamboni"):
        dists = {"pyiceberg": _Dist(), "iceberg-zamboni": _Dist()}
        dists[editable_package] = editable
        _installed(monkeypatch, **dists)
        assert probecache.build_signature() is None, editable_package


def test_a_build_with_no_manifest_is_not_cached(monkeypatch):
    """No RECORD means no way to notice the files changing."""
    _installed(monkeypatch, pyiceberg=_Dist(record=None), **{"iceberg-zamboni": _Dist()})
    assert probecache.build_signature() is None


def test_same_version_different_content_is_a_different_key(monkeypatch):
    """The reason ZMBNI-39's version-keyed table was closed as not planned.

    The maintenance fork and stock PyPI both declare `0.12.0` and answer
    `added_files_honour_spec` differently. Keyed on the version they collide;
    keyed on the RECORD they do not.
    """
    _installed(monkeypatch, pyiceberg=_Dist("0.12.0", "stock\n"), **{"iceberg-zamboni": _Dist()})
    stock = probecache.build_signature()
    _installed(monkeypatch, pyiceberg=_Dist("0.12.0", "fork\n"), **{"iceberg-zamboni": _Dist()})
    fork = probecache.build_signature()
    assert stock and fork and stock != fork


def test_a_vcs_commit_moves_the_key(monkeypatch):
    """Same version and RECORD hash, different commit recorded by the installer."""
    at = '{{"vcs_info": {{"commit_id": "{}"}}}}'
    _installed(
        monkeypatch, pyiceberg=_Dist(direct_url=at.format("a" * 40)), **{"iceberg-zamboni": _Dist()}
    )
    first = probecache.build_signature()
    _installed(
        monkeypatch, pyiceberg=_Dist(direct_url=at.format("b" * 40)), **{"iceberg-zamboni": _Dist()}
    )
    assert first != probecache.build_signature()


def test_the_schema_version_invalidates_everything(monkeypatch):
    """The lever for a change in probe *logic* that no install signature shows."""
    _installed(monkeypatch, pyiceberg=_Dist(), **{"iceberg-zamboni": _Dist()})
    before = probecache.build_signature()
    monkeypatch.setattr(probecache, "CACHE_SCHEMA", probecache.CACHE_SCHEMA + 1)
    assert before != probecache.build_signature()


def test_answers_survive_a_round_trip():
    assert probecache.store("sig", {"a": True}) is not None
    assert probecache.load("sig") == {"a": True}


def test_one_build_never_reads_another_builds_entry():
    """Entries are keyed by signature, so both can exist and neither is stale.

    Measured on a real venv: installing the fork over stock and back again hit
    the original entry rather than re-probing.
    """
    probecache.store("sig-a", {"honours_spec": True})
    probecache.store("sig-b", {"honours_spec": False})
    assert probecache.load("sig-a") == {"honours_spec": True}
    assert probecache.load("sig-b") == {"honours_spec": False}
    assert probecache.load("sig-c") is None


def test_a_tampered_file_is_ignored_rather_than_trusted(tmp_path):
    """The signature is inside the document as well as in its name."""
    path = probecache.store("sig", {"a": True})
    path.write_text(json.dumps({"signature": "someone else", "answers": {"a": False}}))
    assert probecache.load("sig") is None


@pytest.mark.parametrize("content", ["}not json{", "[]", '{"signature": "sig"}'])
def test_unreadable_content_means_probe_rather_than_raise(content):
    path = probecache.store("sig", {"a": True})
    path.write_text(content)
    assert probecache.load("sig") is None


def test_nowhere_writable_costs_time_and_nothing_else(tmp_path, monkeypatch):
    """A read-only filesystem degrades to probing.

    Deliberately unlike the spill directory, which raises `WorkspaceUnavailable`
    and exits 2: a rewrite that cannot spill cannot proceed, whereas a run that
    cannot cache is merely slower.
    """
    locked = tmp_path / "locked"
    locked.mkdir(mode=0o500)
    monkeypatch.setenv("ZAMBONI_CACHE_DIR", str(locked / "under"))
    assert probecache.store("sig", {"a": True}) is None
    assert probecache.load("sig") is None


def test_detect_returns_the_stored_answers_without_probing_again(monkeypatch):
    """The whole point: a second process pays no probe cost.

    `lru_cache` already covers one process. This asserts the part that survives
    it, by making a second probe an error rather than by timing one.
    """
    monkeypatch.setattr(probecache, "build_signature", lambda: "fixed-signature")
    probed = capabilities.detect()

    capabilities.detect.cache_clear()
    monkeypatch.setattr(
        capabilities, "_probe", lambda: pytest.fail("re-probed a build already in the cache")
    )
    assert capabilities.detect() == probed
    assert capabilities.cache_status().startswith("hit")


def test_detect_reprobes_when_the_build_moves(monkeypatch):
    """Same class, different key: the stored answers must not be reused."""
    monkeypatch.setattr(probecache, "build_signature", lambda: "build-one")
    first = capabilities.detect()

    capabilities.detect.cache_clear()
    monkeypatch.setattr(probecache, "build_signature", lambda: "build-two")
    other = replace(first, added_files_honour_spec=not first.added_files_honour_spec)
    monkeypatch.setattr(capabilities, "_probe", lambda: other)
    assert capabilities.detect() == other, "served an answer keyed to a different build"


def test_detect_probes_when_the_build_cannot_be_keyed(monkeypatch):
    monkeypatch.setattr(probecache, "build_signature", lambda: None)
    probed = capabilities._probe()
    marked = replace(probed, pruning_evidence="probed just now")
    monkeypatch.setattr(capabilities, "_probe", lambda: marked)
    assert capabilities.detect() == marked
    assert "disabled" in capabilities.cache_status()


def test_the_cache_can_be_turned_off(monkeypatch):
    monkeypatch.setenv("ZAMBONI_NO_PROBE_CACHE", "1")
    monkeypatch.setattr(
        probecache, "build_signature", lambda: pytest.fail("consulted the cache anyway")
    )
    marked = replace(capabilities._probe(), pruning_evidence="probed just now")
    monkeypatch.setattr(capabilities, "_probe", lambda: marked)
    assert capabilities.detect() == marked


def test_this_environment_caches_exactly_when_nothing_is_editable():
    """Derived from the environment, not hardcoded.

    A developer checkout installs Zamboni editable and must not cache; the
    consumer CI leg installs a wheel and must. One assertion covers both,
    instead of a test that only ever runs one of the two paths.
    """
    from importlib.metadata import distribution

    editable = any(
        probecache._is_editable(distribution(name).read_text("direct_url.json") or "{}")
        for name in probecache._SIGNED
    )
    assert (probecache.build_signature() is None) is editable
