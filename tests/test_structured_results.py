"""`as_dict()` is a promised surface; these are the promises (ZMBNI-32).

`describe()` is prose and deliberately not covered by docs/releasing.md -- a
message improves without a version bump. `as_dict()` is the opposite: an
integrator keys a dashboard on it, so the keys are the contract and they are
written out here rather than derived from the implementation. Deriving them would
make every assertion tautological, which is exactly the trap when the thing under
test is a mapping.
"""

from __future__ import annotations

import json

import pytest

from zamboni.compactor import CompactionResult, GroupResult
from zamboni.deletes import DanglingReport, DeleteCleanupResult
from zamboni.expire import ExpireResult
from zamboni.maintainers import Operation
from zamboni.maintainers.spark import SparkResult
from zamboni.maintainers.trino import TrinoResult
from zamboni.maintenance import MaintenanceReport, Outcome
from zamboni.manifests import RewritePlan, RewriteResult
from zamboni.orphans import OrphanResult
from zamboni.properties import ApplyResult, PropertyChange

#: The promised key set per result. A key removed or renamed here is a breaking
#: change under docs/releasing.md; a key added is not.
PROMISED = {
    "compact": {
        "operation",
        "table",
        "data_files_rewritten",
        "data_files_added",
        "bytes_rewritten",
        "bytes_added",
        "groups_rewritten",
        "groups_evolved",
        "groups_skipped",
        "dangling_delete_files",
        "warnings",
        "snapshot_ids",
        "dry_run",
    },
    "expire": {
        "operation",
        "table",
        "snapshots_expired",
        "snapshots_retained",
        "files_deleted",
        "deletes_failed",
        "stale_refs",
        "dry_run",
    },
    "remove-orphans": {
        "operation",
        "table",
        "files_scanned",
        "files_referenced",
        "orphans_found",
        "orphan_bytes",
        "files_deleted",
        "bytes_deleted",
        "deletes_failed",
        "too_young",
        "too_young_bytes",
        "dry_run",
        "roots",
    },
    "remove-dangling-deletes": {
        "operation",
        "table",
        "delete_files",
        "dangling_files",
        "removable",
        "removable_bytes",
        "stuck",
        "manifests_dropped",
        "files_removed",
        "bytes_removed",
        "snapshot_id",
        "dry_run",
    },
    "rewrite-manifests": {
        "operation",
        "table",
        "manifests_before",
        "manifests_after",
        "manifests_replaced",
        "manifests_kept",
        "manifests_written",
        "entries",
        "partition_spread_before",
        "partition_spread_after",
        "snapshot_id",
        "dry_run",
    },
    "apply-properties": {"operation", "table", "changes", "changed", "dry_run"},
}


def results() -> list:
    """One instance of every result type, populated enough to be meaningful."""
    return [
        CompactionResult(
            "db.t",
            rewritten_data_files=6,
            added_data_files=1,
            groups=[GroupResult("p=1", 6, 1, 600, 100, 42)],
            warnings=["a warning"],
        ),
        ExpireResult("db.t", expired_snapshots=3, retained_snapshots=5, stale_refs=["tag1"]),
        OrphanResult("db.t", scanned=17, referenced=17, roots=["s3://b/t"]),
        DeleteCleanupResult(DanglingReport("db.t", delete_files=2, dangling_files=2), removed=2),
        RewriteResult(RewritePlan("db.t", entries=6), manifests_before=6, manifests_after=1),
        ApplyResult(
            "db.t", changes=[PropertyChange("write.metadata.previous-versions-max", None, "3")]
        ),
    ]


@pytest.mark.parametrize("result", results(), ids=lambda r: type(r).__name__)
def test_every_result_serialises_to_json(result):
    """What the caller actually does with it, so assert it rather than assume.

    A PyIceberg `DataFile` or `ManifestFile` reaching one of these dicts fails
    here -- which is the point. `DanglingReport.removable` and
    `RewritePlan.replaced` both hold such objects, and both are reported as
    counts precisely so this passes.
    """
    payload = result.as_dict()
    round_tripped = json.loads(json.dumps(payload))
    assert round_tripped == payload, "as_dict() must survive a JSON round trip unchanged"


@pytest.mark.parametrize("result", results(), ids=lambda r: type(r).__name__)
def test_every_result_names_its_operation(result):
    """So a consumer can branch without walking the type."""
    payload = result.as_dict()
    assert "operation" in payload, "as_dict() must say which operation it reports"
    assert payload["operation"] in {op.value for op in Operation}, (
        f"{payload['operation']!r} is not one of the six Operation values"
    )


@pytest.mark.parametrize("result", results(), ids=lambda r: type(r).__name__)
def test_the_promised_keys_are_present(result):
    """The contract, written down rather than derived from the implementation."""
    payload = result.as_dict()
    promised = PROMISED[payload["operation"]]
    missing = promised - set(payload)
    assert not missing, (
        f"{type(result).__name__}.as_dict() dropped promised key(s) {sorted(missing)}. "
        "Removing or renaming one is breaking under docs/releasing.md"
    )


def test_every_operation_has_a_promised_key_set():
    """A new operation must declare its keys, not inherit silence.

    Derived from the `Operation` enum, so adding a seventh fails here until its
    contract is written down.
    """
    covered = set(PROMISED)
    declared = {op.value for op in Operation}
    assert covered == declared, (
        f"promised key sets cover {sorted(covered)} but Operation declares {sorted(declared)}"
    )


@pytest.mark.parametrize(
    "result",
    [
        TrinoResult(Operation.COMPACT, "db.t", "ALTER TABLE x EXECUTE optimize"),
        SparkResult(
            Operation.COMPACT, "db.t", "CALL sys.rewrite_data_files(...)", rows=(("a", 1),)
        ),
    ],
    ids=["trino", "spark"],
)
def test_engine_results_serialise_and_name_their_engine(result):
    """A tuple row from a driver must not reach a promised key as its own type."""
    payload = result.as_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert payload["engine"] in {"trino", "spark"}
    assert all(isinstance(row, str) for row in payload["rows"]), (
        "rows must be stringified; a driver's own row type must not reach the contract"
    )


def test_a_whole_run_serialises_in_one_call():
    """The reason `Outcome` and `MaintenanceReport` carry it too: an integrator
    streams outcomes through `observer=` and serialises the lot for a scheduler."""
    report = MaintenanceReport(
        (
            Outcome("db.t", Operation.COMPACT, 0, "did the thing", result=results()[0]),
            Outcome("db.t", Operation.EXPIRE, 0, "disabled in the config (expire_snapshots)"),
            Outcome("db.t", Operation.REMOVE_ORPHANS, 3, "blocked"),
        )
    )
    payload = report.as_dict()
    assert json.loads(json.dumps(payload)) == payload

    assert payload["exit_code"] == 3, "the worst outcome, as the CLI would exit"
    assert payload["failures"] == 1
    assert payload["tables"] == ["db.t"]

    first, skipped, _ = payload["outcomes"]
    assert first["result"]["data_files_rewritten"] == 6
    assert skipped["result"] is None, "a skip produces no result"
    assert skipped["detail"], "and its reason survives only in detail, so it must be there"


@pytest.mark.parametrize("result", results(), ids=lambda r: type(r).__name__)
def test_every_result_says_whether_it_was_a_preview(result):
    """Because the counters are what *would* happen, not what did.

    Measured: `expire` on the `db.aged` fixture reports `files_deleted=4` from a
    dry run that left storage byte-identical, and `rewrite-manifests` on
    `db.partitioned` reports `manifests_after=1` from both a preview and the real
    run after it. That is a coherent design -- a projection is what a preview is
    for, and `expire.py:301-311` computes the set before the commit so a dry run
    can report it -- but it makes `dry_run` the only thing separating "reclaimed
    4 files" from "would reclaim 4 files".

    `CompactionResult` was the one result carrying no such field, which nothing
    noticed while the counters were prose. A consumer summing `bytes_rewritten`
    across runs without filtering on this key would report bytes that were never
    rewritten.
    """
    payload = result.as_dict()
    assert "dry_run" in payload, (
        f"{type(result).__name__}.as_dict() cannot say whether it was a preview, "
        "and its counters are projections -- so a consumer would read a preview "
        "as work that happened"
    )
    assert isinstance(payload["dry_run"], bool)
