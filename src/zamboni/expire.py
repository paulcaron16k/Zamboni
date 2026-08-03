"""Snapshot expiry, and deletion of the files it orphans.

PyIceberg's ``expire_snapshots()`` is metadata-only: it emits a
``RemoveSnapshotsUpdate`` and deletes nothing. It also implements almost none of
the spec's retention rules -- ``older_than()`` protects branch and tag heads and
otherwise expires anything older than a timestamp, ignoring
``min-snapshots-to-keep``, branch ancestry, and ``max-ref-age-ms``.

So this module does two things PyIceberg will not:

1. :func:`decide_retention` implements the retention algorithm from the Iceberg
   spec ("Snapshot Retention Policy") as a pure function, and feeds the result
   to ``ExpireSnapshots().by_ids()``.
2. :class:`SnapshotExpirer` deletes exactly the files that were reachable
   before the expiry and are not reachable after it.

Computing the difference rather than listing storage is what makes this safe:
a file that was never referenced by any snapshot cannot appear in the diff, so
expiry structurally cannot delete a concurrent writer's in-flight output. That
class of file is orphan removal's problem, and it needs an age guard precisely
because it *does* look at raw storage.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field

from pyiceberg.manifest import DataFileContent
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRefType
from pyiceberg.table.snapshots import Snapshot, ancestors_of

from .reachable import canonical, reachable_files

logger = logging.getLogger(__name__)

#: Iceberg spec defaults, used when neither the config nor a table property says.
DEFAULT_MAX_SNAPSHOT_AGE_MS = 5 * 24 * 60 * 60 * 1000
DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 1

PROP_MAX_SNAPSHOT_AGE = "history.expire.max-snapshot-age-ms"
PROP_MIN_SNAPSHOTS = "history.expire.min-snapshots-to-keep"
PROP_MAX_REF_AGE = "history.expire.max-ref-age-ms"


class ExpiryAborted(RuntimeError):
    """A safety check failed; nothing was deleted."""


@dataclass(frozen=True)
class RetentionPolicy:
    max_snapshot_age_ms: int = DEFAULT_MAX_SNAPSHOT_AGE_MS
    min_snapshots_to_keep: int = DEFAULT_MIN_SNAPSHOTS_TO_KEEP
    max_ref_age_ms: int | None = None

    @classmethod
    def resolve(
        cls,
        table_properties: dict[str, str],
        *,
        max_snapshot_age_days: int | None = None,
        min_snapshots_to_keep: int | None = None,
        max_ref_age_days: int | None = None,
    ) -> RetentionPolicy:
        """Config wins, then the Iceberg table property, then the spec default."""

        def prop(name: str, default: int | None) -> int | None:
            raw = table_properties.get(name)
            if raw is None:
                return default
            try:
                return int(raw)
            except ValueError:
                raise ValueError(f"table property {name!r} is not an integer: {raw!r}") from None

        return cls(
            max_snapshot_age_ms=(
                _days_to_ms(max_snapshot_age_days)
                if max_snapshot_age_days is not None
                else prop(PROP_MAX_SNAPSHOT_AGE, DEFAULT_MAX_SNAPSHOT_AGE_MS)
            ),
            min_snapshots_to_keep=(
                min_snapshots_to_keep
                if min_snapshots_to_keep is not None
                else prop(PROP_MIN_SNAPSHOTS, DEFAULT_MIN_SNAPSHOTS_TO_KEEP)
            ),
            max_ref_age_ms=(
                _days_to_ms(max_ref_age_days)
                if max_ref_age_days is not None
                else prop(PROP_MAX_REF_AGE, None)
            ),
        )


def _days_to_ms(days: int) -> int:
    return int(days * 24 * 60 * 60 * 1000)


@dataclass(frozen=True)
class RetentionDecision:
    retain: frozenset[int]
    expire: frozenset[int]
    #: Non-main refs whose snapshot is older than ``max-ref-age-ms``. Reported
    #: but **not** dropped -- see the note in :func:`decide_retention`.
    stale_refs: frozenset[str]

    def describe(self) -> str:
        lines = [f"retain {len(self.retain)} snapshot(s), expire {len(self.expire)}"]
        if self.stale_refs:
            lines.append(
                f"  {len(self.stale_refs)} ref(s) past max-ref-age-ms, kept: "
                + ", ".join(sorted(self.stale_refs))
            )
        return "\n".join(lines)


def decide_retention(
    metadata: TableMetadata, policy: RetentionPolicy, *, now: dt.datetime | None = None
) -> RetentionDecision:
    """Which snapshots survive, per the Iceberg spec's retention algorithm.

    The spec's steps, in order:

    1. Start with an empty retain set.
    2. Drop refs other than ``main`` older than ``max-ref-age-ms``.
    3. Add every branch and tag head.
    4. For each branch, walk ancestors adding each until the snapshot is *both*
       older than ``max-snapshot-age-ms`` *and* not among the first
       ``min-snapshots-to-keep`` of that branch.
    5. Expire everything else.

    Step 2 is **detected but not applied**: dropping a ref is a separate table
    update, and retaining a stale ref is the conservative error. Its snapshots
    stay reachable, so the only cost is reclaiming less.
    """
    now_ms = int((now or dt.datetime.now(dt.UTC)).timestamp() * 1000)
    by_id = {s.snapshot_id: s for s in metadata.snapshots}

    stale_refs = set()
    if policy.max_ref_age_ms is not None:
        for name, ref in metadata.refs.items():
            if name == MAIN_BRANCH:
                continue  # the spec: "The main branch never expires."
            snapshot = by_id.get(ref.snapshot_id)
            if snapshot and now_ms - snapshot.timestamp_ms > policy.max_ref_age_ms:
                stale_refs.add(name)

    retain: set[int] = set()
    for ref in metadata.refs.values():
        snapshot = by_id.get(ref.snapshot_id)
        if snapshot is None:
            continue
        retain.add(snapshot.snapshot_id)  # step 3: every branch and tag head
        if ref.snapshot_ref_type != SnapshotRefType.BRANCH:
            continue

        # Per-ref overrides beat the table-level policy (spec, snapshot refs).
        max_age = ref.max_snapshot_age_ms or policy.max_snapshot_age_ms
        min_keep = ref.min_snapshots_to_keep or policy.min_snapshots_to_keep

        for position, ancestor in enumerate(ancestors_of(snapshot, metadata)):
            too_old = now_ms - ancestor.timestamp_ms > max_age
            within_min = position < min_keep
            if too_old and not within_min:
                break
            retain.add(ancestor.snapshot_id)

    return RetentionDecision(
        retain=frozenset(retain),
        expire=frozenset(by_id) - frozenset(retain),
        stale_refs=frozenset(stale_refs),
    )


def files_of_snapshots(tbl: Table, snapshot_ids: frozenset[int]) -> dict[str, str]:
    """Files reachable from ``snapshot_ids``: canonical key -> original location.

    The original location is kept because that is what ``FileIO.delete`` wants;
    the canonical key is what set arithmetic needs. Metadata JSON and statistics
    are deliberately excluded -- they are not snapshot-scoped, so expiry does
    not orphan them and orphan removal handles the leftovers.
    """
    found: dict[str, str] = {}
    by_id = {s.snapshot_id: s for s in tbl.metadata.snapshots}

    for snapshot_id in snapshot_ids:
        snapshot = by_id.get(snapshot_id)
        if snapshot is None:
            continue
        found[canonical(snapshot.manifest_list)] = snapshot.manifest_list
        for manifest in snapshot.manifests(io=tbl.io):
            found[canonical(manifest.manifest_path)] = manifest.manifest_path
            for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=False):
                data_file = entry.data_file
                if data_file.content in (
                    DataFileContent.DATA,
                    DataFileContent.POSITION_DELETES,
                    DataFileContent.EQUALITY_DELETES,
                ):
                    found[canonical(data_file.file_path)] = data_file.file_path
    return found


@dataclass
class ExpireResult:
    identifier: str
    expired_snapshots: int = 0
    retained_snapshots: int = 0
    deleted_files: int = 0
    failed_deletes: int = 0
    stale_refs: list[str] = field(default_factory=list)
    dry_run: bool = False

    def describe(self) -> str:
        verb = "would delete" if self.dry_run else "deleted"
        lines = [
            (
                f"{self.identifier}: expired {self.expired_snapshots} snapshot(s), "
                f"retained {self.retained_snapshots}, {verb} {self.deleted_files} file(s)"
            )
        ]
        if self.failed_deletes:
            lines.append(f"  {self.failed_deletes} file(s) could not be deleted")
        if self.stale_refs:
            lines.append(
                "  refs past max-ref-age-ms are reported, not dropped: "
                + ", ".join(sorted(self.stale_refs))
            )
        return "\n".join(lines)


class SnapshotExpirer:
    """Applies a retention policy and deletes what it orphans."""

    def __init__(self, policy: RetentionPolicy, *, dry_run: bool = False) -> None:
        self._policy = policy
        self._dry_run = dry_run

    def run(self, tbl: Table, *, now: dt.datetime | None = None) -> ExpireResult:
        identifier = ".".join(tbl.name())
        decision = decide_retention(tbl.metadata, self._policy, now=now)
        result = ExpireResult(
            identifier=identifier,
            retained_snapshots=len(decision.retain),
            stale_refs=sorted(decision.stale_refs),
            dry_run=self._dry_run,
        )
        if not decision.expire:
            return result

        # Files only the doomed snapshots reference. Computed before the commit
        # so a dry run can report it, and so the deletion set never depends on
        # what storage happens to contain.
        doomed = files_of_snapshots(tbl, decision.expire)
        for key in files_of_snapshots(tbl, decision.retain):
            doomed.pop(key, None)

        result.expired_snapshots = len(decision.expire)
        result.deleted_files = len(doomed)
        if self._dry_run:
            return result

        tbl.maintenance.expire_snapshots().by_ids(sorted(decision.expire)).commit()
        tbl.refresh()

        # Cheap post-check against a logic error in the diff: nothing we are
        # about to delete may still be referenced.
        still_referenced = reachable_files(tbl).paths & set(doomed)
        if still_referenced:
            raise ExpiryAborted(
                f"{identifier}: {len(still_referenced)} file(s) marked for deletion are still "
                "referenced after expiry; nothing was deleted"
            )

        result.deleted_files, result.failed_deletes = _delete_all(tbl, doomed.values())
        return result


def _delete_all(tbl: Table, paths) -> tuple[int, int]:
    """Best effort: one unreadable object must not abandon the rest."""
    deleted = failed = 0
    for path in paths:
        try:
            tbl.io.delete(path)
            deleted += 1
        except Exception as exc:
            failed += 1
            logger.warning("could not delete %s: %s", path, exc)
    return deleted, failed


def latest_snapshot(metadata: TableMetadata) -> Snapshot | None:
    return metadata.current_snapshot() if metadata.snapshots else None
