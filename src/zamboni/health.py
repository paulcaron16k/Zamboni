# SPDX-License-Identifier: Apache-2.0
"""Is this table due for maintenance, and why -- from one metadata load.

Zamboni is told when to run. It decides a great deal once running -- which
operations are configured, which the engine supports, which partitions are
candidates -- but not the first question: **does this table need anything at
all?** This module answers that cheaply enough to ask about every table in a
fleet before doing any work.

**The signals are Iceberg's, already published.** Every snapshot carries a
summary, and a `load_table` returns them: ``total-data-files``,
``total-files-size``, ``total-delete-files``, ``total-position-deletes``,
``total-equality-deletes``, ``total-records``. Table metadata adds the snapshot
count and the metadata log. Nothing here reads a manifest, lists storage, or
opens a data file.

**Measured on the dev stack** (one table, 631 files, 159 snapshots), which is
the whole argument for asking before working:

===========================================  ==========  =============================
tier                                               cost  answers
===========================================  ==========  =============================
this module (``load_table``)                  20-38 ms   is compaction or expiry due?
:func:`zamboni.profile.profile_table`        391-402 ms  which partitions, manifests?
listing + :func:`zamboni.reachable`         ~2,035 ms    are there orphans?
===========================================  ==========  =============================

The load dominates entirely: :func:`table_health` against an *already loaded*
table measured **0.018 ms**, so a caller that already holds the table pays
nothing to ask, and a fleet sweep pays one catalog round trip per table and no
more.

**It says what it cannot see.** Three of the six signals in docs/runbook.md §3 --
files per partition, manifests per data file, metadata bytes against data bytes
-- need the manifests, and unreferenced files needs a storage listing. A health
result that silently omitted them would read as "no problems found" when it is
really "no problems visible from here", which is the more dangerous of the two.
:attr:`TableHealth.unseen` names them, and the deeper tiers above answer them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.table import Table

#: The summary key every snapshot-producing operation stamps. Three of the six
#: operations produce a snapshot at all -- compaction, `rewrite-manifests` and
#: `remove-dangling-deletes` -- and all three stamp it. `expire` removes
#: snapshots rather than adding one, and `remove-orphans` and `apply-properties`
#: produce none, so there is nothing for them to mark and the watermark is
#: silent about them by construction.
OPERATION_STAMP = "zamboni.operation"

#: Iceberg's own summary keys. Spelled here rather than inlined so a rename
#: upstream is one edit and a grep finds every use.
TOTAL_DATA_FILES = "total-data-files"
TOTAL_FILES_SIZE = "total-files-size"
TOTAL_DELETE_FILES = "total-delete-files"
TOTAL_POSITION_DELETES = "total-position-deletes"
TOTAL_EQUALITY_DELETES = "total-equality-deletes"
TOTAL_RECORDS = "total-records"

#: What this tier cannot answer, and which one can. Reported rather than
#: omitted: "no problems found" and "no problems visible from here" are
#: different claims, and only one of them is true here.
UNSEEN_FROM_METADATA = (
    "data files per partition (needs the manifests -- profile_table)",
    "manifests per data file (needs the manifests -- profile_table)",
    "metadata bytes vs data bytes (needs manifest and metadata file sizes)",
    "unreferenced files (needs a storage listing -- remove-orphans)",
)


@dataclass(frozen=True)
class TableHealth:
    """What one metadata load can say about a table's layout.

    Every field is either read straight from a snapshot summary or divided out
    of two that were. A value is ``None`` when the summary did not carry what it
    needed -- an older writer, or a table whose snapshots predate the key --
    rather than 0, because "not reported" and "zero" lead to opposite decisions.
    """

    identifier: str
    #: The snapshot these numbers describe. ``None`` for a table with no
    #: snapshots at all, which is a table nothing has written to yet.
    snapshot_id: int | None
    data_files: int | None = None
    total_bytes: int | None = None
    delete_files: int | None = None
    position_deletes: int | None = None
    equality_deletes: int | None = None
    records: int | None = None
    #: Snapshots retained in metadata, and entries in the metadata log. Both are
    #: expiry pressure and neither appears in a snapshot summary -- they are
    #: properties of the metadata document itself.
    snapshots: int = 0
    metadata_log_entries: int = 0
    #: Signals a deeper tier would have to answer. See
    #: :data:`UNSEEN_FROM_METADATA`.
    unseen: tuple[str, ...] = field(default_factory=lambda: UNSEEN_FROM_METADATA)

    @property
    def average_file_bytes(self) -> float | None:
        """Mean live data-file size, the primary compaction signal.

        ``None`` rather than a division when either input is missing or the
        table has no data files -- a table with nothing in it is not a table
        with small files.
        """
        if not self.data_files or self.total_bytes is None:
            return None
        return self.total_bytes / self.data_files

    @property
    def delete_ratio(self) -> float | None:
        """Delete files per data file. Rising means merge-on-read is accumulating.

        Counts *files*, not rows: it is the file count that costs a reader an
        open, and row counts are reported separately by Iceberg for that reason.
        """
        if not self.data_files or self.delete_files is None:
            return None
        return self.delete_files / self.data_files


def _counter(summary: dict[str, str], key: str) -> int | None:
    """One summary counter as an int, or ``None`` if absent or unparseable.

    Summary values are strings by the spec, and a writer is free to omit any of
    them. Neither case should raise in a health check -- the caller wanted a
    reading, and "could not read it" is a reading.
    """
    raw = summary.get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def table_health(tbl: Table) -> TableHealth:
    """Read a table's layout signals from metadata already in hand.

    Takes a loaded ``Table`` rather than an identifier so the caller owns the
    load: a fleet sweep has one catalog round trip per table and this must not
    add a second.
    """
    snapshot = tbl.metadata.current_snapshot_id
    current = None
    for candidate in tbl.metadata.snapshots:
        if candidate.snapshot_id == snapshot:
            current = candidate
            break

    # `Snapshot.summary` is itself optional in the spec, so a snapshot can
    # exist and carry nothing. Treated as an empty summary rather than an error:
    # every field then reads `None`, which is exactly what "not reported" means.
    summary: dict[str, str] = {}
    if current is not None and current.summary is not None:
        summary = dict(current.summary.additional_properties)

    return TableHealth(
        # `Table.name()` returns an Identifier tuple; the rest of this package
        # joins it the same way (profile.py, orphans.py).
        identifier=".".join(tbl.name()),
        snapshot_id=snapshot,
        data_files=_counter(summary, TOTAL_DATA_FILES),
        total_bytes=_counter(summary, TOTAL_FILES_SIZE),
        delete_files=_counter(summary, TOTAL_DELETE_FILES),
        position_deletes=_counter(summary, TOTAL_POSITION_DELETES),
        equality_deletes=_counter(summary, TOTAL_EQUALITY_DELETES),
        records=_counter(summary, TOTAL_RECORDS),
        snapshots=len(tbl.metadata.snapshots),
        metadata_log_entries=len(tbl.metadata.metadata_log),
    )


@dataclass(frozen=True)
class Watermark:
    """When Zamboni last committed to this table, and what has happened since.

    **The table is the store.** Every snapshot Zamboni commits stamps
    :data:`OPERATION_STAMP` into its summary, so "when did maintenance last run"
    is answerable from metadata already in hand -- no sidecar database, nothing
    to lose on a redeploy, and a fleet that can be maintained by a different host
    than the one that maintained it yesterday.

    **A stamp can age out, and that fails the safe way.** `expire` deletes
    snapshots, including ones carrying this stamp, so a table compacted once and
    then left long enough loses its watermark and reads as never maintained. The
    consequence is that it is considered again -- more work, not less -- which is
    the right direction for a check whose job is deciding whether to work.
    """

    identifier: str
    #: The newest snapshot Zamboni stamped, and what it was doing.
    snapshot_id: int | None = None
    operation: str | None = None
    timestamp_ms: int | None = None
    #: Snapshots committed after that one. They are writers by construction:
    #: anything Zamboni committed later would itself be the newest stamp.
    snapshots_since: int = 0

    @property
    def maintained(self) -> bool:
        """Has Zamboni ever left a stamp on this table that still survives?"""
        return self.snapshot_id is not None

    @property
    def written_since(self) -> bool:
        """Has anyone written since Zamboni last committed?

        This is the due-check in one property. False means the table is exactly
        as maintenance left it, and every operation would find nothing to do.
        """
        return self.snapshots_since > 0


def maintenance_watermark(tbl: Table) -> Watermark:
    """Read the last-maintenance mark, and the writes after it, from metadata.

    Ordered by position in `metadata.snapshots` rather than by `timestamp_ms`,
    deliberately: that list is append order, while timestamps come from whichever
    client committed and a writer with a skewed clock could otherwise reorder
    history. The same concern is why `maintainers/spark.py` documents its
    `older_than` literal as the *client's* clock.
    """
    snapshots = list(tbl.metadata.snapshots)

    newest_stamped: tuple[int, int, int, str] | None = None
    for position, snapshot in enumerate(snapshots):
        summary = snapshot.summary
        if summary is None:
            continue
        operation = summary.additional_properties.get(OPERATION_STAMP)
        if operation is not None:
            newest_stamped = (position, snapshot.snapshot_id, snapshot.timestamp_ms, operation)

    identifier = ".".join(tbl.name())
    if newest_stamped is None:
        # Never maintained, or the stamp has been expired away. Either way every
        # snapshot present counts as unreviewed work.
        return Watermark(identifier=identifier, snapshots_since=len(snapshots))

    position, snapshot_id, timestamp_ms, operation = newest_stamped
    return Watermark(
        identifier=identifier,
        snapshot_id=snapshot_id,
        operation=operation,
        timestamp_ms=timestamp_ms,
        snapshots_since=len(snapshots) - position - 1,
    )
