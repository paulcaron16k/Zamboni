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

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .units import human_bytes

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pyiceberg.table import Table
    from pyiceberg.table.snapshots import Snapshot

#: The summary key every snapshot-producing operation stamps. Three of the six
#: operations produce a snapshot at all -- compaction, `rewrite-manifests` and
#: `remove-dangling-deletes` -- and all three stamp it. `expire` removes
#: snapshots rather than adding one, and `remove-orphans` and `apply-properties`
#: produce none, so there is nothing for them to mark and the watermark is
#: silent about them by construction.
OPERATION_STAMP = "zamboni.operation"

#: What each committing operation writes as the value of :data:`OPERATION_STAMP`.
#:
#: One place, because there were three and one of them disagreed: the committer
#: wrote ``"compaction"`` where the other two wrote their `Operation` enum value
#: exactly (ZMBNI-135). A consumer grouping snapshots by this key got three
#: values, one of which was not in the enum that named the other two -- and
#: `zamboni health` printed "last maintenance compaction" beside "last
#: maintenance rewrite-manifests".
#:
#: Declared here rather than derived by importing `maintainers.Operation`:
#: `maintainers/__init__` loads the built-in engines at import time, which
#: reaches `committer.py`, so importing the enum from there would be a cycle
#: resolved only by definition order. `test_every_stamp_is_its_operations_enum_value`
#: checks the agreement instead, which is the same guarantee without the
#: fragility.
COMPACT_STAMP = "compact"
REWRITE_MANIFESTS_STAMP = "rewrite-manifests"
REMOVE_DANGLING_DELETES_STAMP = "remove-dangling-deletes"

#: What compaction stamped before ZMBNI-135. Snapshots carrying it are still out
#: there in every table maintained by an earlier Zamboni, and nothing reads the
#: stamp by comparing it to a literal -- :func:`maintenance_watermark` reports
#: whatever it finds -- so they keep working and simply say the old word. Named
#: so that a reader grouping a long history knows the two are the same thing.
LEGACY_COMPACT_STAMP = "compaction"

#: Written beside :data:`OPERATION_STAMP`, on any stamp, when compaction did
#: not finish in the run that wrote it: partitions held by
#: `skip_partitions_newer_than_windows`, groups still to commit when committing
#: group by group, or a compaction that was refused or failed. The value says
#: which, for a reader; only its presence is acted on.
#:
#: Without it a stamp read as "compaction had nothing left", and the write-driven
#: skip believed it -- but a hold lifts with the clock, not with a write, so on a
#: table whose writes had stopped the held partitions were skipped forever
#: (ZMBNI-196). Every stamp the run writes carries it, not only compaction's:
#: the newest stamp is normally `rewrite-manifests'`, which runs later, and on a
#: quiet table `expire` soon removes compaction's own. The newest stamp is the one
#: that survives, so it is the one that has to say so.
#:
#: Absent means finished, which is also how every stamp written before it reads --
#: deliberately. Reading an old stamp as unfinished would make compaction run on
#: every quiet table on every run, forever: a run with nothing to do commits
#: nothing, so nothing would ever replace that stamp.
COMPACT_INCOMPLETE = "zamboni.compact.incomplete"

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

    def describe(self) -> str:
        """The signals, one per line, with what could not be seen named at the end."""
        rows: list[tuple[str, str]] = [
            ("data files", _count(self.data_files)),
            ("total size", human_bytes(self.total_bytes) if self.total_bytes else "-"),
            (
                "average file size",
                human_bytes(self.average_file_bytes) if self.average_file_bytes else "-",
            ),
            ("delete files", _count(self.delete_files)),
            (
                "delete ratio",
                f"{self.delete_ratio:.2f}" if self.delete_ratio is not None else "-",
            ),
            ("snapshots retained", str(self.snapshots)),
            ("metadata log entries", str(self.metadata_log_entries)),
        ]
        lines = [f"  {name:<24} {value}" for name, value in rows]
        lines.append(f"  {'not visible from here':<24} {len(self.unseen)} signal(s):")
        lines.extend(f"      {signal}" for signal in self.unseen)
        return "\n".join(lines)

    def as_dict(self) -> dict[str, object]:
        """Counters an integrator can trend. See :class:`~zamboni.maintainers.Reportable`.

        The derived ratios are included because they are what a threshold is
        written against -- an integrator recomputing them from the counters would
        have to rediscover that a table with no data files has no average.
        """
        return {
            "table": self.identifier,
            "snapshot_id": self.snapshot_id,
            "data_files": self.data_files,
            "total_bytes": self.total_bytes,
            "delete_files": self.delete_files,
            "position_deletes": self.position_deletes,
            "equality_deletes": self.equality_deletes,
            "records": self.records,
            "snapshots_retained": self.snapshots,
            "metadata_log_entries": self.metadata_log_entries,
            "average_file_bytes": self.average_file_bytes,
            "delete_ratio": self.delete_ratio,
            "unseen_signals": len(self.unseen),
        }

    @property
    def delete_ratio(self) -> float | None:
        """Delete files per data file. Rising means merge-on-read is accumulating.

        Counts *files*, not rows: it is the file count that costs a reader an
        open, and row counts are reported separately by Iceberg for that reason.
        """
        if not self.data_files or self.delete_files is None:
            return None
        return self.delete_files / self.data_files


def _count(value: int | None) -> str:
    """A counter for display. `-` means not reported, which is not zero."""
    return "-" if value is None else str(value)


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
    #: What compaction left undone in the run that wrote that stamp, from
    #: :data:`COMPACT_INCOMPLETE`, or ``None`` if it finished.
    incomplete: str | None = None

    @property
    def maintained(self) -> bool:
        """Has Zamboni ever left a stamp on this table that still survives?"""
        return self.snapshot_id is not None

    def describe(self) -> str:
        if not self.maintained:
            return f"  {'last maintenance':<24} never, or its snapshot has been expired away"
        when = ""
        if self.timestamp_ms is not None:
            at = dt.datetime.fromtimestamp(self.timestamp_ms / 1000, dt.UTC)
            when = f", {at:%Y-%m-%d %H:%M} UTC"
        lines = (
            f"  {'last maintenance':<24} {self.operation}{when}\n"
            f"  {'written since':<24} {self.snapshots_since} snapshot(s)"
        )
        if self.incomplete:
            lines += f"\n  {'left for later':<24} {self.incomplete}"
        return lines

    def as_dict(self) -> dict[str, object]:
        """Counters an integrator can trend. See :class:`~zamboni.maintainers.Reportable`."""
        return {
            "table": self.identifier,
            "last_maintenance_snapshot_id": self.snapshot_id,
            "last_maintenance_operation": self.operation,
            "last_maintenance_timestamp_ms": self.timestamp_ms,
            "snapshots_since": self.snapshots_since,
            "maintained": self.maintained,
            "written_since": self.written_since,
            "incomplete": self.incomplete,
        }

    @property
    def written_since(self) -> bool:
        """Has anyone written since Zamboni last committed?

        This is the due-check in one property. False means the table is exactly
        as maintenance left it, and every operation would find nothing to do.
        """
        return self.snapshots_since > 0


def commit_order(snapshots: Iterable[Snapshot]) -> list[Snapshot]:
    """The snapshots oldest commit first, whatever order the catalog sent them in.

    `metadata.snapshots` is not in commit order. The spec promises no order for
    it, and Lakekeeper (v0.13.1) returns a different one on every load: measured
    on the dev stack, a table maintained two appends ago had its watermark read
    as anything from 0 to 7 writes back over 50 loads (ZMBNI-192). The SQL
    catalog happens to keep append order, which is how reading list position as
    commit order went unnoticed.

    The sequence number is commit order. From format v2 every commit takes the
    table's `last-sequence-number` + 1 (PyIceberg:
    `TableMetadata.next_sequence_number`), so it is unique and increasing across
    every branch. Not `timestamp_ms`: that comes from whichever client
    committed, and a writer with a skewed clock would reorder history -- the
    same concern is why `maintainers/spark.py` documents its `older_than`
    literal as the *client's* clock.

    On v1 every sequence number is 0, and the sort, being stable, leaves the
    catalog's order as it was: no worse than before, and no better on a catalog
    that reorders. Compaction refuses v1 (`profile.py`) but `rewrite-manifests`
    and `remove-dangling-deletes` do not check, so a v1 table can carry a stamp.
    """
    return sorted(snapshots, key=lambda snapshot: snapshot.sequence_number or 0)


def maintenance_watermark(tbl: Table) -> Watermark:
    """Read the last-maintenance mark, and the writes after it, from metadata.

    "After" is commit order, which :func:`commit_order` takes from the sequence
    numbers -- never from position in `metadata.snapshots`, which has none.
    """
    snapshots = commit_order(tbl.metadata.snapshots)

    newest: tuple[int, Snapshot, str] | None = None
    for position, snapshot in enumerate(snapshots):
        if (operation := _stamp(snapshot)) is not None:
            newest = (position, snapshot, operation)

    identifier = ".".join(tbl.name())
    if newest is None:
        # Never maintained, or the stamp has been expired away. Either way every
        # snapshot present counts as unreviewed work.
        return Watermark(identifier=identifier, snapshots_since=len(snapshots))

    position, snapshot, operation = newest
    summary = snapshot.summary
    return Watermark(
        identifier=identifier,
        snapshot_id=snapshot.snapshot_id,
        operation=operation,
        timestamp_ms=snapshot.timestamp_ms,
        snapshots_since=len(snapshots) - position - 1,
        incomplete=summary.additional_properties.get(COMPACT_INCOMPLETE) if summary else None,
    )


def _stamp(snapshot: Snapshot) -> str | None:
    summary = snapshot.summary
    return None if summary is None else summary.additional_properties.get(OPERATION_STAMP)
