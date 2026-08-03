"""Delete files under a table's location that nothing references.

This is the most dangerous operation in the package: it decides what to delete
by subtracting a computed set from a directory listing, so a mistake anywhere in
that computation destroys live data. Everything below exists to make that
mistake either impossible or loud.

**Ordering is the primary safety property.** Storage is listed *before* the
reachable set is computed. A file committed by a concurrent writer between the
two steps is then in both sets and survives. Reversing the order puts it in the
listing but not in reachable, and it gets deleted despite being committed --
so the order here is load-bearing, not incidental.

The age guard is the backstop for writes that were already in flight when we
started. Iceberg's own guidance sizes it to "the time expected for any write to
complete"; the longest write in this system is a large compaction, not the
ingest. It does **not** protect long-running readers -- file mtime is the wrong
clock for that, and snapshot retention is the right mechanism.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field

from pyarrow.fs import FileSelector
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table

from .reachable import Category, ReachableSet, canonical, reachable_files

logger = logging.getLogger(__name__)

DEFAULT_OLDER_THAN_DAYS = 3


class OrphanCleanupAborted(RuntimeError):
    """A safety invariant failed. Nothing was deleted."""


@dataclass(frozen=True)
class StorageFile:
    #: The comparison key: the form a listing returns, which is what
    #: :func:`icemaint.reachable.canonical` maps metadata locations onto.
    path: str
    #: The form to hand to ``FileIO.delete``. On object storage these differ --
    #: a listing yields ``bucket/key`` while the FileIO needs ``s3://bucket/key``
    #: and would otherwise treat the key as a local path and fail every delete.
    #: Expiry keeps the same distinction; see ``files_of_snapshots``.
    location: str
    size: int
    modified: dt.datetime


@dataclass
class OrphanResult:
    identifier: str
    scanned: int = 0
    referenced: int = 0
    orphans: int = 0
    orphan_bytes: int = 0
    deleted: int = 0
    deleted_bytes: int = 0
    failed: int = 0
    #: Unreferenced but younger than the guard, so left alone.
    too_young: int = 0
    too_young_bytes: int = 0
    dry_run: bool = False
    roots: list[str] = field(default_factory=list)
    older_than_days: int = DEFAULT_OLDER_THAN_DAYS

    def describe(self) -> str:
        verb = "would delete" if self.dry_run else "deleted"
        lines = [
            (
                f"{self.identifier}: scanned {self.scanned} file(s), "
                f"{self.referenced} referenced, {self.orphans} unreferenced"
            ),
            f"  {verb} {self.deleted} file(s) ({_human(self.deleted_bytes)})",
        ]
        if self.too_young:
            lines.append(
                f"  {self.too_young} file(s) ({_human(self.too_young_bytes)}) left in place: "
                f"younger than the {self.older_than_days}-day age guard"
            )
        if self.failed:
            lines.append(f"  {self.failed} file(s) could not be deleted")
        return "\n".join(lines)


def _human(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            return f"{value:.0f}{unit}" if unit == "B" else f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{value:.1f}GiB"  # pragma: no cover


def storage_roots(tbl: Table) -> list[str]:
    """Locations this table may own files in.

    The table location, plus the data and metadata paths when a table redirects
    them elsewhere. Never a warehouse-wide sweep: files belonging to a sibling
    table must be out of reach by construction.
    """
    roots = {tbl.location().rstrip("/")}
    for prop in ("write.data.path", "write.metadata.path"):
        if path := tbl.properties.get(prop):
            roots.add(path.rstrip("/"))
    # Drop any root nested inside another so files are not visited twice.
    ordered = sorted(roots, key=len)
    return [r for i, r in enumerate(ordered) if not any(r.startswith(o + "/") for o in ordered[:i])]


def list_storage(tbl: Table, roots: list[str]) -> dict[str, StorageFile]:
    """Every file under ``roots``, keyed canonically.

    ``FileIO`` has no list operation -- only delete/new_input/new_output -- so
    this reaches past it to the underlying filesystem. Which one that is depends
    on the deployment and is not ours to choose: a Lakekeeper warehouse with
    remote signing sets ``py-io-impl`` to ``FsspecFileIO`` in its per-table
    config, and those properties win over anything the client asks for. An
    earlier version of this function only knew ``PyArrowFileIO`` and so failed
    with ``AttributeError`` against exactly the deployment it was written for.
    """
    found: dict[str, StorageFile] = {}
    for root in roots:
        scheme, netloc, path = PyArrowFileIO.parse_location(root)
        for entry in _list_one(tbl, scheme, netloc, path):
            # Keyed by the listed path as-is. It is *already* the canonical form
            # -- that is what `canonical` is defined to produce -- and passing it
            # through again would corrupt it: a listing returns `bucket/key` with
            # no scheme, and `canonical` resolves a scheme-less path as a local
            # one, prefixing the working directory. Every live file then looks
            # unreferenced. See test_canonical_is_not_idempotent_for_object_keys.
            found[entry.path] = entry
    return found


def _list_one(tbl: Table, scheme: str, netloc: str, path: str) -> list[StorageFile]:
    io = tbl.io
    if hasattr(io, "_initialize_fs"):  # PyArrowFileIO
        fs = io._initialize_fs(scheme, netloc)
        return [
            StorageFile(
                info.path, _deletable(scheme, info.path), info.size or 0, _aware(info.mtime)
            )
            for info in fs.get_file_info(FileSelector(path, recursive=True, allow_not_found=True))
            if info.is_file
        ]

    if hasattr(io, "get_fs"):  # FsspecFileIO
        fs = io.get_fs(scheme)
        # `path` from parse_location already carries the bucket for object
        # stores -- ("s3", "warehouse", "warehouse/acme/tbl") -- so prefixing
        # `netloc` again asks for `warehouse/warehouse/acme/tbl` and lists
        # nothing. `find` returns files only, recursively, and an absent prefix
        # comes back empty rather than raising, matching allow_not_found above.
        listing = fs.find(path, detail=True)
        return [
            StorageFile(name, _deletable(scheme, name), info.get("size") or 0, _fsspec_mtime(info))
            for name, info in listing.items()
            if info.get("type", "file") == "file"
        ]

    raise OrphanCleanupAborted(
        f"cannot list storage: {type(io).__name__} exposes neither a pyarrow nor an "
        "fsspec filesystem, so the unreferenced set cannot be computed."
    )


def _deletable(scheme: str, listed_path: str) -> str:
    """The listed path in the form ``FileIO.delete`` accepts.

    A local listing already gives an absolute path. An object-store listing
    gives ``bucket/key`` with the scheme stripped, and a ``FileIO`` handed that
    resolves it as a local path -- so every delete fails and orphan removal
    reclaims nothing while reporting success.
    """
    if scheme in ("", "file", "local"):
        return listed_path
    return f"{scheme}://{listed_path}"


def _aware(when: dt.datetime) -> dt.datetime:
    return when.replace(tzinfo=dt.UTC) if when.tzinfo is None else when


def _fsspec_mtime(info: dict) -> dt.datetime:
    """Modification time from an fsspec entry, whatever the backend called it.

    s3fs reports ``LastModified``; local and most others report ``mtime``, as a
    float or a datetime. A missing time is treated as the epoch -- the age guard
    then keeps the file rather than deleting it, which is the safe direction.
    """
    for key in ("LastModified", "last_modified", "mtime"):
        value = info.get(key)
        if isinstance(value, dt.datetime):
            return _aware(value)
        if isinstance(value, int | float):
            return dt.datetime.fromtimestamp(value, dt.UTC)
    return dt.datetime.now(dt.UTC)


class OrphanCleaner:
    """Finds and (optionally) deletes unreferenced files under a table."""

    def __init__(
        self,
        *,
        older_than_days: int = DEFAULT_OLDER_THAN_DAYS,
        dry_run: bool = False,
    ) -> None:
        if older_than_days < 0:
            raise ValueError(f"older_than_days must be >= 0, got {older_than_days}")
        self._older_than_days = older_than_days
        self._dry_run = dry_run

    def run(self, tbl: Table, *, now: dt.datetime | None = None) -> OrphanResult:
        now = now or dt.datetime.now(dt.UTC)
        cutoff = now - dt.timedelta(days=self._older_than_days)
        identifier = ".".join(tbl.name())
        roots = storage_roots(tbl)

        # ORDER MATTERS. List first, then compute reachable -- see the module
        # docstring. Do not reorder these two lines.
        storage = list_storage(tbl, roots)
        tbl.refresh()
        reachable = reachable_files(tbl)

        self._check_invariants(tbl, identifier, storage, reachable)

        result = OrphanResult(
            identifier=identifier,
            scanned=len(storage),
            referenced=len(reachable),
            dry_run=self._dry_run,
            roots=roots,
            older_than_days=self._older_than_days,
        )

        current_metadata = canonical(tbl.metadata_location)
        candidates: list[StorageFile] = []
        for key, entry in storage.items():
            if key in reachable.paths:
                continue
            if key == current_metadata:
                # Belt and braces: the live pointer is never a candidate, no
                # matter what the diff says.
                continue
            result.orphans += 1
            result.orphan_bytes += entry.size
            if entry.modified > cutoff:
                result.too_young += 1
                result.too_young_bytes += entry.size
                continue
            candidates.append(entry)

        if self._dry_run:
            result.deleted = len(candidates)
            result.deleted_bytes = sum(c.size for c in candidates)
            return result

        for entry in candidates:
            try:
                tbl.io.delete(entry.location)
                result.deleted += 1
                result.deleted_bytes += entry.size
            except Exception as exc:
                result.failed += 1
                logger.warning("could not delete orphan %s: %s", entry.location, exc)
        return result

    def _check_invariants(
        self,
        tbl: Table,
        identifier: str,
        storage: dict[str, StorageFile],
        reachable: ReachableSet,
    ) -> None:
        """Abort rather than delete when our view of the world looks wrong."""
        missing = reachable.paths - set(storage)
        if missing:
            # The one condition under which the diff cannot be trusted: we can
            # see files the table references are absent, so the listing is
            # partial (wrong root, permissions, eventual consistency) and the
            # complement of it is meaningless.
            example = sorted(missing)[0]
            raise OrphanCleanupAborted(
                f"{identifier}: {len(missing)} referenced file(s) are missing from the "
                f"listing, so the unreferenced set cannot be trusted. First: {example}"
            )

        # A table with snapshots necessarily has data files, manifests and
        # manifest lists. Any of these coming back empty means the reachability
        # computation broke, not that the table is empty -- and treating live
        # metadata as unreferenced is precisely how this operation would
        # destroy a table.
        if tbl.metadata.snapshots:
            for category in (Category.DATA, Category.MANIFEST, Category.MANIFEST_LIST):
                if not reachable.by_category[category]:
                    raise OrphanCleanupAborted(
                        f"{identifier}: the table has snapshots but no reachable "
                        f"{category.value}. That is a bug in the reachability "
                        "computation, not an empty table."
                    )

        if not reachable.by_category[Category.METADATA]:
            raise OrphanCleanupAborted(
                f"{identifier}: no reachable table metadata; refusing to treat every "
                "metadata file as an orphan."
            )
