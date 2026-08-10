# SPDX-License-Identifier: Apache-2.0
"""Apply the table properties that govern metadata.json retention.

Unlike everything else in this package, metadata-log retention is not an
operation -- there is nothing to plan or rewrite. It is two table properties,
and the only work is knowing what they do and setting them deliberately.

Measured on a table taken through eight commits:

===================================================  ====  ==========
properties                                            log   on disk
===================================================  ====  ==========
defaults                                                8           9
``previous-versions-max=2``                             2           9
``previous-versions-max=2`` + ``delete-after-commit``   2           3
===================================================  ====  ==========

The middle row is the point. ``write.metadata.previous-versions-max`` trims the
*log*, not the files: every entry it drops becomes an unreferenced
``metadata.json`` that nothing will ever delete. Setting it alone does not
reclaim storage, it manufactures orphans -- which is exactly where the demo's
leaked metadata files came from.

Two ways to deal with them, and they are not equivalent:

``write.metadata.delete-after-commit.enabled``
    The writer deletes the fallen-off file during its own commit
    (``Table._do_commit`` -> ``Catalog._delete_old_metadata``). Immediate and
    free, but there is no age guard: a reader that resolved that metadata
    location a moment earlier gets a missing file. Safe when one process writes;
    a race otherwise.

Orphan removal
    Sweeps them later, behind the 3-day age guard, with the reachable-set
    invariants. Slower to reclaim, and safe under concurrency.

Default here is the second: leave ``delete-after-commit`` alone and let
:mod:`zamboni.orphans` handle it. The first is available by declaring it.

Under a REST catalog the server builds the new metadata, so the *trimming* is
Lakekeeper's to do -- driven by the same property on the same table. The
deletion half stays client-side either way.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyiceberg.table import Table

logger = logging.getLogger(__name__)

PROP_PREVIOUS_VERSIONS_MAX = "write.metadata.previous-versions-max"
PROP_DELETE_AFTER_COMMIT = "write.metadata.delete-after-commit.enabled"

#: PyIceberg's default, matching Iceberg's.
DEFAULT_PREVIOUS_VERSIONS_MAX = 100


@dataclass
class PropertyChange:
    key: str
    was: str | None
    now: str


@dataclass
class ApplyResult:
    identifier: str
    changes: list[PropertyChange] = field(default_factory=list)
    dry_run: bool = False

    def describe(self) -> str:
        if not self.changes:
            return f"{self.identifier}: metadata properties already as declared"
        verb = "would set" if self.dry_run else "set"
        lines = [f"{self.identifier}: {verb} {len(self.changes)} propert(ies)"]
        lines += [f"  {c.key}: {c.was or '(unset)'} -> {c.now}" for c in self.changes]
        return "\n".join(lines)


def desired_properties(settings) -> dict[str, str]:
    """The properties a :class:`MetadataSettings` block asks for.

    ``None`` means "leave whatever is there", which is not the same as "set the
    default" -- a table already tuned by hand must not be reset just because
    this file exists.
    """
    wanted: dict[str, str] = {}
    if settings.previous_versions_max is not None:
        wanted[PROP_PREVIOUS_VERSIONS_MAX] = str(settings.previous_versions_max)
    if settings.delete_after_commit is not None:
        wanted[PROP_DELETE_AFTER_COMMIT] = "true" if settings.delete_after_commit else "false"
    return wanted


def apply_metadata_properties(tbl: Table, settings, *, dry_run: bool = False) -> ApplyResult:
    """Set the declared metadata properties, and only those that differ."""
    identifier = ".".join(tbl.name())
    result = ApplyResult(identifier=identifier, dry_run=dry_run)

    current = dict(tbl.properties)
    for key, value in desired_properties(settings).items():
        if current.get(key) != value:
            result.changes.append(PropertyChange(key=key, was=current.get(key), now=value))

    if not result.changes or dry_run:
        return result

    with tbl.transaction() as txn:
        txn.set_properties(properties={c.key: c.now for c in result.changes})
    tbl.refresh()
    return result


def unreferenced_metadata_files(tbl: Table) -> int:
    """How many ``metadata.json`` files the log no longer points at.

    Reported rather than acted on: deleting them is orphan removal's job, with
    its age guard. This is the number that explains why that guard has anything
    to sweep.

    Listing goes through :func:`zamboni.orphans.list_storage` rather than a
    second copy of it. An earlier version reached for ``PyArrowFileIO`` directly
    and so raised ``AttributeError`` on any deployment using ``FsspecFileIO`` --
    which is every Lakekeeper warehouse with remote signing, i.e. exactly where
    this number gets printed. It also missed ``write.metadata.path``.
    """
    from .orphans import list_storage, storage_roots
    from .reachable import canonical

    referenced = {canonical(tbl.metadata_location)}
    referenced |= {canonical(entry.metadata_file) for entry in tbl.metadata.metadata_log}

    return sum(
        1
        for key in list_storage(tbl, storage_roots(tbl))
        if key.endswith(".metadata.json") and key not in referenced
    )
