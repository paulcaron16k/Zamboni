# SPDX-License-Identifier: Apache-2.0
"""Delete pressure: when a merge-on-read table should be compacted between schedules.

#175. A merge-on-read table answers every read by opening its data files *and*
the delete files that mask them, so each write that deletes or updates makes
every later reader slower until compaction folds the deletes back in. On a
schedule that can be a day of readers paying for it. ``zamboni serve`` measures
each table every ``check_every_minutes`` and offers one over a threshold with
``reason=pressure``, which runs ``compact`` and ``remove-dangling-deletes``.

**The signal is the snapshot summary**, read by
:func:`zamboni.health.table_health`: one metadata load, no manifest opened. The
numbers here are applied to that record as data -- a mapping with
:class:`~zamboni.health.TableHealth`'s field names -- so this module imports
nothing from the engine, and the service process that evaluates it stays light
(#154). The measuring happens in a worker.

**Equality deletes make a table ineligible**, not merely uncounted. Compaction
refuses a table carrying them (ZMBNI-11), so a trigger that fired on one would
be refused and fire again every check. And the summary's ``total-delete-files``
counts equality and position delete *files* together, so "count only position
delete files" cannot be read from it.

**The defaults are chosen starting values, not measurements** (Paul,
2026-10-08): on, at 10 delete files, a delete-file ratio of 0.10, or a
deleted-row ratio of 0.15, checked every 15 minutes. They are DevOps' to change
in the fleet file.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, replace
from typing import Any

DEFAULT_DELETE_FILES = 10
DEFAULT_DELETE_RATIO = 0.10
DEFAULT_DELETED_ROWS_RATIO = 0.15
DEFAULT_CHECK_EVERY_MINUTES = 15

#: The health-record fields a decision reads, by their TableHealth names. A test
#: checks they are TableHealth's, without this module importing it.
HEALTH_FIELDS = ("data_files", "delete_files", "position_deletes", "equality_deletes", "records")


@dataclass(frozen=True)
class PressureSettings:
    """Thresholds -- any one met triggers -- and how often to look.

    A threshold set to ``None`` is switched off; at least one must stay on, or
    the setting means "never", which is ``pressure: false``.
    """

    #: Live delete files, of any kind.
    delete_files: int | None = DEFAULT_DELETE_FILES
    #: Delete files per data file. Files, because each is an open for a reader.
    delete_ratio: float | None = DEFAULT_DELETE_RATIO
    #: Position-deleted rows per row in the data files.
    deleted_rows_ratio: float | None = DEFAULT_DELETED_ROWS_RATIO
    check_every_minutes: int = DEFAULT_CHECK_EVERY_MINUTES

    def __post_init__(self) -> None:
        if self.delete_files is not None and not _positive_int(self.delete_files):
            raise ValueError(
                f"pressure.delete_files: expected a whole number above 0, or null to "
                f"switch it off; found {self.delete_files!r}"
            )
        for name in ("delete_ratio", "deleted_rows_ratio"):
            value = getattr(self, name)
            if value is not None and not _ratio(value):
                raise ValueError(
                    f"pressure.{name}: expected a ratio above 0 and at most 1, or null "
                    f"to switch it off; found {value!r}"
                )
        if not _positive_int(self.check_every_minutes):
            raise ValueError(
                f"pressure.check_every_minutes: expected a whole number above 0; "
                f"found {self.check_every_minutes!r}"
            )
        if (
            self.delete_files is None
            and self.delete_ratio is None
            and (self.deleted_rows_ratio is None)
        ):
            raise ValueError(
                "pressure: every threshold is off, which can never trigger; "
                "write `pressure: false` to switch it off"
            )

    @classmethod
    def keys(cls) -> frozenset[str]:
        return frozenset(f.name for f in fields(cls))

    @classmethod
    def from_value(cls, value: Any, base: PressureSettings | None) -> PressureSettings | None:
        """A fleet file's ``pressure:`` value, over ``base``.

        ``false`` switches it off and ``true`` takes ``base`` -- or the defaults,
        if ``base`` was off. A block sets the keys it names and keeps the rest
        of ``base``, so a warehouse overrides one threshold without restating
        the others. ``ValueError`` names the problem; the caller adds where.
        """
        if value is False:
            return None
        start = base if base is not None else cls()
        if value is True:
            return start
        if not isinstance(value, dict):
            raise ValueError(
                f"pressure: expected true, false or a block of settings, found {value!r}"
            )
        if unknown := sorted(set(value) - cls.keys()):
            raise ValueError(f"pressure: unknown key(s) {unknown}; allowed: {sorted(cls.keys())}")
        return replace(start, **value)

    def triggered(self, health: Mapping[str, Any]) -> str | None:
        """Why this table is over a threshold, or ``None``.

        ``health`` is a health record: TableHealth's fields, as data. A counter
        the summary did not report is ``None`` and meets no threshold -- "not
        reported" is not "zero", and neither is it "over".
        """
        delete_files = health.get("delete_files")
        if not delete_files:  # copy-on-write, a table without deletes, or unreported
            return None
        equality = health.get("equality_deletes")
        if equality is None or equality > 0:
            # Compaction refuses equality deletes; and unreported, they might be
            # there. Either way an offer would only be refused.
            return None
        if self.delete_files is not None and delete_files >= self.delete_files:
            return f"{delete_files} delete files (threshold {self.delete_files})"
        data_files = health.get("data_files")
        if self.delete_ratio is not None and data_files:
            ratio = delete_files / data_files
            if ratio >= self.delete_ratio:
                return (
                    f"{delete_files} delete files to {data_files} data files, "
                    f"{ratio:.2f} (threshold {self.delete_ratio})"
                )
        records, deleted = health.get("records"), health.get("position_deletes")
        if self.deleted_rows_ratio is not None and records and deleted is not None:
            ratio = deleted / records
            if ratio >= self.deleted_rows_ratio:
                return (
                    f"{deleted} of {records} rows position-deleted, {ratio:.2f} "
                    f"(threshold {self.deleted_rows_ratio})"
                )
        return None


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _ratio(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and 0 < value <= 1
