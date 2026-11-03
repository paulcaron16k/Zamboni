# SPDX-License-Identifier: Apache-2.0
"""A table that fails every time stops taking a worker every time
(docs/maintenance-runtime.md §7.1).

After ``after`` failures in a row a table is **quarantined**: every offer of it
is refused until the backoff has passed. Each later failure doubles the
backoff, up to ``most``; a run that ends soundly clears the record. A
``manual`` offer is never refused -- an operator asking is the override.

**What counts.** A failure is a run that ended outside
:data:`~zamboni.pool.SOUND_EXITS`: an uncaught exception, a dead, timed-out
or stalled worker -- the same line that retires a worker. Exits 2, 3 and 4 are
the contract's own answers, so they clear rather than count. A table dropped
at its deadline, or because another replica holds its claim, did not run and
changes nothing. The cost being saved is the hung table's: without this, a
table that stalls every night takes a worker for an hour every night.

Loop-only, like :class:`~zamboni.runstate.Ledger`: the runtime calls it from its
loop and passes the time in, so the backoff is testable without a clock.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from .pool import SOUND_EXITS
from .scheduler import MANUAL, Candidate

if TYPE_CHECKING:
    from .fleet import FleetConfig
    from .pool import ItemResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QuarantineState:
    """A quarantined table, as ``state()`` and observers see it."""

    #: Failures in a row, the one that quarantined it included.
    failures: int
    #: Offers other than ``manual`` are refused until then.
    until: datetime
    #: The backoff ``until`` was set with; the next failure doubles it.
    backoff: timedelta
    #: Why the last run failed, as ``RuntimeState.last_error`` puts it.
    last_failure: str


class Quarantine:
    """Consecutive failures per table, and which tables are backing off."""

    def __init__(self, *, after: int | None, backoff: timedelta, most: timedelta) -> None:
        """``after`` ``None`` never quarantines anything."""
        if after is not None and after < 1:
            raise ValueError(f"quarantine_after must be at least 1, or None; got {after}")
        if backoff <= timedelta(0) or most < backoff:
            raise ValueError(
                f"quarantine backoff must be positive and at most its ceiling; "
                f"got {backoff} up to {most}"
            )
        self.after, self.backoff, self.most = after, backoff, most
        self._failures: dict[str, int] = {}
        self._quarantined: dict[str, QuarantineState] = {}

    @property
    def quarantined(self) -> dict[str, QuarantineState]:
        """Every quarantined table, by ``warehouse/table``. One whose backoff has
        passed stays here until its next run: that run either clears it or
        quarantines it again for twice as long."""
        return dict(self._quarantined)

    def admits(self, candidate: Candidate, at: datetime) -> bool:
        if MANUAL in candidate.reasons:
            return True
        held = self._quarantined.get(_key(candidate.warehouse, candidate.table))
        return held is None or at >= held.until

    def finished(self, result: ItemResult, at: datetime) -> tuple[str, QuarantineState | None]:
        """Record ``result``. Returns what changed: ``("quarantined", state)``
        when the table entered or stayed in quarantine, ``("released", None)``
        when a sound run cleared one, ``("", None)`` otherwise."""
        key = _key(result.item.warehouse, result.item.table)
        if result.dropped is not None:
            return "", None
        if result.exit_code in SOUND_EXITS:
            self._failures.pop(key, None)
            if self._quarantined.pop(key, None) is not None:
                logger.info("%s: released from quarantine by a sound run", key)
                return "released", None
            return "", None
        failures = self._failures.get(key, 0) + 1
        self._failures[key] = failures
        if self.after is None or failures < self.after:
            return "", None
        backoff = min(self.backoff * 2 ** (failures - self.after), self.most)
        state = QuarantineState(
            failures=failures,
            until=at + backoff,
            backoff=backoff,
            last_failure=result.died or f"exit {result.exit_code}",
        )
        self._quarantined[key] = state
        logger.warning(
            "%s: quarantined after %d failures in a row; offers refused until %s",
            key,
            failures,
            state.until.isoformat(),
        )
        return "quarantined", state

    def adopted(self, fleet: FleetConfig) -> None:
        """A new fleet: forget the tables it no longer has."""
        known = {
            _key(warehouse.name, table)
            for warehouse in fleet.warehouses
            for table in warehouse.table_config.tables
        }
        for records in (self._failures, self._quarantined):
            for key in set(records) - known:
                del records[key]


def _key(warehouse: str, table: str) -> str:
    return f"{warehouse}/{table}"
