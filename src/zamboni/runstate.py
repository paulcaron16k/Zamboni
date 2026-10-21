# SPDX-License-Identifier: Apache-2.0
"""What a :class:`~zamboni.runtime.MaintenanceRuntime` can say about itself.

docs/maintenance-runtime.md §8: ``state()`` returns everything ``zamboni
serve``'s state file carries, so an application's UI renders the present
without keeping its own copy of it. The bookkeeping that builds the
per-warehouse part -- last firing, last result, sweeps, the pressure check --
lives here, on the runtime's loop, rather than in each consumer; ``serve``'s
state file is one serialisation of it (`zamboni.service`).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .fleet import FleetConfig
from .pool import ItemResult


@dataclass(frozen=True)
class SweepSummary:
    """The last sweep of a warehouse to complete: every table one firing asked for."""

    fired_at: datetime
    finished_at: datetime
    tables: int
    #: Tables that exited non-zero.
    failures: int
    #: Tables never started because their deadline passed (§3.3).
    dropped: int
    worst_exit_code: int


@dataclass(frozen=True)
class PressureSummary:
    """The last delete-pressure check of a warehouse (#175)."""

    checked_at: datetime
    tables: int
    over: int
    offered: int
    errors: int


@dataclass(frozen=True)
class WarehouseState:
    """One warehouse, as far as the runtime has seen it. ``None``: not yet."""

    last_fired: datetime | None = None
    last_result: datetime | None = None
    last_exit_code: int | None = None
    last_sweep: SweepSummary | None = None
    #: Firings whose tables have not all reported. Set from the first firing.
    open_sweeps: int | None = None
    pressure: PressureSummary | None = None


@dataclass(frozen=True)
class RuntimeState:
    """A consistent snapshot of the runtime, taken on its loop."""

    #: ``created`` -> ``running`` -> ``stopping`` -> ``stopped``.
    phase: str
    generation: int
    workers: int
    last_tick: datetime | None
    #: ``(warehouse, table)``, in the order they will be taken.
    queued: tuple[tuple[str, str], ...]
    in_flight: tuple[tuple[str, str], ...]
    #: In flight, and offered again meanwhile: re-offered when done (§3.2).
    dirty: tuple[tuple[str, str], ...]
    #: The last table to fail, as ``warehouse/table: why``.
    last_error: str | None = None
    #: Why the last candidate fleet was refused; cleared when one is adopted.
    #: The fleet in use is still valid.
    reload_error: str | None = None
    warehouses: Mapping[str, WarehouseState] = field(default_factory=dict)
    #: Per event source: its feed's counters (`zamboni.events.feed.FeedCounters`).
    events: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    #: Whether every event source is connected; ``None`` when there are none,
    #: which is "nothing to wait for" rather than "disconnected".
    sources_connected: bool | None = None

    @property
    def busy(self) -> int:
        return len(self.in_flight)


@dataclass
class _Sweep:
    """The tables one firing asked for, until every one of them has a result.

    **Only the tables the firing actually asked for.** A table already being
    maintained when the schedule fires is not in it: the queue refused that
    offer, and the run in flight started before the firing, so no result will
    ever answer it. A table still *queued* is in it, and its one run answers
    every firing that coalesced into it. Each firing is its own sweep, so a
    schedule faster than a sweep cannot keep one open forever.
    """

    fired_at: datetime
    pending: set[str]
    tables: int
    worst_exit_code: int = 0
    failures: int = 0
    dropped: int = 0


class Ledger:
    """The per-warehouse record behind :class:`RuntimeState`. Loop-only.

    Moved from ``zamboni.service.ServiceState`` by #166, unchanged in what it
    records, so the state file reads as it did.
    """

    def __init__(self) -> None:
        self.last_error: str | None = None
        self.reload_error: str | None = None
        self._warehouses: dict[str, WarehouseState] = {}
        self._sweeps: dict[str, list[_Sweep]] = {}

    @property
    def warehouses(self) -> dict[str, WarehouseState]:
        return dict(self._warehouses)

    def _update(self, warehouse: str, **changes: Any) -> None:
        current = self._warehouses.get(warehouse, WarehouseState())
        self._warehouses[warehouse] = dataclasses.replace(current, **changes)

    def fired(self, warehouse: str, tables: list[str], at: datetime) -> None:
        """A firing that asked for ``tables`` -- excluding any already in flight."""
        if tables:
            self._sweeps.setdefault(warehouse, []).append(_Sweep(at, set(tables), len(tables)))
        self._update(warehouse, last_fired=at, open_sweeps=len(self._sweeps.get(warehouse, [])))

    def finished(self, result: ItemResult, at: datetime) -> None:
        warehouse = result.item.warehouse
        self._update(warehouse, last_result=at, last_exit_code=result.exit_code)
        if result.exit_code:
            self.last_error = f"{warehouse}/{result.item.table}: " + (
                result.died or f"exit {result.exit_code}"
            )
        for sweep in self._sweeps.get(warehouse, []):
            if result.item.table in sweep.pending:
                sweep.pending.discard(result.item.table)
                sweep.worst_exit_code = max(sweep.worst_exit_code, result.exit_code)
                sweep.failures += bool(result.exit_code)
                sweep.dropped += result.dropped is not None
        self._close_finished(warehouse, at)

    def adopted(self, fleet: FleetConfig, at: datetime) -> None:
        """A new fleet: forget what it no longer has, and the last refusal."""
        self.reload_error = None
        for name in list(self._sweeps):
            try:
                tables = set(fleet[name].table_config.tables)
            except KeyError:
                del self._sweeps[name]
                continue
            for sweep in self._sweeps[name]:
                sweep.pending &= tables
            self._close_finished(name, at)
        for name in list(self._warehouses):
            if name not in fleet.names:
                del self._warehouses[name]

    def refused(self, reason: str) -> bool:
        """A candidate fleet was refused; whether that is news."""
        changed = reason != self.reload_error
        self.reload_error = reason
        return changed

    def pressure_checked(self, warehouse: str, found: Mapping[str, Any], at: datetime) -> None:
        self._update(
            warehouse,
            pressure=PressureSummary(
                checked_at=at,
                tables=found["checked"],
                over=len(found["over"]),
                offered=found["offered"],
                errors=len(found["errors"]),
            ),
        )

    def _close_finished(self, warehouse: str, at: datetime) -> None:
        sweeps = self._sweeps.get(warehouse, [])
        done = [s for s in sweeps if not s.pending]
        if not done:
            return
        latest = max(done, key=lambda s: s.fired_at)
        self._sweeps[warehouse] = [s for s in sweeps if s.pending]
        self._update(
            warehouse,
            last_sweep=SweepSummary(
                fired_at=latest.fired_at,
                finished_at=at,
                tables=latest.tables,
                failures=latest.failures,
                dropped=latest.dropped,
                worst_exit_code=latest.worst_exit_code,
            ),
            open_sweeps=len(self._sweeps[warehouse]),
        )
