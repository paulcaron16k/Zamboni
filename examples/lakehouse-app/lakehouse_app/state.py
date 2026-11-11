# SPDX-License-Identifier: Apache-2.0
"""What the maintenance tier tells the dashboard, as rows (docs/maintenance-runtime.md §9.2).

An observer (§8) of the runtime that writes:

- ``zamboni_runtime``: one row per runtime, rewritten on every change it
  reports. A row whose ``last_tick`` is old is a runtime that stopped ticking,
  which the dashboard shows as "maintenance unavailable since ...";
- ``zamboni_run``: one row per finished table, with its run record;
- ``lakehouse_quarantine``: the app's own, one row per quarantined table.

Every call is a database write, so it is handed to the runtime through
`zamboni.observe.handed_off` (:func:`lakehouse_app.maintenance.build`), which
moves it off the runtime's loop. ``STATE`` and ``RUN`` are the document's
statements, held to it by tests/test_lakehouse_app.py.

Imported only by the maintenance tier.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .db import Database

if TYPE_CHECKING:
    from zamboni.pool import ItemResult
    from zamboni.quarantine import QuarantineState
    from zamboni.runstate import RuntimeState
    from zamboni.runtime import MaintenanceRuntime

STATE = """\
INSERT INTO zamboni_runtime AS r
       (holder, phase, last_tick, busy, queued, last_error, updated_at)
VALUES (%(holder)s, %(phase)s, %(last_tick)s, %(busy)s, %(queued)s, %(last_error)s, now())
ON CONFLICT (holder) DO UPDATE
   SET phase = EXCLUDED.phase, last_tick = EXCLUDED.last_tick, busy = EXCLUDED.busy,
       queued = EXCLUDED.queued, last_error = EXCLUDED.last_error, updated_at = now();"""

RUN = """\
INSERT INTO zamboni_run
       (holder, warehouse, table_identifier, reason, exit_code, died, dropped, record)
VALUES (%(holder)s, %(warehouse)s, %(table)s, %(reason)s, %(exit_code)s, %(died)s,
        %(dropped)s, %(record)s::jsonb);"""

QUARANTINED = """\
INSERT INTO lakehouse_quarantine
       (warehouse, table_identifier, holder, failures, until, last_failure, updated_at)
VALUES (%(warehouse)s, %(table)s, %(holder)s, %(failures)s, %(until)s, %(last_failure)s, now())
ON CONFLICT (warehouse, table_identifier) DO UPDATE
   SET holder = EXCLUDED.holder, failures = EXCLUDED.failures, until = EXCLUDED.until,
       last_failure = EXCLUDED.last_failure, updated_at = now();"""

RELEASED = """\
DELETE FROM lakehouse_quarantine
 WHERE warehouse = %(warehouse)s AND table_identifier = %(table)s;"""


class StateRows:
    """The observer. :meth:`attach` it to its runtime before the runtime runs."""

    def __init__(self, database: Database) -> None:
        self._db = database
        self._holder = ""
        self._state: Callable[[], RuntimeState] | None = None

    def attach(self, runtime: MaintenanceRuntime) -> None:
        """Whose rows these are. The runtime takes its observers when it is
        built, and has a ``holder`` only once it is, so this comes after."""
        self._holder, self._state = runtime.holder, runtime.state

    def _changed(self, *_: Any) -> None:
        assert self._state is not None, "attach() the runtime first"
        state = self._state()
        with self._db.connect() as conn:
            conn.execute(
                STATE,
                {
                    "holder": self._holder,
                    "phase": state.phase,
                    "last_tick": state.last_tick,
                    "busy": state.busy,
                    "queued": len(state.queued),
                    "last_error": state.last_error,
                },
            )

    # Every change a dashboard would show. `ticked` is what keeps `last_tick`
    # fresh, and so what tells a stopped tier from a quiet one.
    phase_changed = ticked = fired = offered = started = fleet_changed = refused = _changed

    def finished(self, result: ItemResult) -> None:
        with self._db.connect() as conn:
            conn.execute(
                RUN,
                {
                    "holder": self._holder,
                    "warehouse": result.item.warehouse,
                    "table": result.item.table,
                    "reason": result.item.reason,
                    "exit_code": result.exit_code,
                    "died": result.died,
                    "dropped": result.dropped,
                    "record": None if result.record is None else json.dumps(result.record),
                },
            )
        self._changed()

    def quarantined(self, warehouse: str, table: str, state: QuarantineState) -> None:
        with self._db.connect() as conn:
            conn.execute(
                QUARANTINED,
                {
                    "warehouse": warehouse,
                    "table": table,
                    "holder": self._holder,
                    "failures": state.failures,
                    "until": state.until,
                    "last_failure": state.last_failure,
                },
            )

    def released(self, warehouse: str, table: str) -> None:
        with self._db.connect() as conn:
            conn.execute(RELEASED, {"warehouse": warehouse, "table": table})
