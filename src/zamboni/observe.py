# SPDX-License-Identifier: Apache-2.0
"""Observing a maintenance runtime (docs/maintenance-runtime.md §8).

An :class:`Observer` is told what a :class:`~zamboni.runtime.MaintenanceRuntime`
does, **on the runtime's loop thread**, and must return quickly. ``zamboni
serve``'s state file and run log are two observers (`zamboni.service`); an
application's run history, dashboard and audit trail are others.

**An observer that raises is logged and ignored**: a run that did its work never
fails over its reporting, and no observer can stop the runtime. **An observer
that is slow delays everything on the loop** -- the next firing, the next table
taken -- so anything slow (a database write, an HTTP call) is handed off.
:func:`handed_off` does that for any observer: each call is queued and made, in
order, on a thread of its own.

**Provisional (0.8.0)**, with the rest of the runtime. ``started`` gains the
table's claim with #167.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .fleet import FleetConfig
    from .pool import ItemResult
    from .runtime import OfferReceipt
    from .scheduler import WorkItem

logger = logging.getLogger(__name__)

#: How long a stopping runtime waits for each handed-off observer to deliver
#: what it has queued. **Chosen, not measured**: long enough for a backlog of
#: file or database writes, short enough that one wedged consumer cannot hold
#: a deployment's shutdown past its grace period.
CLOSE_TIMEOUT_SECONDS = 10.0

#: Every method an observer may define, in the order of :class:`Observer`.
EVENTS = (
    "phase_changed",
    "ticked",
    "fired",
    "offered",
    "started",
    "finished",
    "fleet_changed",
    "refused",
    "pressure_checked",
)


class Observer:
    """Every event, as a method that does nothing; override the ones you want.

    Subclassing is optional: the runtime calls whichever of these methods an
    object has, so any object defining some of them is an observer.
    """

    def phase_changed(self, phase: str) -> None:
        """``running`` once the pool exists, ``stopping`` at the first stop
        request, ``stopped`` once every table in flight has ended."""

    def ticked(self, at: datetime) -> None:
        """The scheduler stepped, firing or not: the liveness signal."""

    def fired(self, warehouse: str, at: datetime, tables: Sequence[str]) -> None:
        """``warehouse``'s schedule fired, asking for ``tables`` -- every table
        in its config except those already in flight."""

    def offered(self, receipt: OfferReceipt) -> None:
        """A call to :meth:`~zamboni.runtime.MaintenanceRuntime.offer`, and what it did."""

    def started(self, item: WorkItem) -> None:
        """A worker took ``item``. Not called for a table dropped at its deadline."""

    def finished(self, result: ItemResult) -> None:
        """A table ended: ran, failed, its worker died, or dropped at its deadline."""

    def fleet_changed(self, generation: int, fleet: FleetConfig) -> None:
        """A new fleet was adopted, from the provider or ``update_fleet``."""

    def refused(self, kind: str, detail: str) -> None:
        """Something was not accepted. ``kind`` ``"fleet"``: a candidate fleet,
        and why; the fleet in use is unchanged."""

    def pressure_checked(self, warehouse: str, found: Mapping[str, Any]) -> None:
        """A delete-pressure check of ``warehouse`` (#175): ``checked``, ``over``,
        ``errors``, ``offered`` and the check's ``exit_code``."""


class Observers:
    """The runtime's observers, each call isolated from the others' failures."""

    def __init__(self, observers: Sequence[Any]) -> None:
        self._observers = tuple(observers)

    def __bool__(self) -> bool:
        return bool(self._observers)

    def notify(self, event: str, *args: Any) -> None:
        for observer in self._observers:
            method = getattr(observer, event, None)
            if method is None:
                continue
            try:
                method(*args)
            except Exception:
                logger.exception("observer %r failed on %s; ignored", observer, event)

    def close(self) -> None:
        """Let each handed-off observer deliver what it has queued.

        Only :class:`HandedOff`: an observer's own ``close`` may mean anything
        (a file, a connection the application still needs), so it is the
        application's to call.
        """
        for observer in self._observers:
            if isinstance(observer, HandedOff) and not observer.close(CLOSE_TIMEOUT_SECONDS):
                logger.warning(
                    "%r did not deliver its queued calls within %.0f s; leaving it",
                    observer,
                    CLOSE_TIMEOUT_SECONDS,
                )


class HandedOff:
    """An observer whose calls are made on a thread of its own, in order.

    The runtime's loop only queues each call, so the observer may be as slow as
    it needs. The queue is unbounded: a consumer slower than the runtime for
    good falls behind in memory rather than stalling maintenance. A call that
    raises is logged and the next one is made. :meth:`close` -- which the
    runtime calls once it has stopped, waiting at most
    :data:`CLOSE_TIMEOUT_SECONDS` -- delivers what is queued, then ends the
    thread.
    """

    _CLOSE = object()

    def __init__(self, observer: Any, *, name: str | None = None) -> None:
        self.observer = observer
        self._calls: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._thread = threading.Thread(
            target=self._deliver, name=name or f"observer-{type(observer).__name__}", daemon=True
        )
        self._thread.start()
        for event in EVENTS:
            if callable(getattr(observer, event, None)):
                setattr(self, event, self._queuer(event))

    def _queuer(self, event: str) -> Callable[..., None]:
        def queue_call(*args: Any) -> None:
            self._calls.put((event, args))

        return queue_call

    def _deliver(self) -> None:
        while (call := self._calls.get()) is not self._CLOSE:
            event, args = call
            try:
                getattr(self.observer, event)(*args)
            except Exception:
                logger.exception("observer %r failed on %s; ignored", self.observer, event)

    def close(self, timeout: float | None = None) -> bool:
        """Deliver every queued call, then stop; whether that finished in ``timeout`` s."""
        self._calls.put(self._CLOSE)
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def __repr__(self) -> str:
        return f"handed_off({self.observer!r})"


def handed_off(observer: Any, *, name: str | None = None) -> HandedOff:
    """``observer``, called on its own thread so it never delays the runtime."""
    return HandedOff(observer, name=name)
