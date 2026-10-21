# SPDX-License-Identifier: Apache-2.0
"""The runtime's per-warehouse record (`zamboni.runstate.Ledger`, #166).

Moved from `zamboni serve`'s state, unchanged in what it records: the sweeps
and the last error. test_service.py checks that the state file still reads
as it did, byte for byte.
"""

from __future__ import annotations

from datetime import timedelta

from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse
from zamboni.runstate import Ledger

from .test_service import T0, deadline_drop, result, work


def test_a_sweep_closes_when_every_table_it_offered_has_a_result():
    ledger = Ledger()
    ledger.fired("acme", ["raw.a", "raw.b"], T0)
    ledger.finished(result("raw.a"), T0 + timedelta(minutes=1))
    assert ledger.warehouses["acme"].last_sweep is None

    ledger.finished(result("raw.b", code=3), T0 + timedelta(minutes=2))
    sweep = ledger.warehouses["acme"].last_sweep
    assert sweep is not None
    assert sweep.tables == 2
    assert sweep.failures == 1
    assert sweep.worst_exit_code == 3
    assert sweep.finished_at == T0 + timedelta(minutes=2)


def test_each_firing_is_its_own_sweep_and_one_run_answers_both():
    """Two firings while a table is still queued coalesce into one run, and that
    run answers both. A schedule faster than a sweep cannot hold one open."""
    ledger = Ledger()
    ledger.fired("acme", ["raw.a"], T0)
    ledger.fired("acme", ["raw.a"], T0 + timedelta(minutes=1))
    assert ledger.warehouses["acme"].open_sweeps == 2
    ledger.finished(result("raw.a"), T0 + timedelta(minutes=2))
    entry = ledger.warehouses["acme"]
    assert entry.open_sweeps == 0
    assert entry.last_sweep is not None
    assert entry.last_sweep.fired_at == T0 + timedelta(minutes=1)


def test_a_firing_that_asked_for_nothing_opens_no_sweep():
    """Every table was already in flight: the runtime passes an empty list."""
    ledger = Ledger()
    ledger.fired("acme", [], T0)
    assert ledger.warehouses["acme"].open_sweeps == 0
    assert ledger.warehouses["acme"].last_sweep is None


def test_a_reload_that_removes_a_pending_table_closes_the_sweep():
    """Otherwise a sweep waiting on a table the fleet no longer has never ends."""
    ledger = Ledger()
    ledger.fired("acme", ["raw.a", "raw.gone"], T0)
    ledger.finished(result("raw.a"), T0)
    fleet = FleetConfig(
        warehouses=(FleetWarehouse("acme", CronSchedule("0 2 * * *"), work("raw.a").table_config),)
    )
    ledger.adopted(fleet, T0 + timedelta(minutes=1))
    sweep = ledger.warehouses["acme"].last_sweep
    assert sweep is not None and sweep.tables == 2


def test_a_failure_is_the_last_error():
    ledger = Ledger()
    ledger.finished(result("raw.a", code=1, died="worker died (killed by signal 9)"), T0)
    assert ledger.last_error == "acme/raw.a: worker died (killed by signal 9)"


def test_a_deadline_drop_answers_its_sweep_and_is_counted_there():
    ledger = Ledger()
    ledger.fired("acme", ["raw.a", "raw.b"], T0)
    ledger.finished(result("raw.a"), T0 + timedelta(minutes=1))
    ledger.finished(deadline_drop("raw.b", T0), T0 + timedelta(minutes=2))
    sweep = ledger.warehouses["acme"].last_sweep
    assert sweep is not None
    assert (sweep.tables, sweep.failures, sweep.dropped) == (2, 0, 1)
    assert ledger.last_error is None, "a window closing is not a failure"


def test_the_record_handed_out_is_a_copy():
    """`state()` is read from other threads; the loop must not change it under them."""
    ledger = Ledger()
    ledger.fired("acme", ["raw.a"], T0)
    seen = ledger.warehouses
    ledger.finished(result("raw.a"), T0)
    ledger.fired("globex", ["raw.x"], T0)
    assert set(seen) == {"acme"} and seen["acme"].last_result is None
