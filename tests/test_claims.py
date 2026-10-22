# SPDX-License-Identifier: Apache-2.0
"""Claims (#167): `LocalClaimStore`, and the conformance suite every store must pass.

The suite is only as good as what it rejects, so each rule of §6 is also broken
on purpose -- one store per way of getting it wrong -- and the check for that
rule must fail on it. The store's clock is skewed a day from this process's, so
a store reading the wrong clock cannot pass by coincidence.
"""

from __future__ import annotations

import dataclasses
import re
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

from zamboni import conformance
from zamboni.claims import Claim, LocalClaimStore, _Row, new_holder
from zamboni.conformance import (
    CLAIM_STORE_CHECKS,
    ClaimStoreHarness,
    ConformanceError,
    claim_store_conformance,
)


class StoreClock:
    """A clock a day ahead of this process's, moved only by the test."""

    def __init__(self) -> None:
        self.now = datetime.now(UTC) + timedelta(days=1)
        self._lock = threading.Lock()

    def __call__(self) -> datetime:
        with self._lock:
            return self.now

    def pass_(self, moment: datetime) -> None:
        with self._lock:
            self.now = max(self.now, moment + timedelta(microseconds=1))


def harness(store_class: type[LocalClaimStore] = LocalClaimStore) -> ClaimStoreHarness:
    clocks: dict[int, StoreClock] = {}

    def make_store() -> LocalClaimStore:
        clock = StoreClock()
        store = store_class(clock=clock)
        clocks[id(store)] = clock
        return store

    return ClaimStoreHarness(
        make_store=make_store,
        now=lambda store: clocks[id(store)](),
        pass_time=lambda store, moment: clocks[id(store)].pass_(moment),
        tolerance=timedelta(0),
    )


@pytest.mark.parametrize("check", CLAIM_STORE_CHECKS, ids=lambda c: c.__name__)
def test_local_claim_store_passes(check):
    check(harness())


def test_the_whole_suite_runs_as_one_call():
    claim_store_conformance(harness())


def test_the_suite_runs_against_real_time_too():
    """The default `pass_time` sleeps on the store's clock: what a database store gets."""
    claim_store_conformance(
        ClaimStoreHarness(
            make_store=LocalClaimStore,
            now=lambda store: store.now(),  # type: ignore[attr-defined]
            lease=timedelta(seconds=0.2),
            tolerance=timedelta(seconds=0.1),
        )
    )


# -- every rule, broken -----------------------------------------------------------


class ReadThenWrite(LocalClaimStore):
    """Checks, then writes, outside one atomic step."""

    def acquire(self, warehouse, table, *, holder, lease, reason):
        with self._lock:
            row = self._rows.get((warehouse, table))
            free = row is None or row.expires_at <= self._clock()
        time.sleep(0.005)
        if not free:
            return None
        with self._lock:
            self._token += 1
            row = _Row(holder, self._token, reason, self._clock() + lease)
            self._rows[(warehouse, table)] = row
        return Claim(warehouse, table, holder, row.token, row.expires_at)


class CallersClock(LocalClaimStore):
    """Expires claims on this process's clock, not the store's."""

    def acquire(self, warehouse, table, *, holder, lease, reason):
        claim = super().acquire(warehouse, table, holder=holder, lease=lease, reason=reason)
        if claim is None:
            return None
        claim = dataclasses.replace(claim, expires_at=datetime.now(UTC) + lease)
        row = self._rows[(warehouse, table)]
        self._rows[(warehouse, table)] = dataclasses.replace(row, expires_at=claim.expires_at)
        return claim


class ReacquiresItsOwn(LocalClaimStore):
    def acquire(self, warehouse, table, *, holder, lease, reason):
        row = self._rows.get((warehouse, table))
        if row is not None and row.holder == holder:
            del self._rows[(warehouse, table)]
        return super().acquire(warehouse, table, holder=holder, lease=lease, reason=reason)


class NeverExpires(LocalClaimStore):
    def acquire(self, warehouse, table, *, holder, lease, reason):
        if (warehouse, table) in self._rows:
            return None
        return super().acquire(warehouse, table, holder=holder, lease=lease, reason=reason)


class RowCounter(LocalClaimStore):
    """A token counted per row, which starts again when the row is released."""

    def acquire(self, warehouse, table, *, holder, lease, reason):
        row = self._rows.get((warehouse, table))
        self._token = 0 if row is None else row.token
        return super().acquire(warehouse, table, holder=holder, lease=lease, reason=reason)


class TokenBlind(LocalClaimStore):
    """Matches the holder alone, so a stale token from the same holder still counts."""

    def _own(self, claim):
        row = self._rows.get((claim.warehouse, claim.table))
        return row if row is not None and row.holder == claim.holder else None


class HolderBlind(LocalClaimStore):
    def _own(self, claim):
        row = self._rows.get((claim.warehouse, claim.table))
        return row if row is not None and row.token == claim.token else None


class RenewsTheExpired(LocalClaimStore):
    def renew(self, claim, *, lease):
        with self._lock:
            row = self._own(claim)
            if row is None:
                return None
            self._rows[(claim.warehouse, claim.table)] = dataclasses.replace(
                row, expires_at=self._clock() + lease
            )
            return dataclasses.replace(claim, expires_at=self._clock() + lease)


class RenewsInName(LocalClaimStore):
    """Answers a renewal without extending anything."""

    def renew(self, claim, *, lease):
        return claim if self._own(claim) is not None else None


class ReleaseKeeps(LocalClaimStore):
    def release(self, claim):
        return None


class PerWarehouse(LocalClaimStore):
    def acquire(self, warehouse, table, *, holder, lease, reason):
        claim = super().acquire(warehouse, "*", holder=holder, lease=lease, reason=reason)
        return None if claim is None else dataclasses.replace(claim, table=table)

    def _own(self, claim):
        return super()._own(dataclasses.replace(claim, table="*"))

    def release(self, claim):
        super().release(dataclasses.replace(claim, table="*"))


C = conformance
#: Each broken store, the check that must catch it, and what it must say: a
#: failure on some earlier step ("the first acquire failed") is not the rule.
BROKEN = [
    (ReadThenWrite, C.check_concurrent_acquires_have_exactly_one_winner, "won one table"),
    (CallersClock, C.check_acquire_of_a_free_table_succeeds_on_the_stores_clock, "rule 2"),
    (ReacquiresItsOwn, C.check_acquire_of_a_held_table_fails, "its own table again"),
    (NeverExpires, C.check_an_expired_claim_is_taken_over_and_its_old_holder_fenced, "expiry"),
    (RowCounter, C.check_the_token_rises_across_a_release, "rule 4"),
    (TokenBlind, C.check_renew_extends_only_a_live_claim_of_ones_own, "another token renewed"),
    (HolderBlind, C.check_renew_extends_only_a_live_claim_of_ones_own, "another holder renewed"),
    (RenewsTheExpired, C.check_renew_extends_only_a_live_claim_of_ones_own, "expired claim was"),
    (RenewsInName, C.check_renew_extends_only_a_live_claim_of_ones_own, "was not kept"),
    (ReleaseKeeps, C.check_release_frees_the_table, "still held after release"),
    (PerWarehouse, C.check_claims_are_per_table, "rule 5"),
]


@pytest.mark.parametrize(
    ("store", "check", "says"), BROKEN, ids=[store.__name__ for store, _, _ in BROKEN]
)
def test_a_store_breaking_a_rule_fails_the_check_for_it(store, check, says):
    with pytest.raises(ConformanceError, match=says):
        check(harness(store))
    with pytest.raises(ConformanceError):
        claim_store_conformance(harness(store))


def test_every_check_rejects_some_broken_store():
    """A check nothing fails is a check that proves nothing."""
    assert {check for _, check, _ in BROKEN} == set(CLAIM_STORE_CHECKS)


# -- LocalClaimStore itself -------------------------------------------------------


def test_a_lease_must_be_positive():
    store = LocalClaimStore()
    with pytest.raises(ValueError, match="positive"):
        store.acquire("wh", "raw.a", holder="h", lease=timedelta(0), reason="schedule")


def test_a_holder_names_the_host_and_process_and_is_unique():
    one, two = new_holder(), new_holder()
    assert one != two
    assert re.fullmatch(r"[^:]+:\d+:[0-9a-f-]{36}", one)


# -- the PostgreSQL reference, without a server -----------------------------------


def test_the_reference_sql_is_read_whole_from_the_design_document():
    """tests/test_dev_stack.py runs these against Postgres; this is what it runs.

    Every statement §6.1 prints is found, and every placeholder bound -- a
    statement lost to a reworded comment would otherwise surface only in the
    dev-stack job, as a KeyError.
    """
    pytest.importorskip("psycopg")
    from .claims_postgres import reference_sql

    sql = reference_sql()
    assert set(sql) == {"schema", "acquire", "renew", "release"}
    assert "CREATE SEQUENCE zamboni_claim_token" in sql["schema"]
    assert "CREATE TABLE zamboni_claim" in sql["schema"]
    params = {name: set(re.findall(r"%\((\w+)\)s", text)) for name, text in sql.items()}
    assert params == {
        "schema": set(),
        "acquire": {"warehouse", "table", "holder", "reason", "lease"},
        "renew": {"warehouse", "table", "holder", "token", "lease"},
        "release": {"warehouse", "table", "holder", "token"},
    }
    assert not any(re.search(r"(?<!:):\w", text) for text in sql.values()), "a placeholder left"
