# SPDX-License-Identifier: Apache-2.0
"""Conformance suites for the runtime's protocols (docs/maintenance-runtime.md §9.5).

An implementation of a protocol the runtime depends on for safety -- a
:class:`~zamboni.claims.ClaimStore`, and a worker entry (§7) -- is conformant
when it passes the suite here. Each check is a plain function that raises
:class:`ConformanceError`, so a suite runs under any test runner, or none:
``claim_store_conformance(...)`` runs them all, and ``CLAIM_STORE_CHECKS`` lets a
pytest suite report each check by name (``entry_conformance`` and
``ENTRY_CHECKS`` likewise)::

    @pytest.mark.parametrize("check", CLAIM_STORE_CHECKS, ids=lambda c: c.__name__)
    def test_my_store(check):
        check(ClaimStoreHarness(make_store=fresh_store, now=database_now))

Not :mod:`zamboni.testing`: that module imports private PyIceberg classes to
simulate merge-on-read tables, and an implementer of a claim store or an entry
needs none of them. The entry checks import the worker pool only when they run,
so a claim-store suite does not load it.
"""

from __future__ import annotations

import dataclasses
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .claims import Claim, ClaimStore

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from .pool import ItemResult
    from .scheduler import WorkItem


class ConformanceError(AssertionError):
    """An implementation broke a rule of its protocol. The message names the rule."""


def _require(condition: Any, rule: str, detail: str) -> None:
    if not condition:
        raise ConformanceError(f"{rule}: {detail}")


def _sleep_past(store: ClaimStore, harness: ClaimStoreHarness, moment: datetime) -> None:
    limit = time.monotonic() + max(harness.lease.total_seconds() * 10, 30.0)
    while harness.now(store) <= moment:
        _require(time.monotonic() < limit, "expiry", f"the store's clock never passed {moment}")
        time.sleep(0.05)


@dataclass(frozen=True)
class ClaimStoreHarness:
    """What the suite needs to know about the store under test."""

    #: A store with no claims in it. Called once per check; the store is shared
    #: by every thread a check starts, as the runtime's worker threads share it.
    make_store: Callable[[], ClaimStore]
    #: The store's own clock (for PostgreSQL, ``SELECT now()``): the suite
    #: checks expiry against it, never against this process's clock (§6 rule 2).
    now: Callable[[ClaimStore], datetime]
    #: Waits until the store's clock is past a moment. The default sleeps;
    #: a store with an injected clock advances it instead.
    pass_time: Callable[[ClaimStore, datetime], None] | None = None
    #: The lease the suite asks for. Long enough that no claim expires partway
    #: through a check on a slow store, which would fail checks that are right.
    lease: timedelta = timedelta(seconds=2)
    #: How far a claim's ``expires_at`` may sit from ``now + lease``: a
    #: round trip, and a database's ``now()`` being the transaction's start.
    tolerance: timedelta = timedelta(seconds=1)

    def wait_past(self, store: ClaimStore, moment: datetime) -> None:
        if self.pass_time is not None:
            self.pass_time(store, moment)
        else:
            _sleep_past(store, self, moment)


def _table() -> str:
    # Fresh per check, so a store shared between runs (a database) cannot
    # carry one check's claims into the next.
    return f"conformance.t_{uuid.uuid4().hex[:12]}"


def _holders(n: int) -> list[str]:
    run = uuid.uuid4().hex[:8]
    return [f"conformance-{run}-{i}" for i in range(n)]


def _acquire(h: ClaimStoreHarness, store: ClaimStore, table: str, holder: str) -> Claim | None:
    return store.acquire("wh", table, holder=holder, lease=h.lease, reason="schedule")


# -- the checks -------------------------------------------------------------------


def check_acquire_of_a_free_table_succeeds_on_the_stores_clock(h: ClaimStoreHarness) -> None:
    """Rules 1 and 2: the claim names its table and holder, and expires a lease
    after the *store's* now."""
    rule = "acquire a free table"
    store, table, (a,) = h.make_store(), _table(), _holders(1)
    before = h.now(store)
    claim = _acquire(h, store, table, a)
    after = h.now(store)
    _require(claim is not None, rule, "returned None for a table nobody holds")
    assert claim is not None
    _require(
        (claim.warehouse, claim.table, claim.holder) == ("wh", table, a),
        rule,
        f"the claim names the wrong table or holder: {claim}",
    )
    _require(isinstance(claim.token, int), rule, f"the token is not an int: {claim.token!r}")
    _require(
        before + h.lease - h.tolerance <= claim.expires_at <= after + h.lease + h.tolerance,
        "rule 2, the store's clock",
        f"expires_at {claim.expires_at} is not a lease after the store's now "
        f"({before} .. {after}): the caller's clock was used, or the lease was ignored",
    )


def check_acquire_of_a_held_table_fails(h: ClaimStoreHarness) -> None:
    """Rule 1: while a claim is live, nobody else -- nor its own holder -- acquires the table."""
    store, table, (a, b) = h.make_store(), _table(), _holders(2)
    _require(_acquire(h, store, table, a) is not None, "acquire", "the first acquire failed")
    _require(
        _acquire(h, store, table, b) is None, "rule 1", "a second holder acquired a held table"
    )
    _require(
        _acquire(h, store, table, a) is None,
        "rule 1",
        "the holder acquired its own table again, which would mint a second token for one run",
    )


def _race(h: ClaimStoreHarness, store: ClaimStore, table: str, holders: list[str]) -> list[Claim]:
    """Every holder acquires ``table`` at once; the claims that won."""
    gate = threading.Barrier(len(holders))
    won: list[Claim | None] = []
    errors: list[BaseException] = []

    def contend(holder: str) -> None:
        try:
            gate.wait(timeout=30)
            won.append(_acquire(h, store, table, holder))
        except BaseException as exc:  # reported below, not lost in a thread
            errors.append(exc)

    threads = [threading.Thread(target=contend, args=(x,)) for x in holders]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    _require(not errors, "concurrent acquire", f"an acquire raised: {errors[:1]!r}")
    return [c for c in won if c is not None]


def check_concurrent_acquires_have_exactly_one_winner(h: ClaimStoreHarness) -> None:
    """Rule 1: never read-then-write. Eight threads released together, over
    several tables, so a race that loses only sometimes is given the chance."""
    store = h.make_store()
    for _ in range(5):
        holders = _holders(8)
        winners = _race(h, store, _table(), holders)
        _require(
            len(winners) == 1,
            "rule 1, one atomic compare-and-set",
            f"{len(winners)} of {len(holders)} concurrent acquires won one table",
        )


def check_an_expired_claim_is_taken_over_and_its_old_holder_fenced(h: ClaimStoreHarness) -> None:
    """Rules 1, 3 and 4: after expiry the table is free; the new claim has a
    higher token, and the old holder can neither renew nor release it."""
    store, table, (a, b) = h.make_store(), _table(), _holders(2)
    old = _acquire(h, store, table, a)
    _require(old is not None, "acquire", "the first acquire failed")
    assert old is not None
    h.wait_past(store, old.expires_at)
    new = _acquire(h, store, table, b)
    _require(new is not None, "rule 1, expiry", "an expired claim was not taken over")
    assert new is not None
    _require(
        new.token > old.token,
        "rule 4, the fencing token",
        f"the takeover's token {new.token} is not above the expired one's {old.token}",
    )
    _require(
        store.renew(old, lease=h.lease) is None,
        "rule 3",
        "the old holder renewed a claim that is now someone else's",
    )
    store.release(old)
    _require(
        _acquire(h, store, table, a) is None,
        "rule 3",
        "the old holder's release cleared the new holder's claim",
    )
    _require(
        store.renew(new, lease=h.lease) is not None,
        "rule 3",
        "the new holder could not renew after the old one's release",
    )


def check_the_token_rises_across_a_release(h: ClaimStoreHarness) -> None:
    """Rule 4: a per-row counter would restart after the row is deleted."""
    store, table, (a, b) = h.make_store(), _table(), _holders(2)
    tokens = []
    for holder in (a, b, a):
        claim = _acquire(h, store, table, holder)
        _require(claim is not None, "acquire", "an acquire of a released table failed")
        assert claim is not None
        tokens.append(claim.token)
        store.release(claim)
    _require(
        tokens == sorted(set(tokens)),
        "rule 4, the fencing token",
        f"tokens across acquire-release cycles were {tokens}; each must exceed the last",
    )


def check_renew_extends_only_a_live_claim_of_ones_own(h: ClaimStoreHarness) -> None:
    """Rule 3: renewal is honoured past the original expiry, refused to anyone
    else -- another holder, or the holder with a wrong token -- and refused once
    the claim has expired."""
    store, table, (a, b) = h.make_store(), _table(), _holders(2)
    claim = _acquire(h, store, table, a)
    _require(claim is not None, "acquire", "the first acquire failed")
    assert claim is not None
    for impostor, who in (
        (dataclasses.replace(claim, holder=b), "another holder"),
        (dataclasses.replace(claim, token=claim.token + 1), "the holder with another token"),
    ):
        _require(store.renew(impostor, lease=h.lease) is None, "rule 3", f"{who} renewed the claim")
        store.release(impostor)
        _require(_acquire(h, store, table, b) is None, "rule 3", f"{who} released the claim")

    renewed = store.renew(claim, lease=h.lease * 3)
    _require(renewed is not None, "rule 3", "the holder could not renew its live claim")
    assert renewed is not None
    _require(
        (renewed.holder, renewed.token) == (claim.holder, claim.token),
        "rule 3",
        f"renewal changed the claim's holder or token: {claim} -> {renewed}",
    )
    h.wait_past(store, claim.expires_at)
    _require(
        _acquire(h, store, table, b) is None,
        "rule 3, renew",
        "the table was free at its original expiry: the renewal was not kept",
    )
    h.wait_past(store, renewed.expires_at)
    _require(
        store.renew(renewed, lease=h.lease) is None,
        "rule 3, renew",
        "an expired claim was renewed: a holder that lost its lease got it back",
    )


def check_release_frees_the_table(h: ClaimStoreHarness) -> None:
    store, table, (a, b) = h.make_store(), _table(), _holders(2)
    claim = _acquire(h, store, table, a)
    _require(claim is not None, "acquire", "the first acquire failed")
    assert claim is not None
    store.release(claim)
    store.release(claim)  # releasing twice is harmless: the runtime releases however a table ends
    _require(
        _acquire(h, store, table, b) is not None,
        "release",
        "the table was still held after release",
    )


def check_claims_are_per_table(h: ClaimStoreHarness) -> None:
    """Rule 5: an event for one table runs while a sweep holds the rest of its warehouse."""
    store, (a, b) = h.make_store(), _holders(2)
    one, other = _table(), _table()
    _require(_acquire(h, store, one, a) is not None, "acquire", "the first acquire failed")
    _require(
        _acquire(h, store, other, b) is not None,
        "rule 5",
        "a claim on one table blocked another table of the same warehouse",
    )
    elsewhere = store.acquire("wh2", one, holder=b, lease=h.lease, reason="schedule")
    _require(
        elsewhere is not None,
        "rule 5",
        "a claim blocked the table of the same name in another warehouse",
    )


#: Every check, in the order of §6's rules.
CLAIM_STORE_CHECKS: tuple[Callable[[ClaimStoreHarness], None], ...] = (
    check_acquire_of_a_free_table_succeeds_on_the_stores_clock,
    check_acquire_of_a_held_table_fails,
    check_concurrent_acquires_have_exactly_one_winner,
    check_an_expired_claim_is_taken_over_and_its_old_holder_fenced,
    check_the_token_rises_across_a_release,
    check_renew_extends_only_a_live_claim_of_ones_own,
    check_release_frees_the_table,
    check_claims_are_per_table,
)


def claim_store_conformance(harness: ClaimStoreHarness) -> None:
    """Run every check; raises :class:`ConformanceError` naming the first rule broken."""
    for check in CLAIM_STORE_CHECKS:
        check(harness)


# -- worker entries (§7, §9.5 point 2) ----------------------------------------------


@dataclass(frozen=True)
class EntryHarness:
    """What the entry checks need: the entry, and one table it can maintain.

    The checks run the entry the way the runtime does -- in a spawned worker
    of a real :class:`~zamboni.pool.WorkerPool`, given ``item`` by pickle --
    so they prove what a unit test of the function could not.
    """

    #: The entry under test, ``module:function``, as ``WorkerConfig.entry`` takes it.
    entry: str
    #: A table the entry can reach, with work to do: a preview of a table with
    #: nothing to do proves nothing about previewing. Its ``commit`` is ignored;
    #: each check sets it.
    item: WorkItem
    #: Something that changes whenever the table commits -- its current
    #: metadata location, or its snapshot id.
    table_state: Callable[[], object]
    #: The environment the worker runs with: whatever the entry needs to build
    #: its session, credentials included.
    base_environ: Mapping[str, str]
    #: The credential values in ``base_environ``, or anywhere else the entry
    #: reads them from. None may appear in the item, the record or the output.
    secrets: Sequence[str]
    #: Where the worker may write scratch files.
    workdir: Path
    #: The operator profile and env file, for an entry that resolves its
    #: settings as the CLI does.
    profile_path: str | None = None
    env_path: str | None = None


def _run_entry(h: EntryHarness, *, commit: bool) -> ItemResult:
    import asyncio

    from .pool import PoolSize, WorkerConfig, WorkerPool

    config = WorkerConfig(
        entry=h.entry,
        commit=commit,
        base_environ=dict(h.base_environ),
        workdir=h.workdir,
        profile_path=h.profile_path,
        env_path=h.env_path,
    )

    async def run() -> ItemResult:
        pool = WorkerPool(PoolSize(1, 1, "conformance"), config)
        try:
            return await pool.run(h.item)
        finally:
            await pool.close()

    return asyncio.run(run())


def _import_entry(entry: str) -> None:
    import importlib

    module, _, name = entry.partition(":")
    if not callable(getattr(importlib.import_module(module), name)):
        raise TypeError(f"{entry} is not callable")


def check_entry_is_importable_when_spawned(h: EntryHarness) -> None:
    from multiprocessing import get_context

    process = get_context("spawn").Process(target=_import_entry, args=(h.entry,))
    process.start()
    process.join(timeout=120)
    if process.is_alive():
        process.kill()
        process.join()
    _require(
        process.exitcode == 0,
        "rule 1",
        f"{h.entry} cannot be imported by name in a spawned interpreter (exit {process.exitcode})",
    )


def check_entry_previews_without_commit(h: EntryHarness) -> None:
    before = h.table_state()
    result = _run_entry(h, commit=False)
    _require(result.died is None, "rule 1", f"the worker died: {result.died}")
    after = h.table_state()
    _require(
        after == before,
        "rule 4",
        f"the table changed without commit ({before!r} -> {after!r}); output:\n{result.output}",
    )


def check_entry_returns_a_run_record(h: EntryHarness) -> None:
    from .maintenance import MaintenanceReport

    result = _run_entry(h, commit=False)
    record = result.record
    _require(
        isinstance(record, dict),
        "rule 3",
        f"no run record came back (exit {result.exit_code}); output:\n{result.output}",
    )
    assert isinstance(record, dict)
    # The shape `maintenance --json` writes, read off the report that writes it.
    missing = sorted(set(MaintenanceReport().as_dict()) - set(record))
    _require(not missing, "rule 3", f"the run record lacks {missing}")
    _require(
        h.item.table in record["tables"],
        "rule 3",
        f"the run record names {record['tables']}, not {h.item.table}",
    )


def check_entry_carries_no_secret(h: EntryHarness) -> None:
    import json
    import pickle

    _require(h.secrets, "rule 2", "the harness names no secret, so nothing would be checked")
    sent = pickle.dumps(h.item)
    result = _run_entry(h, commit=False)
    places = {
        "the work item": repr(h.item) + sent.decode("latin-1"),
        "the run record": json.dumps(result.record, default=str),
        "the output": result.output,
    }
    for secret in h.secrets:
        for where, text in places.items():
            _require(secret not in text, "rule 2", f"a secret appears in {where}")


#: Every entry check, in the order of §9.5 point 2.
ENTRY_CHECKS: tuple[Callable[[EntryHarness], None], ...] = (
    check_entry_is_importable_when_spawned,
    check_entry_previews_without_commit,
    check_entry_returns_a_run_record,
    check_entry_carries_no_secret,
)


def entry_conformance(harness: EntryHarness) -> None:
    """Run every entry check; raises :class:`ConformanceError` naming the rule broken."""
    for check in ENTRY_CHECKS:
        check(harness)
