"""The reference Lakehouse App (#161), without a database: what holds it to the design.

examples/lakehouse-app/ is not part of the distribution, so nothing else in this
suite imports it. These tests do, from the source tree. Its database-backed
behaviour -- the conformance suites, the registry reaching the runtime -- is in
tests/test_lakehouse_app_live.py, against the dev stack's PostgreSQL.
"""

from __future__ import annotations

import asyncio
import re
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
# The app needs psycopg, which the dev group has and CI's consumer job, a bare
# install of the wheel, does not: there the app's tests skip rather than error.
pytest.importorskip("psycopg")

APP = ROOT / "examples" / "lakehouse-app"
sys.path.insert(0, str(APP))

from lakehouse_app import claims, request_table, state  # noqa: E402
from lakehouse_app.db import schema_sql  # noqa: E402
from lakehouse_app.maintenance import RegistryWatcher  # noqa: E402
from lakehouse_app.registry import RegistryError, WarehouseRow, check  # noqa: E402

from tests.claims_postgres import reference_sql as claim_sql  # noqa: E402
from tests.requests_postgres import REQUESTS, STATE  # noqa: E402
from tests.requests_postgres import reference_sql as section_sql  # noqa: E402


def words(sql: str) -> str:
    """Whitespace and ``--`` comments ignored: what a statement says, not its layout."""
    return " ".join(re.sub(r"--[^\n]*", "", sql).split())


# -- the app runs the document's SQL -------------------------------------------------


@pytest.mark.parametrize(
    ("ours", "theirs"),
    [
        (claims.ACQUIRE, lambda: claim_sql()["acquire"]),
        (claims.RENEW, lambda: claim_sql()["renew"]),
        (claims.RELEASE, lambda: claim_sql()["release"]),
        (request_table.TAKE, lambda: section_sql(REQUESTS)["take"]),
        (request_table.HANDLE, lambda: section_sql(REQUESTS)["handle"]),
        (state.STATE, lambda: section_sql(STATE)["state"]),
        (state.RUN, lambda: section_sql(STATE)["run"]),
    ],
    ids=["acquire", "renew", "release", "take", "handle", "state", "run"],
)
def test_each_statement_is_the_documents(ours, theirs):
    """docs/maintenance-runtime.md §6.1 and §9.2 print the statements the
    conformance suites pass against; the app runs those, not a paraphrase."""
    assert words(ours) == words(theirs())


@pytest.mark.parametrize(
    "schema",
    [
        lambda: claim_sql()["schema"],
        lambda: section_sql(REQUESTS)["schema"],
        lambda: section_sql(STATE)["schema"],
    ],
    ids=["claims", "requests", "state"],
)
def test_the_schema_creates_the_documents_tables(schema):
    """Each of the document's CREATEs, with IF NOT EXISTS so a tier can apply
    the file on every start, and otherwise as printed."""
    ours = words(schema_sql())
    # Comments stripped before splitting: one of them has a semicolon in it.
    for statement in filter(None, (s.strip() for s in words(schema()).split(";"))):
        idempotent = re.sub(r"^CREATE (TABLE|SEQUENCE) ", r"CREATE \1 IF NOT EXISTS ", statement)
        assert idempotent + ";" in ours, idempotent


# -- the tiers ---------------------------------------------------------------------------

SHARED = ("lakehouse_app.db", "lakehouse_app.registry", "lakehouse_app.request_table")


@pytest.mark.parametrize("module", SHARED)
def test_a_module_the_web_tier_shares_never_loads_the_runtime(module):
    """§9.5 point 3, for the modules both tiers import. A fresh interpreter,
    so nothing this test process already loaded can hide an import."""
    code = (
        f"import sys; sys.path.insert(0, {str(APP)!r}); import {module}; "
        "assert 'zamboni.runtime' not in sys.modules, sorted("
        "m for m in sys.modules if m.startswith('zamboni'))"
    )
    subprocess.run([sys.executable, "-c", code], check=True, cwd=ROOT)


def test_the_wheel_ships_no_example(tmp_path):
    """An example is not part of the distribution: built from this tree, the
    wheel contains `zamboni` and nothing under examples/."""
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path), "--quiet"],
        check=True,
        cwd=ROOT,
    )
    (wheel,) = tmp_path.glob("*.whl")
    names = zipfile.ZipFile(wheel).namelist()
    assert any(n.startswith("zamboni/") for n in names), "the wheel was built from this tree"
    offending = [n for n in names if "examples" in n.split("/") or "lakehouse_app" in n]
    assert not offending, offending


# -- the registry ------------------------------------------------------------------------

TABLE_CONFIG = {"namespaces": {"raw": {"tables": {"events": {}}}}}


def row(**changes) -> WarehouseRow:
    fields = {"name": "acme", "schedule": "0 2 * * *", "table_config": TABLE_CONFIG}
    return WarehouseRow(**{**fields, **changes})


@pytest.mark.parametrize(
    ("catalog", "why"),
    [
        ({"credential": "client:secret"}, "credential"),
        ({"token": "t0ken"}, "credential"),
        ({"uri": "https://user:pass@lakekeeper/catalog"}, "credential in it"),
    ],
    ids=["credential", "token", "uri-userinfo"],
)
def test_the_registry_refuses_a_credential(catalog, why):
    """Every dashboard user reads the registry: a credential belongs in the
    maintenance tier's environment instead."""
    with pytest.raises(RegistryError, match=why):
        check(row(catalog=catalog))


@pytest.mark.parametrize(
    ("changes", "why"),
    [
        ({"schedule": "*/0 * * * *"}, "acme"),
        ({"table_config": {"namespaces": {"raw": {"tables": {"events": {"bogus": 1}}}}}}, "bogus"),
    ],
    ids=["schedule", "table-config"],
)
def test_the_registry_refuses_what_zamboni_would(changes, why):
    """Validated by Zamboni's own fleet loader, where the person who wrote it
    is, rather than by every replica's next reload."""
    with pytest.raises(RegistryError, match=why):
        check(row(**changes))


# -- the registry reaches the runtime only through update_fleet -------------------------


class FakeRegistry:
    def __init__(self, rows):
        self.rows = rows

    def warehouses(self):
        return list(self.rows)


class FakeRuntime:
    def __init__(self):
        self.fleets = []

    def update_fleet(self, fleet):
        self.fleets.append(fleet)
        return len(self.fleets) + 1


def watch(registry, edits):
    """Run the watcher over ``edits`` -- each a list of rows, one per poll."""
    watcher = RegistryWatcher(registry, interval=0.01)
    watcher.first(registry.warehouses())
    runtime = FakeRuntime()

    async def scenario():
        stop = asyncio.Event()
        task = asyncio.create_task(watcher.run(runtime, stop))
        for rows in edits:
            registry.rows = rows
            await asyncio.sleep(0.05)
        stop.set()
        await task

    asyncio.run(scenario())
    return runtime.fleets


def test_an_edit_is_pushed_once_and_an_unchanged_registry_not_at_all():
    registry = FakeRegistry([row()])
    fleets = watch(registry, [[row()], [row(schedule="0 4 * * *")], [row(schedule="0 4 * * *")]])

    assert len(fleets) == 1
    assert fleets[0]["acme"].schedule.expression == "0 4 * * *"


def test_a_registry_emptied_or_broken_keeps_the_fleet_in_use(caplog):
    """Stopping all maintenance because the last row was deleted by mistake is
    what a reload must never do; said once, not on every poll."""
    registry = FakeRegistry([row()])
    fleets = watch(registry, [[], [], [row(schedule="*/0 * * * *")]])

    assert fleets == []
    refusals = [r for r in caplog.records if "not to a usable fleet" in r.getMessage()]
    assert len(refusals) == 2, "the empty registry, then the broken one: each said once"
