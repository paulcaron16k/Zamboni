# SPDX-License-Identifier: Apache-2.0
"""The worker-entry checks (docs/maintenance-runtime.md §7, §9.5 point 2).

The default entry must pass every check against a real table, and each check is
shown to reject an entry broken in its own way -- a check that passes
everything proves nothing. The broken entries live here, at module level, so a
spawned worker can import them by name.
"""

from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
from typing import Any

import pytest

from zamboni.conformance import (
    ENTRY_CHECKS,
    ConformanceError,
    EntryHarness,
    check_entry_carries_no_secret,
    check_entry_is_importable_when_spawned,
    check_entry_previews_without_commit,
    check_entry_returns_a_run_record,
    entry_conformance,
)
from zamboni.pool import WorkerContext, run_cli, run_table
from zamboni.scheduler import WorkItem
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

SECRET = "client:s3cr3t-conformance"


# -- entries broken one way each ------------------------------------------------------


def commits_anyway(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Breaks rule 4: ignores ``item.commit``."""
    return run_cli(dataclasses.replace(item, commit=True), context)


def bare_exit_code(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Breaks rule 3: an exit code, but not the record ``maintenance --json`` writes."""
    run_table(item, context)
    return {"exit_code": 0}


def prints_the_credential(item: WorkItem, context: WorkerContext) -> dict[str, Any]:
    """Breaks rule 2: a debug line that echoes what it authenticated with."""
    print(f"connecting with {os.environ['ZAMBONI_CREDENTIAL']}")
    return run_table(item, context)


# -- a table with work to do ---------------------------------------------------------


@pytest.fixture
def harness(tmp_path, monkeypatch, unpartitioned, session) -> EntryHarness:
    """The default entry, over six small files a compaction would rewrite."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    # A clean directory, so a developer's own zamboni.yml or .env is not read.
    monkeypatch.chdir(workdir)
    item = WorkItem(
        warehouse="acme",
        table="db.unpartitioned",
        reason="schedule",
        table_config=TableConfig(
            warehouse="acme",
            namespaces={"db": NamespaceSettings(tables={"unpartitioned": TableSettings()})},
        ),
    )
    return EntryHarness(
        entry="zamboni.pool:run_table",
        item=item,
        table_state=lambda: session.catalog.load_table("db.unpartitioned").metadata_location,
        base_environ={
            "PATH": os.environ.get("PATH", ""),
            "ZAMBONI_LOCAL_WAREHOUSE": str(tmp_path / "warehouse"),
            "ZAMBONI_CREDENTIAL": SECRET,
        },
        secrets=(SECRET,),
        workdir=workdir,
    )


@pytest.mark.parametrize("check", ENTRY_CHECKS, ids=lambda c: c.__name__)
def test_the_default_entry_passes(harness, check):
    check(harness)


def test_the_table_has_work_a_commit_would_do(harness):
    """The control for the preview check: committing does change the table, so
    the default entry passing it means it previewed, not that nothing was due."""
    from zamboni.conformance import _run_entry

    before = harness.table_state()
    result = _run_entry(harness, commit=True)
    assert result.exit_code == 0, result.output
    assert harness.table_state() != before


def test_the_suite_runs_every_check(harness, monkeypatch):
    ran = []
    monkeypatch.setattr(
        "zamboni.conformance.ENTRY_CHECKS",
        tuple((lambda h, c=c: ran.append(c.__name__)) for c in ENTRY_CHECKS),
    )
    entry_conformance(harness)
    assert ran == [c.__name__ for c in ENTRY_CHECKS]


# -- each check rejects its own breakage ----------------------------------------------


def test_an_entry_a_spawned_interpreter_cannot_import_is_refused(harness):
    broken = dataclasses.replace(harness, entry="tests.no_such_module:run_table")
    with pytest.raises(ConformanceError, match="rule 1"):
        check_entry_is_importable_when_spawned(broken)


def test_an_entry_that_commits_without_consent_is_refused(harness):
    broken = dataclasses.replace(harness, entry="tests.test_entry_conformance:commits_anyway")
    with pytest.raises(ConformanceError, match="rule 4"):
        check_entry_previews_without_commit(broken)


def test_an_entry_returning_a_bare_exit_code_is_refused(harness):
    broken = dataclasses.replace(harness, entry="tests.test_entry_conformance:bare_exit_code")
    with pytest.raises(ConformanceError, match="rule 3: the run record lacks"):
        check_entry_returns_a_run_record(broken)


def test_an_entry_that_prints_a_credential_is_refused(harness):
    broken = dataclasses.replace(
        harness, entry="tests.test_entry_conformance:prints_the_credential"
    )
    with pytest.raises(ConformanceError, match="rule 2: a secret appears in the output"):
        check_entry_carries_no_secret(broken)


def test_a_secret_in_the_work_item_is_refused(harness):
    """Items are logged and appear in state: a URI with a password in it is a leak
    before any entry runs."""
    item = dataclasses.replace(harness.item, uri=f"https://{SECRET}@catalog/")
    with pytest.raises(ConformanceError, match="rule 2: a secret appears in the work item"):
        check_entry_carries_no_secret(dataclasses.replace(harness, item=item))


def test_a_harness_naming_no_secret_checks_nothing_and_says_so(harness):
    with pytest.raises(ConformanceError, match="names no secret"):
        check_entry_carries_no_secret(dataclasses.replace(harness, secrets=()))


def test_a_claim_store_suite_does_not_load_the_worker_pool():
    """The module docstring's promise: an implementer of a claim store imports
    the suite without the pool, the scheduler and what they pull in."""
    code = "import sys, zamboni.conformance; print('zamboni.pool' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"
