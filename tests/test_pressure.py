# SPDX-License-Identifier: Apache-2.0
"""Delete pressure (#175): which tables are offered between schedules, and why.

The decision is a function of a health record, so it is tested on records: the
three thresholds, the tables it must never offer (copy-on-write, equality
deletes, unreported counters), and the settings that tune it.
"""

from __future__ import annotations

import dataclasses
import json
import subprocess
import sys

import pytest

from zamboni.pressure import (
    DEFAULT_CHECK_EVERY_MINUTES,
    DEFAULT_DELETE_FILES,
    DEFAULT_DELETE_RATIO,
    DEFAULT_DELETED_ROWS_RATIO,
    HEALTH_FIELDS,
    PressureSettings,
)

#: A merge-on-read table under every default threshold.
QUIET = {
    "data_files": 100,
    "delete_files": 2,
    "position_deletes": 10,
    "equality_deletes": 0,
    "records": 10_000,
}


def health(**changes) -> dict:
    return QUIET | changes


def test_the_defaults_are_the_ones_decided():
    """Chosen starting values, confirmed on #175 (2026-10-08). A changed
    default changes what a fleet compacts, so it is pinned."""
    s = PressureSettings()
    assert (
        (s.delete_files, s.delete_ratio, s.deleted_rows_ratio, s.check_every_minutes)
        == (
            DEFAULT_DELETE_FILES,
            DEFAULT_DELETE_RATIO,
            DEFAULT_DELETED_ROWS_RATIO,
            DEFAULT_CHECK_EVERY_MINUTES,
        )
        == (10, 0.10, 0.15, 15)
    )


def test_a_table_under_every_threshold_is_not_offered():
    assert PressureSettings().triggered(QUIET) is None


@pytest.mark.parametrize(
    ("changes", "named"),
    [
        ({"delete_files": 10}, "10 delete files"),
        ({"data_files": 20, "delete_files": 3}, "0.15 (threshold 0.1)"),
        ({"position_deletes": 1_500}, "1500 of 10000 rows"),
    ],
)
def test_any_one_threshold_met_triggers_and_says_which(changes, named):
    reason = PressureSettings().triggered(health(**changes))
    assert reason is not None and named in reason


def test_a_threshold_is_met_at_its_value_not_only_above_it():
    s = PressureSettings(delete_files=None, deleted_rows_ratio=None, delete_ratio=0.5)
    assert s.triggered(health(data_files=4, delete_files=2)) is not None
    assert s.triggered(health(data_files=5, delete_files=2)) is None


def test_a_copy_on_write_table_is_never_offered():
    """No delete files at all, however the ratios are set."""
    eager = PressureSettings(delete_files=1, delete_ratio=0.0001, deleted_rows_ratio=0.0001)
    assert eager.triggered(health(delete_files=0, position_deletes=0)) is None


def test_equality_deletes_make_a_table_ineligible():
    """Compaction refuses them (ZMBNI-11): offering would be refused, then
    offered again every check, forever."""
    over = health(delete_files=50, equality_deletes=3)
    assert PressureSettings().triggered(over) is None


def test_unreported_counters_meet_no_threshold():
    """ "Not reported" is not zero, and it is not over either."""
    s = PressureSettings()
    assert s.triggered(health(delete_files=None)) is None
    assert s.triggered(health(delete_files=50, equality_deletes=None)) is None, (
        "equality deletes might be there"
    )
    rows_only = PressureSettings(delete_files=None, delete_ratio=None)
    assert rows_only.triggered(health(position_deletes=5_000, records=None)) is None


def test_a_switched_off_threshold_does_not_trigger():
    s = PressureSettings(delete_files=None)
    assert s.triggered(health(delete_files=12, data_files=1_000)) is None


# -- settings ---------------------------------------------------------------------------------


def test_a_block_overrides_only_the_keys_it_names():
    base = PressureSettings(delete_files=40)
    s = PressureSettings.from_value({"check_every_minutes": 5}, base)
    assert (s.delete_files, s.check_every_minutes) == (40, 5)


def test_false_is_off_and_true_is_the_base_or_the_defaults():
    base = PressureSettings(delete_files=40)
    assert PressureSettings.from_value(False, base) is None
    assert PressureSettings.from_value(True, base) == base
    assert PressureSettings.from_value(True, None) == PressureSettings(), "on again: the defaults"
    assert PressureSettings.from_value({"delete_files": 3}, None).delete_files == 3


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({"delete_file": 3}, "unknown key"),
        ({"delete_files": 0}, "above 0"),
        ({"delete_files": 2.5}, "whole number"),
        ({"delete_files": True}, "whole number"),
        ({"delete_ratio": 0}, "above 0 and at most 1"),
        ({"deleted_rows_ratio": 1.5}, "above 0 and at most 1"),
        ({"check_every_minutes": 0}, "above 0"),
        ({"delete_files": None, "delete_ratio": None, "deleted_rows_ratio": None}, "false"),
        ("sometimes", "true, false or a block"),
    ],
)
def test_settings_that_cannot_mean_what_they_say_are_refused(value, message):
    with pytest.raises(ValueError, match=message):
        PressureSettings.from_value(value, None)


# -- agreement and weight ---------------------------------------------------------------------


def test_the_fields_read_are_table_healths():
    from zamboni.health import TableHealth

    assert set(HEALTH_FIELDS) <= {f.name for f in dataclasses.fields(TableHealth)}


def test_the_module_imports_no_engine():
    """The service evaluates pressure in its own process, which stays light (#154)."""
    heavy = ("pyiceberg", "duckdb", "pyarrow")
    code = (
        "import sys, json; import zamboni.pressure, zamboni.fleet; "
        f"print(json.dumps(sorted(m for m in {heavy!r} if m in sys.modules)))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert json.loads(out.stdout) == []
