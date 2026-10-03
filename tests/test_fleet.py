# SPDX-License-Identifier: Apache-2.0
"""The fleet file, and the cron schedules in it.

Two halves. The schedule tests pin crontab(5)'s semantics -- the day rule most of
all, which is the part a hand-written parser gets wrong -- and check the forward
search against a brute-force walk rather than against hand-computed dates. The
fleet tests are about the one property a reloaded config has to have: an invalid
one can never be constructed, whichever way it arrives.
"""

from __future__ import annotations

import json
import pickle
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from zamboni.fleet import (
    RANDOM_LIMIT_MINUTES,
    RANDOM_PCT,
    CronSchedule,
    FleetConfig,
    FleetConfigError,
    FleetWarehouse,
)
from zamboni.settings import SECRET_PROFILE_KEYS
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

DOCS = Path(__file__).resolve().parent.parent / "docs"


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def firings(schedule: CronSchedule, start: datetime, count: int) -> list[datetime]:
    out, t = [], start
    for _ in range(count):
        t = schedule.next_after(t)
        out.append(t)
    return out


# -- schedules ----------------------------------------------------------------


def test_either_day_field_matches_when_both_are_restricted():
    """crontab(5)'s own example: "30 4 1,15 * 5 would cause a command to be run
    at 4:30 am on the 1st and 15th of each month, plus every Friday"."""
    schedule = CronSchedule("30 4 1,15 * 5")
    got = {t.date().isoformat() for t in firings(schedule, at("2026-10-01T00:00"), 6)}
    # October 2026: the 1st and 15th are Thursdays; Fridays are the 2nd, 9th, 16th, 23rd.
    assert got == {
        "2026-10-01",
        "2026-10-02",
        "2026-10-09",
        "2026-10-15",
        "2026-10-16",
        "2026-10-23",
    }


def test_both_day_fields_must_match_when_one_is_a_star():
    """The rule keys on whether the field *starts with* `*`, not on whether the
    set is full -- so `*/2` in day-of-week still means "and", not "or"."""
    only_dom = CronSchedule("0 0 13 * *")
    assert all(t.day == 13 for t in firings(only_dom, at("2026-01-01T00:00"), 5))

    stepped = CronSchedule("0 0 1 * */2")  # the 1st, when it is Sun/Tue/Thu/Sat
    for t in firings(stepped, at("2026-01-01T00:00"), 5):
        assert t.day == 1
        assert t.isoweekday() % 7 in {0, 2, 4, 6}


def test_next_after_agrees_with_a_minute_by_minute_walk():
    """The search skips ahead by month, day and hour; a walk checks every
    minute. They must agree on the first firing from a range of starting points.
    """
    expressions = [
        "*/7 * * * *",
        "0 2 * * *",
        "15 3,15 * * 1-5",
        "0 0 1,15 * 5",
        "45 23 31 * *",
        "0 12 * jan,jul sun",
        "0 0 29 2 *",
    ]
    starts = [at("2026-02-27T23:58"), at("2026-12-31T23:59"), at("2027-06-15T11:11")]
    for expression in expressions:
        schedule = CronSchedule(expression)
        for start in starts:
            expected = schedule.next_after(start)
            if expected - start > timedelta(days=60):
                continue  # the Feb 29th case is covered on its own below
            t = start.replace(second=0) + timedelta(minutes=1)
            while not schedule.matches(t):
                t += timedelta(minutes=1)
            assert t == expected, (expression, start)


def test_a_leap_day_schedule_reaches_across_a_non_leap_century():
    """2100 is not a leap year, so from 2097 the next Feb 29th is in 2104. This is
    what the search horizon is sized for."""
    schedule = CronSchedule("0 0 29 2 *")
    assert schedule.next_after(at("2097-03-01T00:00")) == at("2104-02-29T00:00")


def test_next_after_is_strictly_after_and_in_utc():
    schedule = CronSchedule("0 2 * * *")
    assert schedule.next_after(at("2026-10-01T02:00")) == at("2026-10-02T02:00")

    # 02:00 UTC is 04:00 at +02:00: the schedule does not move with the caller's zone.
    plus_two = datetime(2026, 10, 1, 3, 0, tzinfo=timezone(timedelta(hours=2)))
    got = schedule.next_after(plus_two)
    assert got == at("2026-10-01T02:00")
    assert got.tzinfo is UTC


def test_a_naive_datetime_is_refused():
    with pytest.raises(ValueError, match="naive"):
        CronSchedule("0 2 * * *").next_after(datetime(2026, 10, 1))


def test_sunday_is_both_zero_and_seven_and_names_are_accepted():
    sundays = [CronSchedule(e) for e in ("0 0 * * 0", "0 0 * * 7", "0 0 * * sun", "0 0 * * SUN")]
    start = at("2026-10-01T00:00")
    assert len({s.next_after(start) for s in sundays}) == 1
    assert CronSchedule("0 0 1 mar *").next_after(start) == at("2027-03-01T00:00")


@pytest.mark.parametrize(
    ("nickname", "expression"),
    [("@daily", "0 0 * * *"), ("@hourly", "0 * * * *"), ("@weekly", "0 0 * * 0")],
)
def test_nicknames_mean_their_expansion(nickname, expression):
    start = at("2026-10-01T12:34")
    assert firings(CronSchedule(nickname), start, 3) == firings(CronSchedule(expression), start, 3)


@pytest.mark.parametrize(
    ("expression", "message"),
    [
        ("0 2 * *", "expected 5 fields"),
        ("60 * * * *", "outside 0-59"),
        ("0 24 * * *", "outside 0-23"),
        ("0 0 0 * *", "outside 1-31"),
        ("0 0 * 13 *", "outside 1-12"),
        ("0 0 * * 8", "outside 0-7"),
        ("5-1 * * * *", "runs backwards"),
        ("*/0 * * * *", "positive"),
        ("5/15 * * * *", "ambiguous"),
        ("x * * * *", "not a number"),
        ("@reboot", "unknown nickname"),
        ("0 0 30 2 *", "can never fire"),
        ("0 0 31 4,6,9,11 *", "can never fire"),
    ],
)
def test_bad_schedules_are_refused(expression, message):
    with pytest.raises(FleetConfigError, match=message):
        CronSchedule(expression)


def test_an_impossible_day_is_accepted_when_the_weekday_can_carry_it():
    """`0 0 30 2 1` is Feb 30th *or* any Monday, and Mondays happen."""
    assert CronSchedule("0 0 30 2 1").next_after(at("2026-10-01T00:00")).isoweekday() == 1


def test_a_schedule_survives_pickling():
    """Work items cross a process boundary in #121; the parsed sets must too."""
    schedule = CronSchedule("15 3,15 * * 1-5")
    copy = pickle.loads(pickle.dumps(schedule))
    start = at("2026-10-01T00:00")
    assert firings(copy, start, 5) == firings(schedule, start, 5)


# -- the fleet ----------------------------------------------------------------


def table_config_body(**tables: dict) -> dict:
    return {"namespaces": {"raw": {"tables": tables or {"events": {}}}}}


def write_table_config(path: Path, warehouse: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"warehouse": warehouse, **table_config_body()}))
    return path


def write_fleet(tmp_path: Path, doc: dict, name: str = "fleet.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(doc) if name.endswith((".yaml", ".yml")) else json.dumps(doc))
    return path


def entry(name: str = "acme", **overrides) -> dict:
    return {"name": name, "schedule": "0 2 * * *", "table_config": table_config_body(), **overrides}


def test_the_documented_fleet_file_loads(tmp_path):
    """The sample in event-driven-maintenance.md §4 is the one a provisioner
    author will copy, so it has to load against the current code."""
    text = (DOCS / "event-driven-maintenance.md").read_text()
    section = text.split("## 4. Configuration", 1)[1]
    sample = re.search(r"```yaml\n(.*?)```", section, re.S)
    assert sample, "the fleet sample was not found; has §4 changed?"
    raw = yaml.safe_load(sample.group(1))

    # The sample references a file by path; supply it where the path says.
    for warehouse in raw["warehouses"]:
        if isinstance(warehouse["table_config"], str):
            write_table_config(tmp_path / warehouse["table_config"], warehouse["name"])

    fleet = FleetConfig.from_dict(raw, base=tmp_path)
    assert fleet.names == tuple(w["name"] for w in raw["warehouses"])


def test_table_config_by_path_and_inline(tmp_path):
    write_table_config(tmp_path / "configs" / "globex.json", "globex")
    path = write_fleet(
        tmp_path,
        {
            "warehouses": [
                entry("acme", uri="https://catalog.internal/catalog"),
                entry("globex", table_config="configs/globex.json"),
            ]
        },
    )

    fleet = FleetConfig.load(path)

    assert fleet["acme"].uri == "https://catalog.internal/catalog"
    assert fleet["globex"].uri is None
    # The tables maintained are the table config's, not a second list.
    assert set(fleet["acme"].table_config.tables) == {"raw.events"}
    assert set(fleet["globex"].table_config.tables) == {"raw.events"}
    # Both files are sources, so a reload can watch the one the fleet points at.
    assert fleet.sources == (path, tmp_path / "configs" / "globex.json")


def test_json_loads_the_same_as_yaml(tmp_path):
    doc = {"warehouses": [entry()]}
    from_yaml = FleetConfig.load(write_fleet(tmp_path, doc, "fleet.yaml"))
    from_json = FleetConfig.load(write_fleet(tmp_path, doc, "fleet.json"))
    assert from_yaml.warehouses == from_json.warehouses


def test_an_empty_fleet_is_refused(tmp_path):
    """A generator that fails open writes an empty list. Accepting it on reload
    would stop every warehouse by being wrong."""
    with pytest.raises(FleetConfigError, match="no warehouses"):
        FleetConfig.load(write_fleet(tmp_path, {"warehouses": []}))
    with pytest.raises(FleetConfigError, match="'warehouses' is required"):
        FleetConfig.load(write_fleet(tmp_path, {"version": 1}))


def test_a_warehouse_listed_twice_is_refused(tmp_path):
    with pytest.raises(FleetConfigError, match="declared twice"):
        FleetConfig.load(write_fleet(tmp_path, {"warehouses": [entry(), entry()]}))


def test_a_table_config_for_another_warehouse_is_refused(tmp_path):
    write_table_config(tmp_path / "acme.json", "acme")
    by_path = {"warehouses": [entry("globex", table_config="acme.json")]}
    with pytest.raises(FleetConfigError, match="describes warehouse 'acme'"):
        FleetConfig.load(write_fleet(tmp_path, by_path))

    inline = {
        "warehouses": [entry("globex", table_config={"warehouse": "acme", **table_config_body()})]
    }
    with pytest.raises(FleetConfigError, match="describes warehouse 'acme'"):
        FleetConfig.load(write_fleet(tmp_path, inline))


def test_a_table_config_error_names_where_it_is(tmp_path):
    bad = entry(table_config=table_config_body(events={"min_input_files": "day"}))
    with pytest.raises(FleetConfigError) as caught:
        FleetConfig.load(write_fleet(tmp_path, {"warehouses": [bad]}))
    message = str(caught.value)
    assert "fleet.yaml" in message
    assert "warehouses[0] (acme).table_config" in message
    assert "min_input_files" in message


def test_a_missing_table_config_file_is_a_config_error(tmp_path):
    with pytest.raises(FleetConfigError, match="table_config"):
        FleetConfig.load(write_fleet(tmp_path, {"warehouses": [entry(table_config="nope.json")]}))


@pytest.mark.parametrize(
    ("doc", "message"),
    [
        ({"warehouses": [entry(tables={})]}, "unknown key"),
        ({"warehouses": [entry()], "defaults": {}}, "unknown key"),
        ({"warehouses": {"acme": entry()}}, "expected a list"),
        ({"warehouses": ["acme"]}, "expected a block"),
        ({"warehouses": [entry(name="")]}, "non-empty string"),
        ({"warehouses": [entry(uri="")]}, "'uri' must be a non-empty string"),
        ({"warehouses": [entry(table_config=7)]}, "expected a path"),
        ({"warehouses": [{"name": "acme", "table_config": {}}]}, "'schedule' is required"),
        ({"warehouses": [entry(schedule=5)]}, "cron expression string"),
        ({"version": 2, "warehouses": [entry()]}, "version 2"),
        ({"version": True, "warehouses": [entry()]}, "expected a number"),
    ],
)
def test_malformed_fleets_are_refused(tmp_path, doc, message):
    with pytest.raises(FleetConfigError, match=message):
        FleetConfig.load(write_fleet(tmp_path, doc))


@pytest.mark.parametrize("key", sorted(SECRET_PROFILE_KEYS))
def test_a_credential_is_refused_by_name(tmp_path, key):
    """Derived from the profile's own list of secret keys, so a key added there
    is refused here without anyone remembering to."""
    for doc in ({"warehouses": [entry(**{key: "x"})]}, {"warehouses": [entry()], key: "x"}):
        with pytest.raises(FleetConfigError, match="is a credential"):
            FleetConfig.load(write_fleet(tmp_path, doc))


def test_an_unquoted_star_schedule_says_to_quote_it(tmp_path):
    path = tmp_path / "fleet.yaml"
    path.write_text("warehouses:\n  - name: acme\n    schedule: */5 * * * *\n")
    with pytest.raises(FleetConfigError, match="must be quoted"):
        FleetConfig.load(path)


# -- the same state, built in code ---------------------------------------------


def table_config(warehouse: str) -> TableConfig:
    return TableConfig(
        warehouse=warehouse,
        namespaces={"raw": NamespaceSettings(tables={"events": TableSettings()})},
    )


def warehouse(name: str = "acme", **overrides) -> FleetWarehouse:
    fields = {"schedule": CronSchedule("0 2 * * *"), "table_config": table_config(name)}
    return FleetWarehouse(name=name, **{**fields, **overrides})


def test_a_fleet_built_in_code_equals_one_loaded(tmp_path):
    """The integrator path is the same object, not a lookalike."""
    loaded = FleetConfig.from_dict({"warehouses": [entry()]})
    built = FleetConfig(warehouses=[warehouse()])

    assert isinstance(built.warehouses, tuple)
    assert built.names == loaded.names
    assert built["acme"].schedule == loaded["acme"].schedule
    assert built["acme"].table_config.tables == loaded["acme"].table_config.tables


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: FleetConfig(warehouses=()), "no warehouses"),
        (lambda: FleetConfig(warehouses=(warehouse(), warehouse())), "declared twice"),
        (lambda: warehouse("globex", table_config=table_config("acme")), "describes warehouse"),
        (lambda: warehouse(schedule="0 2 * * *"), "must be a CronSchedule"),
        (lambda: warehouse(table_config=TableConfig()), "'warehouse' is required"),
        (lambda: FleetConfig(warehouses=("acme",)), "must be FleetWarehouse"),
    ],
)
def test_code_is_held_to_the_same_rules(build, message):
    with pytest.raises(FleetConfigError, match=message):
        build()


def test_a_fleet_survives_pickling():
    fleet = FleetConfig(warehouses=[warehouse("acme"), warehouse("globex")])
    assert pickle.loads(pickle.dumps(fleet)) == fleet


# -- spreading a fleet's firings (ZMBNI-144) ------------------------------------


def daily_span() -> timedelta:
    """What the constants promise for a daily schedule, derived rather than typed."""
    return min(timedelta(hours=24) * RANDOM_PCT / 100, timedelta(minutes=RANDOM_LIMIT_MINUTES))


def test_a_daily_firing_moves_within_the_documented_window():
    """5% of 24 h is 72 min, capped at 30: a 02:00 lands between 01:30 and 02:30."""
    assert daily_span() == timedelta(minutes=30)
    schedule = CronSchedule("0 2 * * *", random=True)
    nominal = at("2026-10-01T02:00")
    offsets = [schedule.offset(nominal, f"warehouse-{i}") for i in range(500)]

    assert all(-daily_span() <= o <= daily_span() for o in offsets)
    # It actually spreads: both directions, and most of the window used.
    assert min(offsets) < -daily_span() * 0.9
    assert max(offsets) > daily_span() * 0.9


def test_a_short_interval_moves_by_its_percentage_not_the_cap():
    schedule = CronSchedule("*/5 * * * *", random=True)
    bound = timedelta(minutes=5) * RANDOM_PCT / 100  # 15 s
    nominal = at("2026-10-01T02:05")
    offsets = [schedule.offset(nominal, f"w{i}") for i in range(200)]
    assert all(abs(o) <= bound for o in offsets)
    assert max(abs(o) for o in offsets) > bound * 0.9


def test_without_random_nothing_moves():
    exact = CronSchedule("0 2 * * *", random=False)
    assert exact.offset(at("2026-10-01T02:00"), "acme") == timedelta(0)


def test_the_offset_is_stable_across_processes():
    """Python salts `hash()` per process; an offset built on it would move a
    warehouse's run time on every restart. Run it under two hash seeds."""
    import os
    import subprocess
    import sys

    code = (
        "from datetime import datetime, UTC;"
        "from zamboni.fleet import CronSchedule;"
        "print(CronSchedule('0 2 * * *', random=True)"
        ".offset(datetime(2026, 10, 1, 2, tzinfo=UTC), 'acme'))"
    )
    answers = {
        subprocess.run(
            [sys.executable, "-c", code],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for seed in ("1", "2")
    }
    assert len(answers) == 1, answers


def test_each_night_gets_a_fresh_offset():
    """Decided 2026-10-01: two warehouses that collide tonight should not be
    stuck colliding every night, so the offset varies by firing."""
    schedule = CronSchedule("0 2 * * *")
    nights = [at("2026-10-01T02:00") + timedelta(days=d) for d in range(30)]
    offsets = {schedule.offset(n, "acme") for n in nights}
    assert len(offsets) > 25
    # ...and asking twice about one night gives one answer.
    assert schedule.offset(nights[0], "acme") == schedule.offset(nights[0], "acme")


def test_random_is_a_change_to_the_schedule():
    """So a reload that turns it on re-arms the warehouse."""
    assert CronSchedule("0 2 * * *") != CronSchedule("0 2 * * *", random=False)
    with pytest.raises(FleetConfigError, match="true or false"):
        CronSchedule("0 2 * * *", random="yes")  # type: ignore[arg-type]


def test_random_in_the_fleet_file_defaults_fleet_wide_and_a_warehouse_overrides(tmp_path):
    default = FleetConfig.load(write_fleet(tmp_path, {"warehouses": [entry()]}))
    assert default["acme"].schedule.random is True, "spreading is on unless switched off"

    doc = {"random": False, "warehouses": [entry("acme"), entry("globex", random=True)]}
    fleet = FleetConfig.load(write_fleet(tmp_path, doc))
    assert fleet["acme"].schedule.random is False
    assert fleet["globex"].schedule.random is True


@pytest.mark.parametrize(
    "doc",
    [{"random": "yes", "warehouses": [entry()]}, {"warehouses": [entry(random=1)]}],
)
def test_random_must_be_a_boolean(tmp_path, doc):
    with pytest.raises(FleetConfigError, match="true or false"):
        FleetConfig.load(write_fleet(tmp_path, doc))
