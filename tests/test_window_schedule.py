# SPDX-License-Identifier: Apache-2.0
"""``WindowSchedule``: a local-time window, daylight saving, and how it fires.

The expected instants are derived from ``zoneinfo`` -- "the first instant whose
local time is not before the hour", "an hour that recurs an hour later" -- rather
than typed in as UTC literals, so each test states the rule of
docs/maintenance-runtime.md §4.2 it pins, not one zone's offsets.
"""

from __future__ import annotations

import pickle
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
import yaml

from zamboni.fleet import (
    RANDOM_LIMIT_MINUTES,
    RANDOM_PCT,
    FleetConfig,
    FleetConfigError,
    FleetWarehouse,
    WindowSchedule,
)
from zamboni.scheduler import SCHEDULE, CandidateQueue, Scheduler
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

#: Two zones from each hemisphere's rules, and one whose shift is half an hour.
#: For each: the local date the clocks go forward, and the one they go back.
TRANSITIONS = {
    "America/Toronto": (date(2026, 3, 8), date(2026, 11, 1)),
    "Europe/Berlin": (date(2026, 3, 29), date(2026, 10, 25)),
    "Australia/Sydney": (date(2026, 10, 4), date(2026, 4, 5)),
    "Australia/Lord_Howe": (date(2026, 10, 4), date(2026, 4, 5)),
}


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def wall(instant: datetime, zone: ZoneInfo) -> datetime:
    return instant.astimezone(zone).replace(tzinfo=None)


def window_on(schedule: WindowSchedule, day: date):
    """The firing of the window that opens on local ``day``."""
    midnight = datetime(day.year, day.month, day.day, tzinfo=schedule.zone)
    return schedule.firing_after(midnight - timedelta(seconds=1), "acme")


def gap_hour(zone: ZoneInfo, day: date) -> int:
    """An hour that does not exist on ``day``: its local time is skipped."""
    for hour in range(24):
        local = datetime(day.year, day.month, day.day, hour)
        if wall(local.replace(tzinfo=zone).astimezone(UTC), zone) != local:
            return hour
    pytest.fail(f"{zone} skips no whole hour on {day}")


def overlap_hour(zone: ZoneInfo, day: date) -> int:
    """An hour that occurs twice on ``day``."""
    for hour in range(24):
        local = datetime(day.year, day.month, day.day, hour)
        first = local.replace(tzinfo=zone).astimezone(UTC)
        second = local.replace(tzinfo=zone, fold=1).astimezone(UTC)
        if first != second:
            return hour
    pytest.fail(f"{zone} repeats no whole hour on {day}")


# -- daylight saving -------------------------------------------------------------


@pytest.mark.parametrize("timezone", TRANSITIONS)
def test_an_hour_in_the_spring_gap_resolves_to_the_first_instant_after_it(timezone):
    zone = ZoneInfo(timezone)
    day = TRANSITIONS[timezone][0]
    hour = gap_hour(zone, day)
    schedule = WindowSchedule(timezone, start_hour=hour, end_hour=(hour + 4) % 24, random=False)

    opens = window_on(schedule, day).nominal
    target = datetime(day.year, day.month, day.day, hour)
    # The first instant whose local time is not before the hour: forward, not back.
    assert wall(opens, zone) >= target
    assert wall(opens - timedelta(seconds=1), zone) < target


def test_an_hour_late_in_a_two_hour_gap_still_resolves_to_the_gap_s_end():
    """Antarctica/Troll goes from +00 to +02, skipping 01:00-03:00. Read with
    the offset from before the gap -- PEP 495's fold=0 -- 02:00 lands at 04:00,
    an hour after the gap ends; the rule is the first instant after it, 03:00.
    The whole-hour zones above cannot tell the two apart."""
    zone = ZoneInfo("Antarctica/Troll")
    day = date(2026, 3, 29)
    schedule = WindowSchedule("Antarctica/Troll", start_hour=2, end_hour=6, random=False)
    naive_reading = datetime(2026, 3, 29, 2, tzinfo=zone).astimezone(UTC)

    opens = window_on(schedule, day).nominal
    assert wall(opens - timedelta(seconds=1), zone) < datetime(2026, 3, 29, 2)
    assert wall(opens, zone) == datetime(2026, 3, 29, 3)
    assert opens < naive_reading


#: Lord Howe goes back half an hour, so no whole hour -- all a window can name --
#: occurs twice there.
WHOLE_HOUR_SHIFTS = [zone for zone in TRANSITIONS if zone != "Australia/Lord_Howe"]


@pytest.mark.parametrize("timezone", WHOLE_HOUR_SHIFTS)
def test_an_hour_in_the_autumn_overlap_resolves_to_its_first_occurrence(timezone):
    zone = ZoneInfo(timezone)
    day = TRANSITIONS[timezone][1]
    hour = overlap_hour(zone, day)
    schedule = WindowSchedule(timezone, start_hour=hour, end_hour=(hour + 4) % 24, random=False)

    opens = window_on(schedule, day).nominal
    target = datetime(day.year, day.month, day.day, hour)
    assert wall(opens, zone) == target
    assert opens.astimezone(zone).fold == 0
    # The same wall time recurs later; the window opened at the earlier one.
    recurs = target.replace(tzinfo=zone, fold=1).astimezone(UTC)
    assert recurs > opens and wall(recurs, zone) == target


@pytest.mark.parametrize("timezone", TRANSITIONS)
def test_a_window_is_shorter_or_longer_on_the_transition_days_by_the_shift(timezone):
    zone = ZoneInfo(timezone)
    spring, autumn = TRANSITIONS[timezone]
    # A window spanning the whole night covers whichever hour the zone moves.
    schedule = WindowSchedule(timezone, start_hour=22, end_hour=6, random=False)

    def shift(day: date) -> timedelta:
        before = datetime(day.year, day.month, day.day, tzinfo=zone) - timedelta(days=1)
        after = datetime(day.year, day.month, day.day, 12, tzinfo=zone)
        return after.utcoffset() - before.utcoffset()

    for day in (spring, autumn):
        firing = window_on(schedule, day - timedelta(days=1))
        assert firing.not_after - firing.nominal == schedule.length - shift(day)
    ordinary = window_on(schedule, spring - timedelta(days=7))
    assert ordinary.not_after - ordinary.nominal == schedule.length


@pytest.mark.parametrize("timezone", TRANSITIONS)
def test_no_window_vanishes_on_either_transition_day(timezone):
    """Every start and end hour, on both days."""
    for day in TRANSITIONS[timezone]:
        for start in range(24):
            for end in range(24):
                schedule = WindowSchedule(timezone, start, end, random=False)
                firing = window_on(schedule, day)
                assert firing.not_after > firing.nominal, (timezone, day, start, end)


@pytest.mark.parametrize("timezone", WHOLE_HOUR_SHIFTS)
def test_a_window_the_gap_would_empty_keeps_its_usual_length(timezone):
    """A one-hour window on the skipped hour: both ends resolve to the same
    instant, after the gap. Where the gap is shorter (Lord Howe's half hour)
    the window is only shortened, and the test above covers it."""
    spring = TRANSITIONS[timezone][0]
    hour = gap_hour(ZoneInfo(timezone), spring)
    inside = WindowSchedule(timezone, start_hour=hour, end_hour=hour + 1, random=False)
    firing = window_on(inside, spring)
    assert firing.not_after - firing.nominal == inside.length


# -- shape -------------------------------------------------------------------------


def test_a_window_fires_when_it_opens_and_its_close_is_the_deadline():
    schedule = WindowSchedule("America/Toronto", start_hour=1, end_hour=5, random=False)
    firing = schedule.firing_after(utc("2026-10-24T12:00"), "acme")

    assert wall(firing.nominal, schedule.zone) == datetime(2026, 10, 25, 1)
    assert firing.due == firing.nominal
    assert wall(firing.not_after, schedule.zone) == datetime(2026, 10, 25, 5)


def test_a_window_may_wrap_midnight():
    schedule = WindowSchedule("Europe/Berlin", start_hour=22, end_hour=4, random=False)
    firing = schedule.firing_after(utc("2026-10-20T12:00"), "acme")

    assert wall(firing.nominal, schedule.zone) == datetime(2026, 10, 20, 22)
    assert wall(firing.not_after, schedule.zone) == datetime(2026, 10, 21, 4)


def test_equal_hours_are_the_whole_day():
    schedule = WindowSchedule("UTC", start_hour=3, end_hour=3, random=False)
    firing = schedule.firing_after(utc("2026-10-20T12:00"), "acme")

    assert schedule.length == timedelta(hours=24)
    assert firing.not_after - firing.nominal == timedelta(hours=24)


def test_the_next_firing_after_one_is_the_next_window_not_the_same_one():
    schedule = WindowSchedule("America/Toronto", start_hour=1, end_hour=5)
    first = schedule.firing_after(utc("2026-10-20T12:00"), "acme")
    second = schedule.firing_after(first.nominal, "acme")
    assert second.nominal - first.nominal == timedelta(days=1)


def test_a_window_already_open_is_the_first_firing_and_one_not_yet_open_is_next():
    schedule = WindowSchedule("America/Toronto", start_hour=1, end_hour=5)
    inside = utc("2026-10-24T07:00")  # 03:00 in Toronto
    open_now = schedule.first_firing(inside, "acme")
    assert open_now.nominal < inside < open_now.not_after
    assert open_now.due <= inside + timedelta(minutes=RANDOM_LIMIT_MINUTES)

    outside = utc("2026-10-24T12:00")  # 08:00
    assert schedule.first_firing(outside, "acme") == schedule.firing_after(outside, "acme")


# -- spread --------------------------------------------------------------------------


@pytest.mark.parametrize(("start", "end"), [(1, 5), (22, 4), (3, 3), (1, 2)])
def test_spread_is_forward_only_and_inside_its_bound(start, end):
    schedule = WindowSchedule("America/Toronto", start_hour=start, end_hour=end)
    bound = min(schedule.length * RANDOM_PCT / 100, timedelta(minutes=RANDOM_LIMIT_MINUTES))
    after = utc("2026-06-01T12:00")
    offsets = [
        schedule.firing_after(after, f"w{i}").due - schedule.next_after(after) for i in range(300)
    ]

    assert min(offsets) >= timedelta(0), "a window must never start early"
    assert max(offsets) < bound
    # Actually spread, not collapsed onto one edge.
    assert max(offsets) - min(offsets) > bound / 2


def test_spread_is_derived_per_firing_and_stable_across_asks():
    schedule = WindowSchedule("America/Toronto", start_hour=1, end_hour=5)
    after = utc("2026-06-01T12:00")
    firings = []
    for _ in range(10):
        firings.append(schedule.firing_after(after, "acme"))
        after = firings[-1].nominal
    offsets = {f.due - f.nominal for f in firings}
    assert len(offsets) > 1, "the same offset every night pins one collision forever"
    assert schedule.firing_after(utc("2026-06-01T12:00"), "acme") == firings[0]


# -- validation ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"timezone": "Mars/Olympus_Mons"}, "unknown time zone"),
        ({"timezone": "America"}, "unknown time zone"),
        ({"timezone": "../etc/passwd"}, "unknown time zone"),
        ({"timezone": 5}, "IANA name"),
        ({"start_hour": 24}, "0-23"),
        ({"end_hour": -1}, "0-23"),
        ({"start_hour": True}, "0-23"),
        ({"end_hour": 4.5}, "0-23"),
        ({"min_interval_hours": -1}, "no shorter than the 4-hour window"),
        ({"min_interval_hours": 3}, "no shorter than the 4-hour window"),
        ({"min_interval_hours": float("inf")}, "min_interval_hours"),
        ({"min_interval_hours": float("nan")}, "min_interval_hours"),
        ({"min_interval_hours": "18"}, "min_interval_hours"),
        ({"random": "yes"}, "random must be true or false"),
    ],
)
def test_a_window_that_cannot_mean_what_it_says_is_refused_at_construction(kwargs, message):
    fields = {"timezone": "America/Toronto", "start_hour": 1, "end_hour": 5, **kwargs}
    with pytest.raises(FleetConfigError, match=message):
        WindowSchedule(**fields)


def test_the_interval_defaults_to_the_window_and_may_be_longer():
    assert WindowSchedule("UTC", 22, 4).min_interval == timedelta(hours=6)
    assert WindowSchedule("UTC", 5, 5).min_interval == timedelta(hours=24)
    assert WindowSchedule("UTC", 1, 5, min_interval_hours=18).min_interval == timedelta(hours=18)
    # The default is a value, not "unset": written out, it is the same schedule.
    assert WindowSchedule("UTC", 1, 5) == WindowSchedule("UTC", 1, 5, min_interval_hours=4)


def test_a_window_survives_pickling():
    schedule = WindowSchedule("America/Toronto", 1, 5, min_interval_hours=18)
    copy = pickle.loads(pickle.dumps(schedule))
    after = utc("2026-10-24T12:00")
    assert copy == schedule
    assert copy.firing_after(after, "acme") == schedule.firing_after(after, "acme")


# -- the fleet file ----------------------------------------------------------------


def fleet_doc(schedule, **root) -> dict:
    return {
        "warehouses": [
            {
                "name": "acme",
                "schedule": schedule,
                "table_config": {"namespaces": {"raw": {"tables": {"events": {}}}}},
            }
        ],
        **root,
    }


def test_the_fleet_file_declares_a_window():
    text = """
warehouses:
  - name: acme
    schedule:
      window:
        timezone: America/Toronto
        start_hour: 1
        end_hour: 5
        min_interval_hours: 18
    table_config:
      namespaces: {raw: {tables: {events: {}}}}
"""
    schedule = FleetConfig.from_dict(yaml.safe_load(text))["acme"].schedule
    assert schedule == WindowSchedule("America/Toronto", 1, 5, min_interval_hours=18)


def test_the_fleet_wide_random_switch_applies_to_a_window():
    window = {"window": {"timezone": "UTC", "start_hour": 1, "end_hour": 5}}
    assert FleetConfig.from_dict(fleet_doc(window))["acme"].schedule.random is True
    off = FleetConfig.from_dict(fleet_doc(window, random=False))["acme"].schedule
    assert off.random is False


@pytest.mark.parametrize(
    ("schedule", "message"),
    [
        ({"window": {"timezone": "UTC", "start_hour": 1}}, "'end_hour' is required"),
        ({"window": {"start_hour": 1, "end_hour": 5}}, "'timezone' is required"),
        (
            {"window": {"timezone": "UTC", "start_hour": 1, "end_hour": 5, "tz": "UTC"}},
            "unknown key",
        ),
        ({"windows": {}}, "unknown key"),
        ({}, "needs 'window'"),
        ({"window": "01-05"}, "expected a block"),
        ([1, 5], "cron expression string or a 'window' block"),
        (
            {"window": {"timezone": "Nowhere/Land", "start_hour": 1, "end_hour": 5}},
            "unknown time zone",
        ),
    ],
)
def test_a_bad_window_in_the_fleet_file_is_refused_and_located(schedule, message):
    with pytest.raises(FleetConfigError, match=message) as raised:
        FleetConfig.from_dict(fleet_doc(schedule))
    assert "warehouses[0] (acme)" in str(raised.value)


# -- the scheduler ---------------------------------------------------------------


def window_fleet(schedule: WindowSchedule) -> FleetConfig:
    return FleetConfig(
        warehouses=(
            FleetWarehouse(
                name="acme",
                schedule=schedule,
                table_config=TableConfig(
                    warehouse="acme",
                    namespaces={
                        "raw": NamespaceSettings(
                            tables={t: TableSettings() for t in ("events", "sessions")}
                        )
                    },
                ),
            ),
        )
    )


def test_a_window_already_open_at_start_fires_on_the_first_tick():
    """Unlike cron, whose fresh start waits: a restart inside the window keeps it."""
    schedule = WindowSchedule("America/Toronto", start_hour=1, end_hour=5)
    current = window_fleet(schedule)
    start = utc("2026-10-24T07:00")  # 03:00 in Toronto, two hours in
    scheduler, queue = Scheduler(current, start), CandidateQueue()

    assert scheduler.tick(start, queue) == ["acme"]
    items = [queue.take(current), queue.take(current)]
    closes = schedule.first_firing(start, "acme").not_after
    assert {i.table for i in items} == {"raw.events", "raw.sessions"}
    assert all(i.reason == SCHEDULE and i.not_after == closes for i in items)
    # Re-armed for tomorrow's window, not this one again.
    assert scheduler.next_firing("acme") > closes


def test_a_window_not_yet_open_at_start_waits_for_it():
    schedule = WindowSchedule("America/Toronto", start_hour=1, end_hour=5, random=False)
    start = utc("2026-10-24T12:00")
    scheduler = Scheduler(window_fleet(schedule), start)
    assert scheduler.tick(start, CandidateQueue()) == []
    assert scheduler.next_firing("acme") == schedule.next_after(start)


def test_a_window_missed_entirely_fires_once_with_its_deadline_already_past():
    """A host suspended through the night: the tables are offered once, and the
    deadline then stops each from starting in the working day."""
    schedule = WindowSchedule("America/Toronto", start_hour=1, end_hour=5)
    current = window_fleet(schedule)
    scheduler, queue = Scheduler(current, utc("2026-10-24T12:00")), CandidateQueue()

    late = utc("2026-10-25T14:00")  # 10:00 the next morning
    assert scheduler.tick(late, queue) == ["acme"]
    item = queue.take(current)
    assert item is not None and item.not_after < late
    assert scheduler.tick(late, queue) == []


def test_rearm_keeps_an_unchanged_window_and_rearms_a_changed_one():
    before = WindowSchedule("America/Toronto", start_hour=1, end_hour=5)
    start = utc("2026-10-24T12:00")
    scheduler = Scheduler(window_fleet(before), start)
    waiting = scheduler.next_firing("acme")

    scheduler.rearm(window_fleet(WindowSchedule("America/Toronto", 1, 5)), start)
    assert scheduler.next_firing("acme") == waiting

    later = WindowSchedule("America/Toronto", start_hour=2, end_hour=6)
    scheduler.rearm(window_fleet(later), start)
    assert scheduler.next_firing("acme") == later.first_firing(start, "acme").due


def test_a_window_warehouse_s_work_item_carries_its_interval_and_a_cron_one_none():
    schedule = WindowSchedule("America/Toronto", 1, 5, min_interval_hours=18)
    current = window_fleet(schedule)
    queue = CandidateQueue()
    Scheduler(current, utc("2026-10-24T07:00")).tick(utc("2026-10-24T07:00"), queue)
    item = queue.take(current)
    assert item is not None and item.min_interval == timedelta(hours=18)
    assert item.interval_rule == (timedelta(hours=18), ())


# -- the zone database -----------------------------------------------------------------


def test_a_window_needs_no_zone_database_on_the_host():
    """Distroless and Alpine images ship none. With the host's search path
    emptied, the zone must still resolve -- from the `tzdata` dependency."""
    import os
    import subprocess
    import sys

    code = "from zamboni.fleet import WindowSchedule; WindowSchedule('America/Toronto', 1, 5)"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "PYTHONTZPATH": ""},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
