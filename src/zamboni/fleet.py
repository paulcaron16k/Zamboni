# SPDX-License-Identifier: Apache-2.0
"""The fleet file: which warehouses exist, their tables, and when to look at them.

One of the two configuration files ``zamboni serve`` reads, split from the other
by who writes it (docs/event-driven-maintenance.md §4):

* **the fleet file** -- written by a provisioning system, read by Zamboni.
  Conceptually a dump of that system's database: warehouses, their table
  configuration and a schedule each. It changes whenever a tenant is
  provisioned, and **nothing in it is secret**.
* ``zamboni.yml`` + ``.env`` -- written by the operator, changing when the
  deployment does. Catalog auth, storage credentials, engine, spill directory.
  Unchanged by this module; see :mod:`zamboni.settings`.

**This file is the one source of which tables exist.** An integrator holding the
same state in a database constructs :class:`FleetConfig` directly rather than
writing a file for Zamboni to read back -- the same validated object, not a
second store. Nothing here writes a fleet file, so there is no path by which
the two can disagree (the decision recorded on #106, 2026-09-30).

**It composes with ``table-config.json`` rather than replacing it.** Each
warehouse's ``table_config`` is either a path to one of the existing files or
the same body written inline; either way it is parsed and validated by
:class:`~zamboni.tableconfig.TableConfig`, so the fleet file adds a schedule and
a list and not a second table format.

**Validation happens at construction.** A fleet file is machine-generated from
another system's database, and the service reloads it while running (#119), so
the invariant that matters is that an invalid one can never become the running
config. Checking in ``__post_init__`` rather than in a separate ``validate()``
means that holds for the programmatic path too: there is no unvalidated
``FleetConfig`` to hand to a scheduler.
"""

from __future__ import annotations

import calendar
import hashlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .pressure import PressureSettings
from .settings import SECRET_PROFILE_KEYS, describe_yaml_error
from .tableconfig import TableConfig, TableConfigError
from .urls import carries_userinfo

#: The fleet file's own format version, independent of table-config's.
FLEET_VERSION = 1

_ROOT_KEYS = frozenset({"version", "warehouses", "random", "pressure"})
_WAREHOUSE_KEYS = frozenset({"name", "uri", "schedule", "table_config", "random", "pressure"})


class FleetConfigError(ValueError):
    """The fleet file, or a fleet built in code, is unusable.

    Raised at load or construction, never mid-run -- so a reload that raises
    this leaves the running config in place.
    """


# -- schedules --------------------------------------------------------------


#: ``(low, high)`` per cron field, in crontab(5) order.
_FIELDS = (
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day-of-month", 1, 31),
    ("month", 1, 12),
    ("day-of-week", 0, 7),
)

_MONTH_NAMES = {name.lower(): i for i, name in enumerate(calendar.month_abbr) if name}
#: Sunday is 0 here, as in crontab(5); `calendar.day_abbr` starts on Monday.
_DAY_NAMES = {name.lower(): (i + 1) % 7 for i, name in enumerate(calendar.day_abbr)}

#: The crontab(5) nicknames with a fixed meaning. ``@reboot`` is deliberately
#: absent: it is an event, not a schedule, and a service that restarts under
#: Kubernetes would run it at every restart.
_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

#: The longest a February 29th can be away: 2096 to 2104, because 2100 is not a
#: leap year. Bounds the search in :meth:`CronSchedule.next_after`, which
#: construction has already proved will find a match.
_SEARCH_HORIZON = timedelta(days=366 * 9)

#: With ``random=True`` (the default), how far a firing may move, as a percentage of the gap
#: to the following firing -- either way. Kept under 50 so a moved firing can
#: never pass its neighbour.
RANDOM_PCT = 5
#: ...and the cap on that, either way. A daily 02:00 is 5% of 24 h = 72 min,
#: capped to 30: it lands between 01:30 and 02:30.
RANDOM_LIMIT_MINUTES = 30


@dataclass(frozen=True)
class Firing:
    """One moment a schedule offers its warehouse's tables (docs/maintenance-runtime.md §4).

    Both schedule types produce these, so the scheduler arms, compares and
    re-arms without knowing which one it holds.
    """

    #: What the schedule names: a cron minute, or the instant a window opens.
    nominal: datetime
    #: When the tables are actually offered: ``nominal`` plus the spread.
    due: datetime
    #: A worker does not start a table after this instant (§3.3): a window's
    #: close. ``None`` for cron, which has no deadline.
    not_after: datetime | None = None


@dataclass(frozen=True)
class CronSchedule:
    """A five-field cron expression, evaluated in **UTC**.

    Written here rather than taken as a dependency because the whole of it is a
    parser and a forward search, and the part that is easy to get wrong -- the
    day rule below -- has to be pinned by our own tests either way.

    **UTC, not local time.** A schedule in a zone with daylight saving has
    wall-clock times that happen twice and times that do not happen at all, and
    cron implementations disagree about both. A ``timezone`` key can be added
    later without changing what an existing schedule means; the reverse -- a
    default zone that later changes -- would move every backstop run.

    **The day rule is crontab(5)'s:** when *both* day-of-month and day-of-week
    are restricted (do not start with ``*``), a day matches if *either* does. So
    ``30 4 1,15 * 5`` is the 1st, the 15th, and every Friday -- the man page's
    own example, and a test here.

    Every field accepts ``*``, ``n``, ``a-b``, ``*/s``, ``a-b/s`` and comma
    lists of those; months and days accept the three-letter English names;
    day-of-week 7 is Sunday. ``n/s`` is refused: some crons read it as
    ``n-max/s`` and others reject it, and a schedule that means different
    things in different places is not one to accept silently.

    **Firings are spread by default** (``random=True``, ZMBNI-144), so
    warehouses that share an expression do not all start in the same minute.
    Each firing moves by up to ±:data:`RANDOM_PCT` percent of the gap to the
    next one, capped at ±:data:`RANDOM_LIMIT_MINUTES`. See :meth:`offset`.

    On by default because the default is the mode the system is designed to
    survive the worst case in: a fleet provisioned from one template, every
    warehouse at ``0 2 * * *``, against one object store shared with everything
    else scheduled on the hour. ``random=False`` fires on the exact minute, and
    is the operator's choice to make knowingly.
    """

    expression: str
    random: bool = True
    minutes: frozenset[int] = field(init=False, repr=False, compare=False)
    hours: frozenset[int] = field(init=False, repr=False, compare=False)
    days_of_month: frozenset[int] = field(init=False, repr=False, compare=False)
    months: frozenset[int] = field(init=False, repr=False, compare=False)
    days_of_week: frozenset[int] = field(init=False, repr=False, compare=False)
    #: Whether day-of-month / day-of-week start with ``*``, which is what
    #: crontab(5) keys the either-matches rule on -- not whether the set is full.
    dom_star: bool = field(init=False, repr=False, compare=False)
    dow_star: bool = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.random, bool):
            raise FleetConfigError(
                f"schedule {self.expression!r}: random must be true or false, "
                f"not {type(self.random).__name__}"
            )
        if not isinstance(self.expression, str):
            raise FleetConfigError(
                f"a schedule is a cron expression string, not {type(self.expression).__name__}"
            )
        text = self.expression.strip()
        text = _MACROS.get(text.lower(), text)
        if text.startswith("@"):
            raise FleetConfigError(
                f"schedule {self.expression!r}: unknown nickname; allowed: {sorted(_MACROS)}"
            )
        parts = text.split()
        if len(parts) != len(_FIELDS):
            raise FleetConfigError(
                f"schedule {self.expression!r}: expected 5 fields "
                "(minute hour day-of-month month day-of-week), found "
                f"{len(parts)}"
            )
        parsed = [
            _parse_field(part, name, low, high, self.expression)
            for part, (name, low, high) in zip(parts, _FIELDS, strict=True)
        ]
        # Sunday is both 0 and 7; normalise so matching need only know one.
        parsed[4] = frozenset(d % 7 for d in parsed[4])
        for attr, values in zip(
            ("minutes", "hours", "days_of_month", "months", "days_of_week"), parsed, strict=True
        ):
            object.__setattr__(self, attr, values)
        object.__setattr__(self, "dom_star", parts[2].startswith("*"))
        object.__setattr__(self, "dow_star", parts[4].startswith("*"))

        # `0 0 30 2 *` parses and never fires. Refused here because a backstop
        # schedule that never fires is a warehouse that is never maintained,
        # and nothing downstream would say so. Only reachable when day-of-week
        # is `*`: otherwise either field can match, and every weekday recurs.
        if not self.dom_star and self.dow_star:
            longest = max(calendar.monthrange(2024, m)[1] for m in self.months)  # 2024: leap
            if min(self.days_of_month) > longest:
                raise FleetConfigError(
                    f"schedule {self.expression!r} can never fire: no selected month "
                    f"has a day {min(self.days_of_month)}"
                )

    def matches(self, moment: datetime) -> bool:
        """Whether ``moment``'s minute is one this schedule fires on."""
        t = _as_utc(moment)
        return (
            t.minute in self.minutes
            and t.hour in self.hours
            and t.month in self.months
            and self._day_matches(t)
        )

    def next_after(self, moment: datetime) -> datetime:
        """The first firing strictly after ``moment``, as an aware UTC datetime."""
        start = _as_utc(moment).replace(second=0, microsecond=0) + timedelta(minutes=1)
        t = start
        while t - start <= _SEARCH_HORIZON:
            if t.month not in self.months:
                t = _first_of_next_month(t)
            elif not self._day_matches(t):
                t = t.replace(hour=0, minute=0) + timedelta(days=1)
            elif t.hour not in self.hours:
                t = t.replace(minute=0) + timedelta(hours=1)
            else:
                later = [m for m in self.minutes if m >= t.minute]
                if later:
                    return t.replace(minute=min(later))
                t = t.replace(minute=0) + timedelta(hours=1)
        # Unreachable: construction refused every schedule that cannot fire,
        # and every one that can fires within the horizon.
        raise AssertionError(f"schedule {self.expression!r} found no firing after {moment}")

    def offset(self, nominal: datetime, seed: str) -> timedelta:
        """How far the firing at ``nominal`` moves; zero unless ``random``.

        ``min(RANDOM_PCT% of the gap to the next firing, RANDOM_LIMIT_MINUTES)``,
        scaled by a fraction in [-1, 1) derived from ``seed`` -- the warehouse's
        name, in the scheduler -- *and* ``nominal``.

        **A fresh offset for every firing** (decided 2026-10-01), as systemd's
        ``RandomizedDelaySec`` draws one per iteration: two large warehouses
        that land together tonight are unlikely to land together tomorrow, so
        no pair is stuck colliding every night.

        **Derived, not drawn.** A SHA-256 of seed and firing rather than a
        random number generator, so asking twice gives one answer: a restart or
        a reload at 01:45 does not re-roll tonight's 02:00 and move it past a
        window that has already opened. Not ``hash()``, which Python salts per
        process -- that would re-roll on every restart, which is the property
        this exists to avoid.

        The gap is measured forward, so an irregular schedule (``0 2,3 * * *``)
        moves each firing by its own share.
        """
        if not self.random:
            return timedelta(0)
        gap = self.next_after(nominal) - _as_utc(nominal)
        span = min(gap * RANDOM_PCT / 100, timedelta(minutes=RANDOM_LIMIT_MINUTES))
        fraction = _draw(seed, nominal) / 2**63 - 1  # [-1, 1)
        # Whole seconds: nothing downstream is finer, and it keeps a due time
        # readable in a log or a state file.
        return timedelta(seconds=round(span.total_seconds() * fraction))

    @property
    def min_interval(self) -> None:
        """Cron has no per-table minimum interval; see :class:`WindowSchedule`."""
        return None

    def firing_after(self, moment: datetime, seed: str) -> Firing:
        """The first firing strictly after ``moment``, spread for ``seed``."""
        nominal = self.next_after(moment)
        return Firing(nominal, nominal + self.offset(nominal, seed))

    def first_firing(self, now: datetime, seed: str) -> Firing:
        """The firing a fresh start waits for: the next one, as cron would."""
        return self.firing_after(now, seed)

    def _day_matches(self, t: datetime) -> bool:
        dom = t.day in self.days_of_month
        dow = t.isoweekday() % 7 in self.days_of_week
        if self.dom_star or self.dow_star:
            return dom and dow
        return dom or dow


def _parse_field(text: str, name: str, low: int, high: int, expression: str) -> frozenset[int]:
    names = _MONTH_NAMES if name == "month" else _DAY_NAMES if name == "day-of-week" else {}
    where = f"schedule {expression!r}, {name} field {text!r}"

    def number(token: str) -> int:
        value = names.get(token.lower())
        if value is None:
            if not token.isdigit():
                raise FleetConfigError(f"{where}: {token!r} is not a number")
            value = int(token)
        if not low <= value <= high:
            raise FleetConfigError(f"{where}: {value} is outside {low}-{high}")
        return value

    values: set[int] = set()
    for item in text.split(","):
        spec, slash, step_text = item.partition("/")
        step = 1
        if slash:
            if not step_text.isdigit() or int(step_text) == 0:
                raise FleetConfigError(f"{where}: step {step_text!r} must be a positive number")
            step = int(step_text)
        if spec == "*":
            start, end = low, high
        elif "-" in spec:
            first, _, last = spec.partition("-")
            start, end = number(first), number(last)
            if start > end:
                raise FleetConfigError(f"{where}: range {spec!r} runs backwards")
        elif slash:
            raise FleetConfigError(
                f"{where}: {item!r} is ambiguous -- crons disagree on whether it means "
                f"'{spec}-{high}/{step_text}' or is an error. Write the range out."
            )
        else:
            start = end = number(spec)
        values.update(range(start, end + 1, step))
    return frozenset(values)


def _as_utc(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        # A naive datetime would be read as local time by `astimezone`, which
        # is the zone confusion the UTC rule exists to avoid.
        raise ValueError("a schedule is evaluated against an aware datetime; got a naive one")
    return moment.astimezone(UTC)


def _first_of_next_month(t: datetime) -> datetime:
    year, month = (t.year + 1, 1) if t.month == 12 else (t.year, t.month + 1)
    return t.replace(year=year, month=month, day=1, hour=0, minute=0)


def _draw(seed: str, nominal: datetime) -> int:
    """A 64-bit number fixed by ``seed`` and the firing; see :meth:`CronSchedule.offset`."""
    firing = _as_utc(nominal).isoformat()
    digest = hashlib.sha256(f"{seed}@{firing}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


#: How many local days either side of a moment :meth:`WindowSchedule._windows`
#: looks at. One back finds a window that opened yesterday and wraps midnight;
#: the rest forward cover the longest gap a zone has ever skipped --
#: Pacific/Apia dropped the whole of 2011-12-30 -- with a day to spare.
_WINDOW_DAYS = (-1, 0, 1, 2, 3)


@dataclass(frozen=True)
class WindowSchedule:
    """ "Maintain me overnight, in my time zone" (docs/maintenance-runtime.md §4.2).

    A half-open window ``[start_hour, end_hour)`` of local wall-clock time in
    ``timezone``, every day. It may wrap midnight (``22`` to ``4``), and
    ``start_hour == end_hour`` is the whole day. **It fires when the window
    opens**, with the close as the firing's deadline, so a table not started
    by then waits for the next window rather than running into the working day.

    **Daylight saving, resolved one way** (§4.2 point 3): an hour that does not
    exist that day resolves to the first instant after the gap, and an hour
    that occurs twice to its first occurrence. A window is therefore an hour
    shorter or longer on the two transition days. It never vanishes: one that
    the gap would empty -- ``[2, 3)`` in America/Toronto on the spring day, both
    ends resolving to 03:00 EDT -- closes its usual length after it opens.
    That case is not in §4.2, which promises the window never vanishes without
    saying how; this is the rule that keeps the promise.

    **Spread is forward only.** With ``random`` the firing moves by
    ``[0, min(RANDOM_PCT% of that day's window, RANDOM_LIMIT_MINUTES))``,
    derived as cron's is, because a window that opens early runs during the
    hours its owner excluded.

    **A window already open fires at once** (:meth:`first_firing`), unlike
    cron: a window is a period during which work is wanted, and a restart inside
    it should not forfeit it.

    ``min_interval_hours`` is how long a table must have gone since Zamboni last
    maintained it before a firing runs it again, read from the table's own
    watermark. It defaults to the window's length and may not be shorter: a
    shorter one would allow two runs in one window, which is never what was
    meant.
    """

    timezone: str
    start_hour: int
    end_hour: int
    min_interval_hours: float | None = None
    random: bool = True
    zone: ZoneInfo = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        where = f"window {self.timezone!r} {self.start_hour}-{self.end_hour}"
        if not isinstance(self.random, bool):
            raise FleetConfigError(
                f"{where}: random must be true or false, not {type(self.random).__name__}"
            )
        if not isinstance(self.timezone, str):
            raise FleetConfigError(
                f"{where}: timezone must be an IANA name such as 'America/Toronto', "
                f"not {type(self.timezone).__name__}"
            )
        try:
            zone = ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError, OSError):
            # OSError: a key naming a directory of the database ("America").
            raise FleetConfigError(
                f"{where}: unknown time zone {self.timezone!r}; expected an IANA name "
                "such as 'America/Toronto'"
            ) from None
        object.__setattr__(self, "zone", zone)
        for key in ("start_hour", "end_hour"):
            hour = getattr(self, key)
            if not isinstance(hour, int) or isinstance(hour, bool) or not 0 <= hour <= 23:
                raise FleetConfigError(f"{where}: {key} must be a whole hour, 0-23; got {hour!r}")
        length = self.length / timedelta(hours=1)
        interval = self.min_interval_hours
        if interval is None:
            object.__setattr__(self, "min_interval_hours", length)
        elif (
            not isinstance(interval, int | float)
            or isinstance(interval, bool)
            or not interval < float("inf")
            or interval < length
        ):
            raise FleetConfigError(
                f"{where}: min_interval_hours must be a number of hours no shorter than "
                f"the {length:g}-hour window, or a table could run twice in one window; "
                f"got {interval!r}"
            )

    @property
    def length(self) -> timedelta:
        """The window's length on a day without a daylight-saving transition."""
        return timedelta(hours=(self.end_hour - self.start_hour) % 24 or 24)

    @property
    def min_interval(self) -> timedelta:
        assert self.min_interval_hours is not None  # set in __post_init__
        return timedelta(hours=self.min_interval_hours)

    def next_after(self, moment: datetime) -> datetime:
        """The first opening strictly after ``moment``, as an aware UTC datetime."""
        return self._window_after(moment)[0]

    def firing_after(self, moment: datetime, seed: str) -> Firing:
        """The first window opening strictly after ``moment``, spread for ``seed``."""
        return self._firing(*self._window_after(moment), seed)

    def first_firing(self, now: datetime, seed: str) -> Firing:
        """The window open at ``now``, whose ``due`` may be past; else the next one."""
        now = _as_utc(now)
        # In order of opening, the first window not yet closed either holds
        # `now` or is the next to open.
        for opens, closes in self._windows(now):
            if now < closes:
                return self._firing(opens, closes, seed)
        raise AssertionError(f"{self!r} found no window around {now}")  # see _WINDOW_DAYS

    def _window_after(self, moment: datetime) -> tuple[datetime, datetime]:
        moment = _as_utc(moment)
        for opens, closes in self._windows(moment):
            if opens > moment:
                return opens, closes
        raise AssertionError(f"{self!r} found no window after {moment}")  # see _WINDOW_DAYS

    def _firing(self, opens: datetime, closes: datetime, seed: str) -> Firing:
        if not self.random:
            return Firing(opens, opens, closes)
        span = min((closes - opens) * RANDOM_PCT / 100, timedelta(minutes=RANDOM_LIMIT_MINUTES))
        fraction = _draw(seed, opens) / 2**64  # [0, 1)
        # Whole seconds, as cron's; truncated rather than rounded so the offset
        # stays below the span.
        return Firing(
            opens, opens + timedelta(seconds=int(span.total_seconds() * fraction)), closes
        )

    def _windows(self, moment: datetime) -> Iterator[tuple[datetime, datetime]]:
        """The windows of the local days around ``moment``, in order of opening."""
        today = moment.astimezone(self.zone).date()
        for days in _WINDOW_DAYS:
            day = today + timedelta(days=days)
            opens = self._instant(day, self.start_hour)
            closes_on = day if self.end_hour > self.start_hour else day + timedelta(days=1)
            closes = self._instant(closes_on, self.end_hour)
            if closes <= opens:
                # The spring gap emptied it; see the class docstring.
                closes = opens + self.length
            yield opens, closes

    def _instant(self, day: date, hour: int) -> datetime:
        """``hour`` o'clock on ``day`` in this zone, as UTC, under §4.2's DST rules."""
        wall = datetime(day.year, day.month, day.day, hour)
        # fold=0 is the first occurrence of an hour that occurs twice.
        first = wall.replace(tzinfo=self.zone).astimezone(UTC)
        if first.astimezone(self.zone).replace(tzinfo=None) == wall:
            return first
        # The hour does not exist: it is in the spring gap. PEP 495 maps it
        # with the offset from before the gap (fold=0) and from after it
        # (fold=1); the transition lies between the two. Bisect for it rather
        # than trust fold=0 to land on it, which it does only when the hour is
        # the gap's first.
        earlier = wall.replace(tzinfo=self.zone, fold=1).astimezone(UTC)
        before, after = min(first, earlier), max(first, earlier)
        while after - before > timedelta(seconds=1):
            middle = before + (after - before) / 2
            if middle.astimezone(self.zone).replace(tzinfo=None) >= wall:
                after = middle
            else:
                before = middle
        return after.replace(microsecond=0)


#: Either kind of schedule a warehouse may have.
Schedule = CronSchedule | WindowSchedule

_SCHEDULE_KEYS = frozenset({"window"})
_WINDOW_KEYS = frozenset({"timezone", "start_hour", "end_hour", "min_interval_hours"})


# -- the fleet ----------------------------------------------------------------


@dataclass(frozen=True)
class FleetWarehouse:
    """One warehouse: when to consider it, and how its tables are laid out.

    Which tables are maintained is ``table_config``'s tables, exactly as for a
    ``zamboni maintenance`` run; the fleet file does not keep a second list.
    """

    name: str
    schedule: Schedule
    table_config: TableConfig
    #: The catalog URI, where this warehouse's differs from the profile's. One
    #: Lakekeeper serves many warehouses from one URI, so ``None`` -- use
    #: ``zamboni.yml`` -- is the usual case.
    uri: str | None = None
    #: When a merge-on-read table is compacted between schedules (#175).
    #: ``None`` is off. On by default, as the fleet file's default is: an
    #: unmaintained merge-on-read table degrades every reader.
    pressure: PressureSettings | None = field(default_factory=PressureSettings)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise FleetConfigError("a warehouse needs a non-empty 'name'")
        if not isinstance(self.schedule, CronSchedule | WindowSchedule):
            raise FleetConfigError(
                f"warehouse {self.name!r}: schedule must be a CronSchedule or a "
                f"WindowSchedule; build one with CronSchedule({self.schedule!r})"
            )
        if self.uri is not None and (not isinstance(self.uri, str) or not self.uri):
            raise FleetConfigError(
                f"warehouse {self.name!r}: 'uri' must be a non-empty string, or omitted "
                "to use the profile's"
            )
        # The fleet file refuses credential keys and is never mode-checked, so
        # a password inside the URI would be the one secret in a file meant to
        # be committed (ZMBNI-194). Not echoed: the URI is the secret.
        if self.uri is not None and carries_userinfo(self.uri):
            raise FleetConfigError(
                f"warehouse {self.name!r}: 'uri' has credentials written into it. The "
                "fleet file holds none; put the catalog credential in zamboni.yml or .env"
            )
        if not isinstance(self.table_config, TableConfig):
            raise FleetConfigError(f"warehouse {self.name!r}: table_config must be a TableConfig")
        if self.pressure is not None and not isinstance(self.pressure, PressureSettings):
            raise FleetConfigError(
                f"warehouse {self.name!r}: pressure must be PressureSettings, or None for off"
            )
        try:
            self.table_config.validate()
        except TableConfigError as exc:
            raise FleetConfigError(f"warehouse {self.name!r}: {exc}") from exc
        # The assertion `TableConfig.warehouse` exists to make: a config
        # written for acme, listed under globex, is an error rather than a run
        # against the wrong tenant.
        if self.table_config.warehouse != self.name:
            raise FleetConfigError(
                f"warehouse {self.name!r}: its table config describes warehouse "
                f"{self.table_config.warehouse!r}"
                + (f" ({self.table_config.source})" if self.table_config.source else "")
            )


@dataclass(frozen=True)
class FleetConfig:
    """Every warehouse the service maintains. Validated when constructed."""

    warehouses: tuple[FleetWarehouse, ...]
    version: int = FLEET_VERSION
    #: Every file this config was read from -- the fleet file and each
    #: ``table_config`` it references -- so a reload can watch all of them
    #: rather than only the one that names the others. Empty when built in code.
    sources: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        if self.version != FLEET_VERSION:
            raise FleetConfigError(
                f"unsupported fleet file version {self.version!r}; this build "
                f"understands {FLEET_VERSION}"
            )
        # Coerce a list to a tuple so a fleet built in code is as immutable as
        # one loaded from a file.
        object.__setattr__(self, "warehouses", tuple(self.warehouses))
        # Refused rather than allowed as "nothing to do". The file is generated,
        # and a generator that fails open writes an empty list; accepting that
        # on reload would stop every warehouse's maintenance by being wrong,
        # which is the one thing a reload must never do.
        if not self.warehouses:
            raise FleetConfigError("the fleet declares no warehouses")
        seen: set[str] = set()
        for warehouse in self.warehouses:
            if not isinstance(warehouse, FleetWarehouse):
                raise FleetConfigError(
                    f"warehouses must be FleetWarehouse, found {type(warehouse).__name__}"
                )
            if warehouse.name in seen:
                raise FleetConfigError(f"warehouse {warehouse.name!r} is declared twice")
            seen.add(warehouse.name)

    def __getitem__(self, name: str) -> FleetWarehouse:
        for warehouse in self.warehouses:
            if warehouse.name == name:
                return warehouse
        raise KeyError(name)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(w.name for w in self.warehouses)

    # -- loading ------------------------------------------------------------

    @classmethod
    def load(cls, path: str | Path) -> FleetConfig:
        """Read a fleet file, YAML or JSON.

        Relative ``table_config`` paths resolve against the fleet file's
        directory *as given*, not its symlink target. A Kubernetes ConfigMap
        projects ``fleet.yaml`` as a link into a hidden ``..data`` directory;
        its sibling files are reachable through the mount path, which is the
        path a reader wrote.
        """
        import yaml

        path = Path(path)
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise FleetConfigError(f"{path}: cannot read: {exc}") from None
        except yaml.YAMLError as exc:
            # The one YAML mistake a cron schedule invites: an unquoted value
            # starting with `*` is an alias, not a string.
            # Described, not quoted: a fleet file holds no credentials by
            # contract, but the key refusal that enforces it runs only after a
            # parse, and under `serve` this message also reaches the state file
            # and the error log (ZMBNI-207).
            raise FleetConfigError(
                f"{path}: invalid YAML: {describe_yaml_error(exc)}. "
                "A schedule starting with '*' must be quoted."
            ) from None
        try:
            return cls.from_dict(raw, base=path.parent, source=path)
        except FleetConfigError as exc:
            raise FleetConfigError(f"{path}: {exc}") from exc

    @classmethod
    def from_dict(
        cls, raw: Any, *, base: Path | None = None, source: Path | None = None
    ) -> FleetConfig:
        """Build from the file's shape, for a caller holding it as data.

        ``base`` is where relative ``table_config`` paths resolve; ``None``
        means the working directory.
        """
        _check_block(raw, _ROOT_KEYS, "<root>")
        if "warehouses" not in raw:
            raise FleetConfigError("<root>: 'warehouses' is required")
        entries = raw["warehouses"]
        # A list rather than a mapping keyed by name, because PyYAML keeps the
        # last of two duplicate keys without a word. As a list, a warehouse
        # listed twice is visible and refused.
        if not isinstance(entries, list):
            raise FleetConfigError(
                f"<root>.warehouses: expected a list of warehouses, found {_json_name(entries)}"
            )
        # The fleet-wide default for `random`, which a warehouse may override.
        # On unless switched off -- see CronSchedule for why the default is the
        # worst-case mode -- and switching it off is one line, at DevOps' risk.
        spread = raw.get("random", True)
        if not isinstance(spread, bool):
            raise FleetConfigError(
                f"<root>.random: expected true or false, found {_json_name(spread)}"
            )
        # The fleet-wide pressure settings, which a warehouse overrides key by
        # key or switches off. On unless switched off (#175).
        try:
            pressure = PressureSettings.from_value(raw.get("pressure", True), None)
        except ValueError as exc:
            raise FleetConfigError(f"<root>.{exc}") from None
        version = raw.get("version", FLEET_VERSION)
        if not isinstance(version, int) or isinstance(version, bool):
            raise FleetConfigError(
                f"<root>.version: expected a number, found {_json_name(version)}"
            )

        sources = [source] if source is not None else []
        warehouses = []
        for index, entry in enumerate(entries):
            warehouse, read = _warehouse_from_dict(
                entry, f"warehouses[{index}]", base, spread, pressure
            )
            warehouses.append(warehouse)
            sources.extend(read)
        return cls(warehouses=tuple(warehouses), version=version, sources=tuple(sources))


def _warehouse_from_dict(
    raw: Any,
    where: str,
    base: Path | None,
    spread: bool,
    pressure: PressureSettings | None,
) -> tuple[FleetWarehouse, list[Path]]:
    _check_block(raw, _WAREHOUSE_KEYS, where)
    for key in ("name", "schedule", "table_config"):
        if key not in raw:
            raise FleetConfigError(f"{where}: {key!r} is required")
    name = raw["name"]
    # Checked before the table config is read, which would otherwise report a
    # bad name as a type error in a file the author did not write.
    if not isinstance(name, str) or not name:
        raise FleetConfigError(
            f"{where}.name: expected a non-empty string, found {_json_name(name)}"
        )
    where = f"{where} ({name})"

    read: list[Path] = []
    spec = raw["table_config"]
    try:
        if isinstance(spec, str):
            path = Path(spec)
            if not path.is_absolute():
                path = (base or Path.cwd()) / path
            table_config = TableConfig.load(path)
            read.append(path)
        elif isinstance(spec, dict):
            # Inline: the body of a table-config.json. Its `warehouse` is
            # implied by the entry and, if written anyway, checked against it.
            body = {"warehouse": name, **spec}
            table_config = TableConfig.from_dict(body, source=f"{where}.table_config")
        else:
            raise FleetConfigError(
                f"{where}.table_config: expected a path to a table-config.json or the "
                f"same body inline, found {_json_name(spec)}"
            )
    except (TableConfigError, OSError) as exc:
        raise FleetConfigError(f"{where}.table_config: {exc}") from exc

    if "pressure" in raw:
        try:
            pressure = PressureSettings.from_value(raw["pressure"], pressure)
        except ValueError as exc:
            raise FleetConfigError(f"{where}.{exc}") from None

    try:
        warehouse = FleetWarehouse(
            name=name,
            schedule=_schedule_from(raw["schedule"], raw.get("random", spread), "schedule"),
            table_config=table_config,
            uri=raw.get("uri"),
            pressure=pressure,
        )
    except FleetConfigError as exc:
        raise FleetConfigError(f"{where}: {exc}") from exc
    return warehouse, read


def _schedule_from(raw: Any, spread: Any, where: str) -> Schedule:
    """A cron expression string, or ``{window: {...}}``."""
    if isinstance(raw, str):
        return CronSchedule(raw, random=spread)
    if not isinstance(raw, dict):
        raise FleetConfigError(
            f"{where}: expected a cron expression string or a 'window' block, "
            f"found {_json_name(raw)}"
        )
    _check_block(raw, _SCHEDULE_KEYS, where)
    if "window" not in raw:
        raise FleetConfigError(f"{where}: a schedule block needs 'window'")
    window = raw["window"]
    _check_block(window, _WINDOW_KEYS, f"{where}.window")
    for key in ("timezone", "start_hour", "end_hour"):
        if key not in window:
            raise FleetConfigError(f"{where}.window: {key!r} is required")
    return WindowSchedule(
        timezone=window["timezone"],
        start_hour=window["start_hour"],
        end_hour=window["end_hour"],
        min_interval_hours=window.get("min_interval_hours"),
        random=spread,
    )


def _check_block(raw: Any, allowed: frozenset[str], where: str) -> None:
    """Refuse a non-mapping, and refuse unknown keys rather than ignoring them."""
    if not isinstance(raw, dict):
        raise FleetConfigError(
            f"{where}: expected a block of settings, found {_json_name(raw)}; "
            f"allowed keys are {sorted(allowed)}"
        )
    unknown = sorted(set(raw) - allowed)
    secret = [key for key in unknown if key in SECRET_PROFILE_KEYS]
    if secret:
        # Named specifically: the likely cause is not a typo but a credential
        # put in the one file defined as holding none, which a provisioner
        # would then be writing into a ConfigMap.
        raise FleetConfigError(
            f"{where}: {secret} is a credential, and the fleet file holds none. "
            "Credentials belong in zamboni.yml or .env (see docs/devops.md)."
        )
    if unknown:
        raise FleetConfigError(
            f"{where}: unknown key(s) {unknown}; allowed: {sorted(allowed)}. "
            "Rejected rather than ignored so a typo cannot silently change what is maintained."
        )


def _json_name(value: Any) -> str:
    return {
        type(None): "null",
        bool: "a boolean",
        int: "a number",
        float: "a number",
        str: "a string",
        list: "a list",
        dict: "a block",
    }.get(type(value), type(value).__name__)
