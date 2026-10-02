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
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .settings import SECRET_PROFILE_KEYS
from .tableconfig import TableConfig, TableConfigError

#: The fleet file's own format version, independent of table-config's.
FLEET_VERSION = 1

_ROOT_KEYS = frozenset({"version", "warehouses"})
_WAREHOUSE_KEYS = frozenset({"name", "uri", "schedule", "table_config"})


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
    """

    expression: str
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


# -- the fleet ----------------------------------------------------------------


@dataclass(frozen=True)
class FleetWarehouse:
    """One warehouse: when to consider it, and how its tables are laid out.

    Which tables are maintained is ``table_config``'s tables, exactly as for a
    ``zamboni maintenance`` run; the fleet file does not keep a second list.
    """

    name: str
    schedule: CronSchedule
    table_config: TableConfig
    #: The catalog URI, where this warehouse's differs from the profile's. One
    #: Lakekeeper serves many warehouses from one URI, so ``None`` -- use
    #: ``zamboni.yml`` -- is the usual case.
    uri: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise FleetConfigError("a warehouse needs a non-empty 'name'")
        if not isinstance(self.schedule, CronSchedule):
            raise FleetConfigError(
                f"warehouse {self.name!r}: schedule must be a CronSchedule; "
                f"build one with CronSchedule({self.schedule!r})"
            )
        if self.uri is not None and (not isinstance(self.uri, str) or not self.uri):
            raise FleetConfigError(
                f"warehouse {self.name!r}: 'uri' must be a non-empty string, or omitted "
                "to use the profile's"
            )
        if not isinstance(self.table_config, TableConfig):
            raise FleetConfigError(f"warehouse {self.name!r}: table_config must be a TableConfig")
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
            raw = yaml.safe_load(path.read_text())
        except OSError as exc:
            raise FleetConfigError(f"{path}: cannot read: {exc}") from None
        except yaml.YAMLError as exc:
            # The one YAML mistake a cron schedule invites: an unquoted value
            # starting with `*` is an alias, not a string.
            raise FleetConfigError(
                f"{path}: invalid YAML: {exc}. A schedule starting with '*' must be quoted."
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
        version = raw.get("version", FLEET_VERSION)
        if not isinstance(version, int) or isinstance(version, bool):
            raise FleetConfigError(
                f"<root>.version: expected a number, found {_json_name(version)}"
            )

        sources = [source] if source is not None else []
        warehouses = []
        for index, entry in enumerate(entries):
            warehouse, read = _warehouse_from_dict(entry, f"warehouses[{index}]", base)
            warehouses.append(warehouse)
            sources.extend(read)
        return cls(warehouses=tuple(warehouses), version=version, sources=tuple(sources))


def _warehouse_from_dict(
    raw: Any, where: str, base: Path | None
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

    try:
        warehouse = FleetWarehouse(
            name=name,
            schedule=CronSchedule(raw["schedule"]),
            table_config=table_config,
            uri=raw.get("uri"),
        )
    except FleetConfigError as exc:
        raise FleetConfigError(f"{where}: {exc}") from exc
    return warehouse, read


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
