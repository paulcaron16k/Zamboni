#!/usr/bin/env python3
"""Generate the five days of HIMS discharge CSVs the demo ingests.

Run once; the output is committed. Everything is seeded, so regenerating
produces byte-identical files and a reviewer can diff the data like code.

    uv run scripts/generate_healthims_data.py

Three things about the shape of the output are worth knowing before reading the
code:

* **The discharge file is a change-log, not a snapshot.** A discharge appears
  once per status transition, each row carrying the `updated_at` at which that
  version became current. That is what real incremental replication delivers,
  and it is what gives the demo its updates -- without them copy-on-write and
  merge-on-read would look identical.

* **Ingestion batches by the hour of the replication key**, so no batch column
  is needed. Roughly ten micro-batches a day per table reproduces the
  small-file condition the maintenance tool exists to fix.

* **Dates are fixed in the past** (2026-01-05 .. 2026-01-09) rather than
  relative to the run. That keeps the CSVs deterministic, and puts the data
  comfortably past the 90-day partition-evolution threshold so `maintenance`
  demonstrates day-to-month condensation without an artificial setting.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "healthims"

SEED = 20260105
DAY_ONE = dt.date(2026, 1, 5)
DAYS = 5
BEDS = 30
TURNOVER = 0.30
TURNOVER_JITTER = 0.10  # relative, so 30% +/- 10% -> 8..10 discharges a day

#: Days on which a discharge fails billing clearance and is restarted the next
#: day. Never day 4 or 5 -- there would be no following day to restart in.
CANCEL_DAYS = (1, 2, 3)

ROLES = {
    "doctor": 5,
    "nurse-staff": 8,
    "nurse-mgr": 2,
    "pharma-mgr": 2,
    "billing-mgr": 3,
    "transport-staff": 3,
    "evs-staff": 4,
}

FIRST_NAMES = [
    "Amara",
    "Bilal",
    "Chen",
    "Divya",
    "Ewan",
    "Farida",
    "Grigor",
    "Hana",
    "Idris",
    "Jolene",
    "Kwame",
    "Lucia",
    "Mattias",
    "Nadia",
    "Oskar",
    "Priya",
    "Quinn",
    "Rosa",
    "Sami",
    "Tove",
    "Uma",
    "Viktor",
    "Wanda",
    "Xiulan",
    "Yusuf",
    "Zofia",
    "Aleksy",
    "Beatriz",
]
LAST_NAMES = [
    "Abara",
    "Bianchi",
    "Costa",
    "Dahl",
    "Eriksen",
    "Farouk",
    "Gruber",
    "Haddad",
    "Ibrahim",
    "Jansen",
    "Kowalski",
    "Lindqvist",
    "Moreau",
    "Nakamura",
    "Oyelaran",
    "Petrov",
    "Quintero",
    "Rossi",
    "Sandberg",
    "Tanaka",
    "Ueda",
    "Vargas",
    "Weiss",
    "Xu",
    "Yilmaz",
    "Zielinski",
    "Andersen",
    "Berg",
]

#: Happy path, in causal order. (event_name, actor_role, status_after)
#: status_after None means the event does not advance the state machine.
HAPPY_PATH = [
    ("pending_discharge_logged", "nurse-mgr", "pending"),
    ("attending_doctor_initiated", "doctor", "initiated"),
    ("dietary_tray_canceled", "nurse-staff", None),
    ("floor_nurse_clearance", "nurse-staff", "nurse_cleared"),
    ("pharma_return_started", "nurse-staff", None),
    ("pharma_return_received", "pharma-mgr", "pharma_cleared"),
    ("billing_initiated_insurance", "billing-mgr", "billing_pending"),
    ("billing_insurance_received", "billing-mgr", None),
    ("billing_clearance", "billing-mgr", "billing_cleared"),
    ("patient_displaced_discharged", "transport-staff", "discharged"),
]

#: After the room is empty. Actor None means system-generated.
EVS_PATH = [
    ("bed_cleaning_required", None, None),
    ("cleaning_priority_assigned", "evs-staff", None),
    ("bed_cleaning_started", "evs-staff", None),
    ("bed_cleaned", "evs-staff", None),
]

#: Where the failure path diverges: everything up to and including the
#: insurance response happens, then clearance fails instead of succeeding.
FAILURE_PREFIX = HAPPY_PATH[:8]
FAILURE_EVENT = ("billing_clearance_failed", "billing-mgr", "cancelled")

#: Restart the following day: billing is retried from scratch, then the patient
#: leaves and EVS cleans the room.
RESTART_PATH = HAPPY_PATH[6:]


def uuid7(when: dt.datetime, rng: random.Random) -> str:
    """A UUIDv7 for ``when``. Python 3.13's stdlib has no ``uuid.uuid7``.

    Layout per the RFC: 48-bit big-endian Unix millisecond timestamp, version
    nibble, 12 random bits, variant bits, 62 random bits. Being time-ordered is
    not incidental here -- it gives ids real locality under the z-order the
    demo declares.
    """
    ms = int(when.replace(tzinfo=dt.UTC).timestamp() * 1000)
    value = (ms & ((1 << 48) - 1)) << 80
    value |= 0x7 << 76
    value |= rng.getrandbits(12) << 64
    value |= 0b10 << 62
    value |= rng.getrandbits(62)
    return str(uuid.UUID(int=value))


@dataclass
class Employee:
    employee_id: str
    role: str
    full_name: str


@dataclass
class Discharge:
    process_id: str
    patient_id: str
    attending_doctor_id: str
    room_id: str
    scheduled_at: dt.datetime | None
    created_at: dt.datetime
    #: Set when the attending doctor initiates; carried through a next-day
    #: restart. Held here rather than dug back out of `versions` by index --
    #: that broke silently the moment the transition order changed.
    initiated_at: dt.datetime | None = None
    #: (updated_at, status, initiated_at, completed_at) per transition.
    versions: list[tuple[dt.datetime, str, dt.datetime | None, dt.datetime | None]] = field(
        default_factory=list
    )


def build_employees(rng: random.Random) -> list[Employee]:
    employees: list[Employee] = []
    used: set[str] = set()
    n = 0
    for role, count in ROLES.items():
        for _ in range(count):
            n += 1
            while True:
                name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
                if name not in used:
                    used.add(name)
                    break
            employees.append(Employee(f"EMP-{n:03d}", role, name))
    return employees


def by_role(employees: list[Employee], role: str) -> list[Employee]:
    return [e for e in employees if e.role == role]


def at(day: dt.date, hour: int, minute: int = 0, second: int = 0) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(hour, minute, second))


def random_between(rng: random.Random, start: dt.datetime, end: dt.datetime) -> dt.datetime:
    span = int((end - start).total_seconds())
    return start + dt.timedelta(seconds=rng.randrange(max(span, 1)))


def event_payload(name: str, discharge: Discharge, rng: random.Random) -> dict:
    """Event-specific properties, as the JSON text the `data` column holds."""
    if name == "attending_doctor_initiated":
        return {"order_id": f"ORD-{rng.randrange(10**6):06d}", "clinically_stable": True}
    if name == "floor_nurse_clearance":
        return {"home_equipment_ready": rng.random() > 0.25, "instructions_ack": True}
    if name == "pharma_return_started":
        return {"items_pulled": rng.randint(1, 6)}
    if name == "pharma_return_received":
        return {"items_accepted": rng.randint(1, 6), "inventory_adjusted": True}
    if name == "billing_initiated_insurance":
        return {"claim_id": f"CLM-{rng.randrange(10**7):07d}", "line_items": rng.randint(4, 22)}
    if name == "billing_insurance_received":
        return {"adjudication": "approved", "covered_pct": rng.choice([70, 80, 90, 100])}
    if name == "billing_clearance":
        return {"patient_responsibility": round(rng.uniform(0, 850), 2)}
    if name == "billing_clearance_failed":
        return {
            "reason": rng.choice(["insurance_rejected", "prior_auth_missing", "coverage_lapsed"]),
            "retry_scheduled": True,
        }
    if name == "dietary_tray_canceled":
        return {"meals_cancelled": rng.randint(1, 3)}
    if name == "pending_discharge_logged":
        return {"bed_board": "predicted_discharge", "room": discharge.room_id}
    if name == "patient_displaced_discharged":
        return {"room": discharge.room_id, "transport": rng.choice(["wheelchair", "walking"])}
    if name == "bed_cleaning_required":
        return {"room": discharge.room_id, "room_status": "dirty"}
    if name == "cleaning_priority_assigned":
        return {"room": discharge.room_id, "priority": rng.choice(["STAT", "NEXT", "NORMAL"])}
    if name == "bed_cleaning_started":
        return {"room": discharge.room_id, "room_status": "in_progress"}
    if name == "bed_cleaned":
        return {"room": discharge.room_id, "room_status": "clean_available"}
    return {}


class Generator:
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.employees = build_employees(rng)
        self.doctors = by_role(self.employees, "doctor")
        self.patient_seq = 0
        #: Discharges cancelled on a day, keyed by the day they restart on.
        self.pending_restart: dict[int, list[Discharge]] = {}
        self.rows: dict[int, dict[str, list[dict]]] = {
            d: {"discharges": [], "events": []} for d in range(1, DAYS + 1)
        }

    # -- helpers ---------------------------------------------------------

    def actor(self, role: str | None) -> str | None:
        return None if role is None else self.rng.choice(by_role(self.employees, role)).employee_id

    def next_patient(self) -> str:
        self.patient_seq += 1
        return f"PAT-{self.patient_seq:04d}"

    def emit_event(
        self, day_no: int, when: dt.datetime, name: str, role: str | None, discharge: Discharge
    ) -> None:
        self.rows[day_no]["events"].append(
            {
                "event_id": uuid7(when, self.rng),
                "occurred_at": when.isoformat(sep=" "),
                "event_name": name,
                "actor_id": self.actor(role) or "",
                "process_type": "discharge",
                "process_id": discharge.process_id,
                "data": json.dumps(event_payload(name, discharge, self.rng), sort_keys=True),
            }
        )

    def emit_version(
        self,
        day_no: int,
        discharge: Discharge,
        when: dt.datetime,
        status: str,
        initiated_at: dt.datetime | None,
        completed_at: dt.datetime | None,
    ) -> None:
        discharge.versions.append((when, status, initiated_at, completed_at))
        self.rows[day_no]["discharges"].append(
            {
                "process_type": "discharge",
                "process_id": discharge.process_id,
                "status": status,
                "patient_id": discharge.patient_id,
                "attending_doctor_id": discharge.attending_doctor_id,
                "room_id": discharge.room_id,
                "scheduled_at": discharge.scheduled_at.isoformat(sep=" ")
                if discharge.scheduled_at
                else "",
                "created_at": discharge.created_at.isoformat(sep=" "),
                "updated_at": when.isoformat(sep=" "),
                "initiated_at": initiated_at.isoformat(sep=" ") if initiated_at else "",
                "completed_at": completed_at.isoformat(sep=" ") if completed_at else "",
            }
        )

    # -- the day ---------------------------------------------------------

    def run(self) -> None:
        for day_no in range(1, DAYS + 1):
            self.generate_day(day_no)

    def generate_day(self, day_no: int) -> None:
        day = DAY_ONE + dt.timedelta(days=day_no - 1)

        for discharge in self.pending_restart.pop(day_no, []):
            self.restart(day_no, day, discharge)

        target = BEDS * TURNOVER
        count = round(
            self.rng.uniform(target * (1 - TURNOVER_JITTER), target * (1 + TURNOVER_JITTER))
        )
        cancel_index = self.rng.randrange(count) if day_no in CANCEL_DAYS else None

        for i in range(count):
            self.new_discharge(day_no, day, cancelled=(i == cancel_index))

    def new_discharge(self, day_no: int, day: dt.date, *, cancelled: bool) -> None:
        rng = self.rng
        logged_at = random_between(rng, at(day, 8), at(day, 9))
        initiated_at = random_between(rng, at(day, 9), at(day, 13))
        completed_at = random_between(rng, at(day, 15), at(day, 18))

        discharge = Discharge(
            process_id=uuid7(logged_at, rng),
            patient_id=self.next_patient(),
            attending_doctor_id=rng.choice(self.doctors).employee_id,
            room_id=f"R-{rng.randint(1, BEDS):02d}",
            # Unplanned discharges have no scheduled departure.
            scheduled_at=(
                None if rng.random() < 0.2 else random_between(rng, at(day, 14), at(day, 19))
            ),
            created_at=logged_at,
            initiated_at=initiated_at,
        )

        # Everything up to but not including the departure/failure, which is
        # emitted afterwards at its own time.
        body = FAILURE_PREFIX if cancelled else HAPPY_PATH[:-1]
        # The middle of the process is scattered between initiation and
        # departure, but must stay in causal order -- so sample and sort.
        middle = sorted(
            random_between(rng, initiated_at, completed_at) for _ in range(len(body) - 2)
        )
        times = [logged_at, initiated_at, *middle]

        for (name, role, status), when in zip(body, times, strict=True):
            self.emit_event(day_no, when, name, role, discharge)
            if status:
                self.emit_version(
                    day_no,
                    discharge,
                    when,
                    status,
                    initiated_at if name != "pending_discharge_logged" else None,
                    None,
                )

        if cancelled:
            failed_at = random_between(rng, times[-1], completed_at)
            self.emit_event(day_no, failed_at, FAILURE_EVENT[0], FAILURE_EVENT[1], discharge)
            self.emit_version(day_no, discharge, failed_at, "cancelled", initiated_at, None)
            self.pending_restart.setdefault(day_no + 1, []).append(discharge)
            return

        self.emit_event(day_no, completed_at, *HAPPY_PATH[-1][:2], discharge)
        self.emit_version(day_no, discharge, completed_at, "discharged", initiated_at, completed_at)
        self.emit_evs(day_no, discharge, completed_at)

    def restart(self, day_no: int, day: dt.date, discharge: Discharge) -> None:
        """Rerun billing and departure for a discharge cancelled yesterday.

        Same ``process_id``: this updates an existing row rather than inserting
        a new one, which is the only reason copy-on-write and merge-on-read
        behave differently in this demo.
        """
        rng = self.rng
        initiated_at = discharge.initiated_at
        retry_from = random_between(rng, at(day, 9), at(day, 12))
        completed_at = random_between(rng, at(day, 15), at(day, 18))
        body = RESTART_PATH[:-1]
        middle = sorted(random_between(rng, retry_from, completed_at) for _ in range(len(body) - 1))
        times = [retry_from, *middle]

        for (name, role, status), when in zip(body, times, strict=True):
            self.emit_event(day_no, when, name, role, discharge)
            if status:
                self.emit_version(day_no, discharge, when, status, initiated_at, None)

        self.emit_event(day_no, completed_at, *HAPPY_PATH[-1][:2], discharge)
        self.emit_version(day_no, discharge, completed_at, "discharged", initiated_at, completed_at)
        self.emit_evs(day_no, discharge, completed_at)

    def emit_evs(self, day_no: int, discharge: Discharge, completed_at: dt.datetime) -> None:
        """EVS events, strictly after the room is empty.

        The discharge row is already terminal at `discharged` and does not
        change again -- these events reference it without touching it.
        """
        when = completed_at
        for name, role, _ in EVS_PATH:
            when = when + dt.timedelta(minutes=self.rng.randint(3, 40))
            self.emit_event(day_no, when, name, role, discharge)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


DISCHARGE_FIELDS = [
    "process_type",
    "process_id",
    "status",
    "patient_id",
    "attending_doctor_id",
    "room_id",
    "scheduled_at",
    "created_at",
    "updated_at",
    "initiated_at",
    "completed_at",
]
EVENT_FIELDS = [
    "event_id",
    "occurred_at",
    "event_name",
    "actor_id",
    "process_type",
    "process_id",
    "data",
]
EMPLOYEE_FIELDS = ["employee_id", "role", "full_name"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()

    rng = random.Random(SEED)
    gen = Generator(rng)
    gen.run()

    if gen.pending_restart:
        raise SystemExit(
            f"cancelled discharges left unrestarted past day {DAYS}: {gen.pending_restart}"
        )

    for day_no in range(1, DAYS + 1):
        day_dir = args.out / f"day{day_no}"
        # The staff roster is a full snapshot every day; one nurse is promoted
        # on day 3 so the daily replace is not a no-op.
        roster = [
            Employee(
                e.employee_id,
                "nurse-mgr" if (day_no >= 3 and e.employee_id == "EMP-006") else e.role,
                e.full_name,
            )
            for e in gen.employees
        ]
        write_csv(
            day_dir / "employees.csv",
            [
                {"employee_id": e.employee_id, "role": e.role, "full_name": e.full_name}
                for e in roster
            ],
            EMPLOYEE_FIELDS,
        )
        discharges = sorted(gen.rows[day_no]["discharges"], key=lambda r: r["updated_at"])
        events = sorted(gen.rows[day_no]["events"], key=lambda r: r["occurred_at"])
        write_csv(day_dir / "discharges.csv", discharges, DISCHARGE_FIELDS)
        write_csv(day_dir / "events.csv", events, EVENT_FIELDS)

        distinct = len({r["process_id"] for r in discharges})
        print(
            f"day{day_no}: {distinct:>3} discharges  "
            f"{len(discharges):>3} row versions  {len(events):>4} events"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
