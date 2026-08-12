# SPDX-License-Identifier: Apache-2.0
"""Demo state: which day we are on, and which write mode is in play.

Kept in a shell-readable `demo.env` so a developer can `cat` it, and so the
demo has no hidden state beyond the Iceberg tables themselves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

TOTAL_DAYS = 5
MODES = ("cow", "mor")


@dataclass
class DemoState:
    #: Where the demo *writes*: the catalog, the warehouse, `demo.env`, spill
    #: files. Must be writable, and is not where the inputs come from.
    root: Path
    #: Where the demo *reads* its five days of CSV and its two config files.
    #: Separate from `root` because an installed copy has them inside the
    #: package, which is read-only -- and because a demo that writes into
    #: site-packages is one that cannot be run twice, or by two users.
    #: Defaults to `root` so a checkout, where the two genuinely are the same
    #: directory, needs no ceremony.
    inputs: Path | None = None
    write_mode: str = "cow"
    days_ingested: int = 0
    #: Set while a day is mid-ingest. Each hourly batch commits on its own, so
    #: a crash leaves the tables partly loaded while the day counter has not
    #: advanced -- rerunning would replay the day and inflate the very file and
    #: delete-file counts the demo presents as evidence.
    ingesting_day: int | None = None
    #: Problems found parsing demo.env, surfaced rather than swallowed.
    warnings: list[str] = field(default_factory=list)

    @property
    def env_path(self) -> Path:
        return self.root / "demo.env"

    @property
    def catalog_path(self) -> Path:
        return self.root / "iceberg_catalog.db"

    @property
    def warehouse_path(self) -> Path:
        return self.root / "iceberg_warehouse"

    @property
    def spill_path(self) -> Path:
        """Where DuckDB spills sorts during maintenance."""
        return self.root / ".spill"

    @property
    def input_root(self) -> Path:
        return self.inputs if self.inputs is not None else self.root

    @property
    def schema_path(self) -> Path:
        return self.input_root / "table_schema.json"

    @property
    def table_config_path(self) -> Path:
        return self.input_root / "table-config.json"

    def day_dir(self, day_no: int) -> Path:
        return self.input_root / f"day{day_no}"

    @property
    def has_more_days(self) -> bool:
        return self.days_ingested < TOTAL_DAYS

    # -- persistence -----------------------------------------------------

    @classmethod
    def load(cls, root: Path, inputs: Path | None = None) -> DemoState:
        state = cls(root=root, inputs=inputs)
        if not state.env_path.exists():
            return state
        for line in state.env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key == "WRITE_MODE":
                if value in MODES:
                    state.write_mode = value
                else:
                    state.warnings.append(
                        f"demo.env: WRITE_MODE={value!r} is not one of {list(MODES)}; "
                        f"using {state.write_mode!r}"
                    )
            elif key == "DAYS_INGESTED":
                if value.isdigit() and int(value) <= TOTAL_DAYS:
                    state.days_ingested = int(value)
                else:
                    state.warnings.append(
                        f"demo.env: DAYS_INGESTED={value!r} is not 0..{TOTAL_DAYS}; using 0"
                    )
            elif key == "INGESTING_DAY" and value.isdigit():
                state.ingesting_day = int(value)
        return state

    def save(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        marker = f"INGESTING_DAY={self.ingesting_day}\n" if self.ingesting_day else ""
        self.env_path.write_text(
            "# zamboni HIMS demo state. Safe to read; edit via ./bin/zamboni-demo.\n"
            f"WRITE_MODE={self.write_mode}\n"
            f"DAYS_INGESTED={self.days_ingested}\n" + marker
        )

    def reset_counters(self) -> None:
        """Back to day 0, keeping the chosen write mode.

        Removing the *tables* is :mod:`himsdemo.catalogs`' job, because how you
        do that depends on the catalog: locally it is a directory and a file,
        remotely it is a series of drop-table calls. This method owns only the
        part that is the same either way.
        """
        self.days_ingested = 0
        self.ingesting_day = None
        self.save()
