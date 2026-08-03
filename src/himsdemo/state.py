"""Demo state: which day we are on, and which write mode is in play.

Kept in a shell-readable `demo.env` so a developer can `cat` it, and so the
demo has no hidden state beyond the Iceberg tables themselves.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

TOTAL_DAYS = 5
MODES = ("cow", "mor")


@dataclass
class DemoState:
    root: Path
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
    def schema_path(self) -> Path:
        return self.root / "table_schema.json"

    @property
    def table_config_path(self) -> Path:
        return self.root / "table-config.json"

    def day_dir(self, day_no: int) -> Path:
        return self.root / f"day{day_no}"

    @property
    def has_more_days(self) -> bool:
        return self.days_ingested < TOTAL_DAYS

    # -- persistence -----------------------------------------------------

    @classmethod
    def load(cls, root: Path) -> DemoState:
        state = cls(root=root)
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
            "# zamboni HIMS demo state. Safe to read; edit via ./bin/demo.\n"
            f"WRITE_MODE={self.write_mode}\n"
            f"DAYS_INGESTED={self.days_ingested}\n" + marker
        )

    def clear(self) -> None:
        """Drop everything the demo created, keeping the chosen write mode.

        The source CSVs and the two config files are inputs and are never
        touched -- only the catalog, the warehouse, and the day counter.
        """
        if self.warehouse_path.exists():
            shutil.rmtree(self.warehouse_path)
        if self.spill_path.exists():
            shutil.rmtree(self.spill_path)
        self.catalog_path.unlink(missing_ok=True)
        # SQLite side files, if the process died mid-write.
        for suffix in ("-wal", "-shm", "-journal"):
            Path(str(self.catalog_path) + suffix).unlink(missing_ok=True)
        self.days_ingested = 0
        self.ingesting_day = None
        self.save()
