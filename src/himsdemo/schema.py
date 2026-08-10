# SPDX-License-Identifier: Apache-2.0
"""Turn `table_schema.json` into Iceberg tables.

`table_schema.json` is the logical contract -- columns, types, primary key,
replication key. The physical layout it gets written under comes from
`table-config.json` and is applied by `zamboni`. Keeping them apart means an
analyst can change how a table is laid out without touching what it holds.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestampType

from zamboni.tableconfig import TableConfig

#: Only the types this demo's schema uses. Deliberately small: an unmapped type
#: should fail loudly rather than be silently coerced to string.
ICEBERG_TYPES = {"string": StringType, "timestamp": TimestampType}
ARROW_TYPES = {"string": pa.string(), "timestamp": pa.timestamp("us")}

TRANSFORMS = {"identity": IdentityTransform, "day": DayTransform}


@dataclass(frozen=True)
class TableDefinition:
    name: str
    identifier: str
    description: str
    primary_key: tuple[str, ...]
    replication_mode: str
    replication_key: str | None
    columns: tuple[dict, ...]

    @property
    def column_names(self) -> list[str]:
        return [c["name"] for c in self.columns]

    def single_primary_key(self) -> str:
        """The one column rows are merged on.

        Composite keys would need a different merge strategy than upsert's
        single join column, so refuse rather than silently use the first.
        """
        if len(self.primary_key) != 1:
            raise ValueError(
                f"{self.identifier}: ingestion needs exactly one primary key column, "
                f"got {list(self.primary_key)}"
            )
        return self.primary_key[0]

    def require_replication_key(self) -> str:
        if self.replication_mode != "incremental" or not self.replication_key:
            raise ValueError(
                f"{self.identifier}: batched ingestion needs an incremental "
                f"replication key, but replication is {self.replication_mode!r}"
            )
        return self.replication_key

    def iceberg_schema(self) -> Schema:
        return Schema(
            *[
                NestedField(
                    field_id=i,
                    name=c["name"],
                    field_type=ICEBERG_TYPES[c["type"]](),
                    required=bool(c.get("required", False)),
                )
                for i, c in enumerate(self.columns, start=1)
            ]
        )

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field(c["name"], ARROW_TYPES[c["type"]], nullable=not c.get("required", False))
                for c in self.columns
            ]
        )


@dataclass(frozen=True)
class SchemaDocument:
    namespace: str
    tables: tuple[TableDefinition, ...]

    def by_name(self, name: str) -> TableDefinition:
        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(name)

    @classmethod
    def load(cls, path: str | Path) -> SchemaDocument:
        raw = json.loads(Path(path).read_text())
        namespace = raw["namespace"]
        tables = tuple(
            TableDefinition(
                name=name,
                identifier=f"{namespace}.{name}",
                description=block.get("description", ""),
                primary_key=tuple(block.get("primary_key") or ()),
                replication_mode=(block.get("replication") or {}).get("mode", "full_table"),
                replication_key=(block.get("replication") or {}).get("key"),
                columns=tuple(block["columns"]),
            )
            for name, block in raw["tables"].items()
        )
        return cls(namespace=namespace, tables=tables)


def partition_spec_for(definition: TableDefinition, config: TableConfig) -> PartitionSpec:
    """Build the Iceberg spec from the *layout* config, not the schema.

    Field ids start at 1000, which is the Iceberg convention for partition
    fields and keeps them clear of the schema's column ids.
    """
    settings = config.for_table(definition.identifier)
    if not settings.partition:
        return PartitionSpec()

    schema = definition.iceberg_schema()
    fields = []
    for i, pf in enumerate(settings.partition):
        source = schema.find_field(pf.column)
        transform_cls = TRANSFORMS.get(pf.transform)
        if transform_cls is None:
            raise ValueError(
                f"{definition.identifier}: transform {pf.transform!r} is not wired up in the "
                f"demo (supported: {sorted(TRANSFORMS)})"
            )
        fields.append(
            PartitionField(
                source_id=source.field_id,
                field_id=1000 + i,
                transform=transform_cls(),
                name=pf.iceberg_name(),
            )
        )
    return PartitionSpec(*fields)


def create_tables(
    catalog: Catalog, schema: SchemaDocument, config: TableConfig
) -> dict[str, Table]:
    """Create the namespace and every table, if they do not already exist."""
    catalog.create_namespace_if_not_exists(schema.namespace)
    tables: dict[str, Table] = {}
    for definition in schema.tables:
        tables[definition.name] = catalog.create_table_if_not_exists(
            definition.identifier,
            schema=definition.iceberg_schema(),
            partition_spec=partition_spec_for(definition, config),
            properties={"format-version": "2"},
        )
    return tables


def load_tables(catalog: Catalog, schema: SchemaDocument) -> dict[str, Table]:
    """Load the tables that already exist, creating nothing.

    Read-only commands use this. Going through `create_tables` would have
    `status` and `query` quietly bring a catalog and three empty tables into
    existence on a cleared demo -- a mutation from commands documented as
    changing nothing.
    """
    tables: dict[str, Table] = {}
    for definition in schema.tables:
        try:
            tables[definition.name] = catalog.load_table(definition.identifier)
        except NoSuchTableError:
            continue
    return tables


def read_csv(path: Path, definition: TableDefinition) -> pa.Table:
    """Read one of the demo CSVs into Arrow, typed by the schema document.

    Empty strings become nulls: `scheduled_at` is legitimately absent for
    unplanned discharges, and `actor_id` is absent for system-generated events.
    Reading them as empty strings would quietly turn "unknown" into a value.
    """
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        header = set(reader.fieldnames or ())
        rows = list(reader)

    # A column present in the schema but absent from the file would otherwise
    # read as all-null, turning a schema/CSV mismatch into silently empty data.
    missing = [c["name"] for c in definition.columns if c["name"] not in header]
    if missing:
        raise ValueError(f"{path}: missing column(s) {missing} declared in table_schema.json")

    columns: dict[str, list] = {}
    for column in definition.columns:
        name, kind = column["name"], column["type"]
        raw = [row.get(name, "") for row in rows]
        if kind == "timestamp":
            columns[name] = [
                dt.datetime.fromisoformat(v) if v not in ("", None) else None for v in raw
            ]
        else:
            columns[name] = [v if v not in ("", None) else None for v in raw]

    return pa.table(columns, schema=definition.arrow_schema())
