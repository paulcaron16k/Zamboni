# SPDX-License-Identifier: Apache-2.0
"""Generate ``table-config.json`` from a Meltano / Singer catalog.

Analysts already describe streams in the catalog, so layout intent belongs next
to the stream it describes rather than in a second file kept in sync by hand.
This module reads that intent and *generates* the config the compactor consumes.

Why generate rather than read the catalog at compaction time
------------------------------------------------------------

Because the catalog is a reliable authoring surface but an unreliable transport.

* **Meltano core** manipulates the catalog as raw dicts -- its metadata and
  schema rules assign into ``stream["metadata"]`` directly -- so arbitrary
  ``x-iceberg`` keys authored in ``meltano.yml`` survive intact.
* **The Singer SDK does not.** ``singer_sdk.singerlib.Metadata`` is a dataclass
  whose ``from_dict``/``to_dict`` only round-trip its own declared fields, and
  ``Schema`` likewise. Verified against the SDK checkout: a catalog carrying
  ``x-iceberg-partition`` in the schema and ``x-iceberg-sort-order`` in stream
  metadata comes back with both silently dropped. Any SDK-based tap that reads
  ``--catalog`` and re-emits it loses them.

So the catalog is where humans write it, and ``table-config.json`` is the
artifact that survives. Generation is a pipeline step with a reviewable diff,
which also means a dropped property shows up as a config change rather than as
a table quietly reverting to an unpartitioned layout.

Authoring shape
---------------

One ``x-iceberg`` object per stream, holding exactly a ``table-config`` table
block. Same schema in both places, so there is one thing to learn::

    metadata:
      events:
        x-iceberg:
          partition:
            - {column: occurred_at, transform: day}
          ordering:
            mode: zorder
            zorder: {columns: [customer_id, product_id]}

It is read from stream metadata (breadcrumb ``[]``) or, failing that, from the
stream's JSON Schema root -- the SDK declares ``x-`` schema keywords like
``x-sql-datatype``, so that is a familiar place for authors to reach for.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .tableconfig import (
    DEFAULT_SETTINGS,
    TableConfig,
    TableConfigError,
    TableSettings,
    _settings_from_dict,
)

#: The single key an author sets, in stream metadata or the schema root.
EXTENSION_KEY = "x-iceberg"


@dataclass(frozen=True)
class ImportedStream:
    stream_id: str
    identifier: str
    settings: TableSettings
    origin: str


@dataclass
class ImportReport:
    """What the import found, and what it deliberately did not."""

    imported: list[ImportedStream]
    skipped: list[tuple[str, str]]

    def describe(self) -> str:
        lines = [f"imported {len(self.imported)} stream(s)"]
        lines += [f"  {s.stream_id} -> {s.identifier}  (from {s.origin})" for s in self.imported]
        # Streams without layout intent are normal, but silence about them would
        # hide a misspelled extension key.
        lines += [f"  skipped {stream}: {reason}" for stream, reason in self.skipped]
        return "\n".join(lines)


def load_catalog(path: str | Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text())
    except json.JSONDecodeError as exc:
        raise TableConfigError(f"{path}: invalid catalog JSON: {exc}") from None


def config_from_catalog(
    catalog: dict[str, Any],
    *,
    namespace: str | None = None,
    defaults: TableSettings | None = None,
    source: str | None = None,
) -> tuple[TableConfig, ImportReport]:
    """Build a :class:`TableConfig` from a Singer catalog.

    Args:
        namespace: Iceberg namespace for streams whose catalog entry does not
            name one. Streams still unresolved after that are skipped rather
            than guessed at.
        defaults: Fleet-wide defaults for the generated config. ``None`` uses
            the package default, which enables days-to-months evolution.
    """
    streams = catalog.get("streams")
    if not isinstance(streams, list):
        raise TableConfigError("catalog has no 'streams' list")

    imported: list[ImportedStream] = []
    skipped: list[tuple[str, str]] = []

    for entry in streams:
        stream_id = entry.get("tap_stream_id") or entry.get("stream") or "<unnamed>"
        raw, origin = _extension_for(entry)
        if raw is None:
            skipped.append((stream_id, f"no {EXTENSION_KEY!r} block"))
            continue
        if not isinstance(raw, dict):
            raise TableConfigError(f"{stream_id}: {EXTENSION_KEY!r} must be an object")

        raw = dict(raw)
        table_override = raw.pop("table", None)
        identifier = _identifier_for(entry, table_override, namespace)
        if identifier is None:
            reason = (
                "could not resolve a 'namespace.table' identifier; set "
                f"{EXTENSION_KEY}.table or pass a namespace"
            )
            skipped.append((stream_id, reason))
            continue

        settings = _settings_from_dict(raw, f"{EXTENSION_KEY}({stream_id})")
        settings.validate(f"{EXTENSION_KEY}({stream_id})")
        imported.append(ImportedStream(stream_id, identifier, settings, origin))

    config = TableConfig(
        defaults=defaults if defaults is not None else DEFAULT_SETTINGS,
        tables={s.identifier: s.settings for s in imported},
        source=source,
    )
    config.validate()
    return config, ImportReport(imported=imported, skipped=skipped)


def _extension_for(entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """Find the extension block: stream metadata first, then the schema root."""
    for md in entry.get("metadata") or ():
        if tuple(md.get("breadcrumb") or ()) == () and EXTENSION_KEY in (md.get("metadata") or {}):
            return md["metadata"][EXTENSION_KEY], "stream metadata"

    schema = entry.get("schema") or {}
    if EXTENSION_KEY in schema:
        return schema[EXTENSION_KEY], "schema"

    return None, ""


def _identifier_for(
    entry: dict[str, Any], table_override: str | None, namespace: str | None
) -> str | None:
    """Resolve the Iceberg ``namespace.table`` this stream lands in."""
    if table_override:
        if "." in table_override:
            return table_override
        if namespace:
            return f"{namespace}.{table_override}"
        return None

    name = entry.get("stream") or entry.get("tap_stream_id")
    if not name:
        return None

    # A Singer stream id is conventionally "<schema>-<table>"; the SDK also
    # carries the source schema as stream metadata 'schema-name'. Prefer an
    # explicit namespace argument over either, because the destination namespace
    # is a property of the warehouse, not of the source system.
    if namespace:
        return f"{namespace}.{_leaf_name(entry, name)}"

    schema_name = None
    for md in entry.get("metadata") or ():
        if tuple(md.get("breadcrumb") or ()) == ():
            schema_name = (md.get("metadata") or {}).get("schema-name")
            break
    if schema_name:
        return f"{schema_name}.{_leaf_name(entry, name)}"

    return name if "." in name else None


def _leaf_name(entry: dict[str, Any], name: str) -> str:
    if entry.get("table"):
        return str(entry["table"])
    # "public-events" -> "events"; a name with no dash is already the leaf.
    return name.split("-", 1)[1] if "-" in name else name
