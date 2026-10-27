# SPDX-License-Identifier: Apache-2.0
"""The `write.completed` contract: what a writer says when it has finished with a table.

docs/write-completed.md is the contract as a producer reads it;
docs/maintenance-runtime.md §5 is why it is shaped this way. This module is the
executable form of both: the payload is a dataclass, the JSON Schema served by
:func:`get_write_completed_spec` is generated from it, and :func:`parse_event`
enforces it -- so the three cannot drift apart (the rule `tableconfig_schema`
established).

**Zamboni defines the contract** because it is the one consumer of many possible
producers. A producer publishes the schema of what *it* emits, and a spec-diff
test in the producer's repository compares the two.

**The event is a hint; the table is the truth.** Every field is derivable from
table metadata. A lost event costs latency, a duplicate costs one metadata load,
and neither can cause work the schedule would not have done. So the parser is
strict about the fields that *identify* a table and is never the reason a
maintenance decision is made: the due-check makes that, against the table.

**Versioning.** :data:`CONTRACT_VERSION` is ``major.minor``; a minor increment
only *adds* optional fields. An event's CloudEvents ``dataschema`` names the
version it was written against. A consumer accepts any minor of its major --
and therefore **ignores payload fields it does not know**, which is what makes a
minor increment safe to deploy producer-first. A different major is refused.

This module imports nothing from the engine and nothing from a transport. The
NATS source lives in ``zamboni.events.nats`` (the ``zamboni[nats]`` extra).
"""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, get_args, get_origin

#: The contract's version, ``major.minor``. A minor increment only adds optional
#: fields; anything else is a new major.
CONTRACT_VERSION = "1.0"

#: The CloudEvents ``type``. camelCase, as Lakekeeper names its own events
#: (``updateTable``, from ``CloudEventsPublisher::transaction_committed`` in
#: v0.13.1; tests/fixtures holds a captured one), so one subscriber stack reads
#: both -- and distinct from all of them, so ``parse_event`` refuses theirs.
EVENT_TYPE = "writeCompleted"

#: The ``dataschema`` a producer sets: this URI plus ``/<major>.<minor>``.
DATASCHEMA_BASE = "https://github.com/paulcaron16k/Zamboni/blob/main/docs/write-completed.md"

SPEC_PATH = Path(__file__).resolve().parent.parent / "zamboni-write-completed_spec.json"


class EventError(ValueError):
    """An event that is not a usable `write.completed`. Never retried."""


def _describe(text: str) -> dict[str, Any]:
    return {"description": text}


@dataclass(frozen=True)
class WriteCompleted:
    """The ``data`` of a `write.completed` event.

    The first five fields are required and identify the table and the write;
    everything after them is optional and describes the write. Only the
    identifying fields are ever used to decide anything.
    """

    warehouse: str = field(
        metadata=_describe(
            "The warehouse's name, as the catalog knows it. The matching key: a "
            "consumer maps the event to a table by this name, exactly."
        )
    )
    namespace: tuple[str, ...] = field(
        metadata=_describe(
            "The namespace as a list of levels, outermost first -- never a dotted "
            'string, because a dot inside a level is legal and "a.b" is ambiguous.'
        )
    )
    table: str = field(metadata=_describe("The table's name within the namespace."))
    snapshot_id: int = field(
        metadata=_describe("The last snapshot this writer committed to the table in this run.")
    )
    completed_at: str = field(
        metadata=_describe(
            "When the writer finished with the table: the end of the write window, "
            "ISO-8601 with a UTC offset."
        )
    )
    started_at: str | None = field(
        default=None,
        metadata=_describe("When the writer's first commit to the table began, ISO-8601 UTC."),
    )
    writer: str | None = field(
        default=None,
        metadata=_describe('The producer and its version, e.g. "target-iceberg/0.3.0".'),
    )
    load_method: str | None = field(
        default=None,
        metadata=_describe('How the writer loaded: "append-only", "upsert" or "overwrite".'),
    )
    full_refresh: bool | None = field(
        default=None,
        metadata=_describe("True when the table's contents were replaced wholesale."),
    )
    rows: int | None = field(
        default=None, metadata=_describe("Rows the writer committed to the table in this run.")
    )
    data_files_added: int | None = field(
        default=None, metadata=_describe("Data files the run added.")
    )
    bytes_added: int | None = field(
        default=None, metadata=_describe("Bytes of data files the run added.")
    )
    partitions_changed: int | None = field(
        default=None,
        metadata=_describe(
            "Partitions the run changed. Omitted rather than estimated when not "
            "exact: summing per-snapshot counts over-counts."
        ),
    )
    snapshot_count: int | None = field(
        default=None, metadata=_describe("Snapshots the run committed to the table.")
    )
    streams: tuple[str, ...] | None = field(
        default=None, metadata=_describe("The source streams that wrote the table.")
    )
    table_uuid: str | None = field(
        default=None,
        metadata=_describe(
            "The table's `table-uuid` from its metadata (required from format v2). "
            "Informational: tells a consumer the table was recreated since the write."
        ),
    )
    warehouse_id: str | None = field(
        default=None,
        metadata=_describe(
            "The catalog's own identifier for the warehouse, where it has one. "
            "Informational only: not part of the Iceberg REST specification."
        ),
    )
    catalog_uri: str | None = field(
        default=None,
        metadata=_describe(
            "The catalog URI as the writer reached it. Informational only: the same "
            "catalog is reached by different URLs from different places."
        ),
    )

    @property
    def identifier(self) -> str:
        """``namespace.table``, the form `table-config.json` keys tables by."""
        return ".".join((*self.namespace, self.table))

    @property
    def expected_id(self) -> str:
        """The deterministic CloudEvents ``id`` a producer should set."""
        return f"{self.warehouse}/{'/'.join(self.namespace)}/{self.table}@{self.snapshot_id}"

    @property
    def completed(self) -> datetime:
        return _timestamp(self.completed_at, "data.completed_at")


@dataclass(frozen=True)
class Event:
    """A parsed CloudEvents envelope carrying a :class:`WriteCompleted`."""

    id: str
    source: str
    data: WriteCompleted
    dataschema: str | None = None


#: The CloudEvents attributes the envelope requires.
_ENVELOPE_REQUIRED = ("specversion", "type", "source", "id", "data")


# -- parsing ----------------------------------------------------------------------


def parse_event(payload: bytes | str | dict[str, Any]) -> Event:
    """Parse and validate one structured-mode CloudEvents `write.completed`.

    Strict about what identifies the table and the envelope; tolerant of payload
    fields it does not know, so a newer minor version of the contract still
    parses. Raises :class:`EventError`, naming the field, on anything else.
    """
    if isinstance(payload, bytes | str):
        try:
            doc = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise EventError(f"not JSON: {exc}") from None
    else:
        doc = payload
    if not isinstance(doc, dict):
        raise EventError(f"an event is a JSON object, not {type(doc).__name__}")

    missing = [key for key in _ENVELOPE_REQUIRED if key not in doc]
    if missing:
        raise EventError(f"missing CloudEvents attribute(s) {missing}")
    if doc["specversion"] != "1.0":
        raise EventError(f"specversion {doc['specversion']!r}: only CloudEvents 1.0 is read")
    if doc["type"] != EVENT_TYPE:
        raise EventError(f"type {doc['type']!r} is not {EVENT_TYPE!r}")
    for key in ("id", "source"):
        if not isinstance(doc[key], str) or not doc[key]:
            raise EventError(f"{key}: expected a non-empty string")

    dataschema = doc.get("dataschema")
    if dataschema is not None:
        _check_version(dataschema)

    data = doc["data"]
    if not isinstance(data, dict):
        raise EventError("data: expected a JSON object")
    return Event(
        id=doc["id"],
        source=doc["source"],
        data=_payload(data),
        dataschema=dataschema,
    )


def _check_version(dataschema: Any) -> None:
    if not isinstance(dataschema, str) or "/" not in dataschema:
        raise EventError(f"dataschema {dataschema!r}: expected '<uri>/<major>.<minor>'")
    version = dataschema.rsplit("/", 1)[1]
    major, _, minor = version.partition(".")
    if not (major.isdigit() and minor.isdigit()):
        raise EventError(f"dataschema {dataschema!r}: no '<major>.<minor>' at the end")
    ours = CONTRACT_VERSION.split(".", 1)[0]
    if major != ours:
        raise EventError(
            f"dataschema names contract {version}; this consumer reads {ours}.x. "
            "A new major is not compatible by definition."
        )


def _payload(data: dict[str, Any]) -> WriteCompleted:
    hints = typing.get_type_hints(WriteCompleted)
    values: dict[str, Any] = {}
    for f in dataclasses.fields(WriteCompleted):
        required = f.default is dataclasses.MISSING
        if f.name not in data or data[f.name] is None:
            if required:
                raise EventError(f"data.{f.name} is required")
            continue
        values[f.name] = _coerce(data[f.name], hints[f.name], f"data.{f.name}")
    # Unknown keys are ignored on purpose: a minor version only adds fields.
    if not values["namespace"]:
        raise EventError("data.namespace: at least one level is required")
    for key in ("warehouse", "table"):
        if not values[key]:
            raise EventError(f"data.{key}: must not be empty")
    _timestamp(values["completed_at"], "data.completed_at")
    if "started_at" in values:
        _timestamp(values["started_at"], "data.started_at")
    return WriteCompleted(**values)


def _coerce(value: Any, annotation: Any, where: str) -> Any:
    _, inner = _optional(annotation)
    if get_origin(inner) is tuple:
        if not isinstance(value, list) or not all(isinstance(v, str) and v for v in value):
            raise EventError(f"{where}: expected a list of non-empty strings")
        return tuple(value)
    # `bool` is an `int`; refuse it where an integer is meant, and the reverse.
    if inner is int and (not isinstance(value, int) or isinstance(value, bool)):
        raise EventError(f"{where}: expected an integer, found {type(value).__name__}")
    if inner is bool and not isinstance(value, bool):
        raise EventError(f"{where}: expected true or false, found {type(value).__name__}")
    if inner is str and not isinstance(value, str):
        raise EventError(f"{where}: expected a string, found {type(value).__name__}")
    return value


def _timestamp(text: str, where: str) -> datetime:
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise EventError(f"{where}: {text!r} is not ISO-8601") from None
    if moment.tzinfo is None:
        raise EventError(f"{where}: {text!r} has no UTC offset")
    return moment.astimezone(UTC)


def _optional(annotation: Any) -> tuple[bool, Any]:
    if get_origin(annotation) in (types.UnionType, typing.Union):
        args = [a for a in get_args(annotation) if a is not type(None)]
        return True, args[0]
    return False, annotation


# -- the published schema -----------------------------------------------------------


def _schema_for(annotation: Any) -> dict[str, Any]:
    _, inner = _optional(annotation)
    if get_origin(inner) is tuple:
        return {"type": "array", "items": {"type": "string", "minLength": 1}}
    return {
        int: {"type": "integer"},
        bool: {"type": "boolean"},
        str: {"type": "string"},
    }[inner]


def build_spec() -> dict[str, Any]:
    """The contract's JSON Schema, generated from :class:`WriteCompleted`."""
    hints = typing.get_type_hints(WriteCompleted)
    properties = {}
    required = []
    for f in dataclasses.fields(WriteCompleted):
        node = _schema_for(hints[f.name])
        if f.default is not dataclasses.MISSING:
            # An optional field may be omitted or sent as null; the parser
            # treats both as absent, and producers that serialise unset fields
            # as null are common. The schema has to say the same.
            node["type"] = [node["type"], "null"]
        node.update(f.metadata)
        if f.name in ("completed_at", "started_at"):
            node["format"] = "date-time"
        properties[f.name] = node
        if f.default is dataclasses.MISSING:
            required.append(f.name)
    properties["namespace"]["minItems"] = 1
    for key in ("warehouse", "table"):
        properties[key]["minLength"] = 1
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"{DATASCHEMA_BASE}/{CONTRACT_VERSION}",
        "title": "iceberg.table.write.completed",
        "description": (
            "A CloudEvents 1.0 event, structured-mode JSON, that an Iceberg writer "
            "emits once when it has finished with a table for a run. Generated from "
            "zamboni.events; do not edit by hand. See docs/write-completed.md."
        ),
        "x-contract-version": CONTRACT_VERSION,
        "type": "object",
        "required": list(_ENVELOPE_REQUIRED),
        "properties": {
            "specversion": {"const": "1.0"},
            "type": {"const": EVENT_TYPE},
            "source": {
                "type": "string",
                "minLength": 1,
                "description": 'The producer and its host, "uri:<producer>:<host>".',
            },
            "id": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Deterministic, so a redelivered or re-emitted event can be "
                    'recognised: "<warehouse>/<namespace levels joined by />/<table>'
                    '@<snapshot_id>".'
                ),
            },
            "dataschema": {
                "type": "string",
                "description": (
                    f"{DATASCHEMA_BASE}/<major>.<minor> -- the contract version the "
                    "event was written against."
                ),
            },
            "datacontenttype": {"const": "application/json"},
            "data": {
                "type": "object",
                "required": required,
                "properties": properties,
                # A minor version adds optional fields; a consumer of an older
                # minor must still accept the event.
                "additionalProperties": True,
            },
        },
    }


def get_write_completed_spec() -> dict[str, Any]:
    """The published JSON Schema for `write.completed`, as a dict."""
    loaded: dict[str, Any] = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    return loaded


# -- mapping an event to a table -------------------------------------------------------


@dataclass(frozen=True)
class Mapped:
    warehouse: str
    table: str


@dataclass(frozen=True)
class Unmapped:
    """An event for a warehouse or table this fleet does not manage. Not an error."""

    reason: str


def map_event(event: Event, fleet: Any) -> Mapped | Unmapped:
    """Which managed table `event` is about, if any.

    By warehouse **name, exactly**, then ``namespace.table`` against that
    warehouse's table config. ``catalog_uri``, ``warehouse_id`` and
    ``table_uuid`` are never matched (docs/maintenance-runtime.md §5.2).
    ``fleet`` is a :class:`~zamboni.fleet.FleetConfig`; typed loosely so this
    module stays free of imports a transport does not need.
    """
    data = event.data
    try:
        warehouse = fleet[data.warehouse]
    except KeyError:
        return Unmapped(f"warehouse {data.warehouse!r} is not in the fleet")
    if data.identifier not in warehouse.table_config.tables:
        return Unmapped(f"table {data.identifier!r} is not in {data.warehouse!r}'s table config")
    return Mapped(warehouse=data.warehouse, table=data.identifier)
