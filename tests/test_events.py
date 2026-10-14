# SPDX-License-Identifier: Apache-2.0
"""The `write.completed` contract: spec, parser, docs and mapping agree.

Four descriptions of one contract exist -- the dataclass, the published JSON
Schema, the parser and docs/write-completed.md -- and the tests here are what
keep them one. The schema is generated, so the shipped file is compared with a
fresh generation; the parser and the schema are run over the same good and bad
events; and the doc's example and field table are checked against the code.
"""

from __future__ import annotations

import copy
import json
import re
import uuid
from pathlib import Path

import jsonschema
import pytest

import zamboni
from zamboni.events import (
    CONTRACT_VERSION,
    DATASCHEMA_BASE,
    SPEC_PATH,
    EventError,
    Mapped,
    Unmapped,
    build_spec,
    get_write_completed_spec,
    map_event,
    parse_event,
)
from zamboni.fleet import CronSchedule, FleetConfig, FleetWarehouse
from zamboni.tableconfig import NamespaceSettings, TableConfig, TableSettings

DOC = Path(__file__).resolve().parent.parent / "docs" / "write-completed.md"


def event(**data_overrides) -> dict:
    data = {
        "warehouse": "acme",
        "namespace": ["raw"],
        "table": "events",
        "snapshot_id": 42,
        "completed_at": "2026-10-08T02:00:00Z",
    }
    data.update(data_overrides)
    return {
        "specversion": "1.0",
        "type": "writeCompleted",
        "source": "uri:test:host",
        "id": "acme/raw/events@42",
        "dataschema": f"{DATASCHEMA_BASE}/{CONTRACT_VERSION}",
        "data": data,
    }


def schema_accepts(doc: dict) -> bool:
    try:
        jsonschema.validate(doc, get_write_completed_spec())
    except jsonschema.ValidationError:
        return False
    return True


# -- the published schema -----------------------------------------------------------


def test_the_shipped_spec_is_a_fresh_generation():
    """The one failure a committed generated file is prone to: going stale."""
    assert SPEC_PATH.read_text() == json.dumps(build_spec(), indent=2) + "\n", (
        "zamboni-write-completed_spec.json is stale; regenerate it from zamboni.events"
    )


def test_the_spec_is_valid_json_schema_and_versioned():
    spec = get_write_completed_spec()
    jsonschema.Draft202012Validator.check_schema(spec)
    assert spec["x-contract-version"] == CONTRACT_VERSION
    assert spec["$id"].endswith(f"/{CONTRACT_VERSION}")


def test_the_spec_is_public_api():
    assert zamboni.get_write_completed_spec() == get_write_completed_spec()
    assert "get_write_completed_spec" in zamboni.__all__


# -- parser and schema agree ----------------------------------------------------------


GOOD = [
    event(),
    event(namespace=["raw", "telemetry"]),
    event(rows=10, full_refresh=False, streams=["a", "b"], table_uuid="u"),
    event(completed_at="2026-10-08T04:00:00+02:00"),
    event(started_at=None),  # an explicit null on an optional field
    event(future_field={"added": "in 1.1"}),  # a newer minor version
]

BAD = [
    ("missing table", event(table=None)),
    ("empty namespace", event(namespace=[])),
    ("dotted-string namespace", event(namespace="raw.telemetry")),
    ("bool as integer", event(snapshot_id=True)),
    ("string as integer", event(snapshot_id="42")),
    ("empty warehouse", event(warehouse="")),
    ("empty namespace level", event(namespace=["raw", ""])),
    ("integer as bool", event(full_refresh=1)),
]


@pytest.mark.parametrize("doc", GOOD)
def test_a_good_event_parses_and_validates(doc):
    parsed = parse_event(json.dumps(doc))
    assert parsed.data.warehouse == "acme"
    assert schema_accepts(doc)


@pytest.mark.parametrize(("what", "doc"), BAD, ids=[w for w, _ in BAD])
def test_a_bad_event_is_refused_by_both(what, doc):
    with pytest.raises(EventError):
        parse_event(doc)
    assert not schema_accepts(doc), f"the schema accepted an event the parser refuses: {what}"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"specversion": "0.3"}, "only CloudEvents 1.0"),
        ({"type": "updateTable"}, "is not 'writeCompleted'"),
        ({"id": ""}, "id: expected a non-empty string"),
        ({"dataschema": f"{DATASCHEMA_BASE}/2.0"}, "A new major is not compatible"),
        ({"dataschema": "nonsense"}, "expected '<uri>/<major>.<minor>'"),
    ],
)
def test_the_envelope_is_checked(change, message):
    doc = event()
    doc.update(change)
    with pytest.raises(EventError, match=re.escape(message)):
        parse_event(doc)


def test_a_newer_minor_version_is_accepted():
    doc = event(future_field=1)
    doc["dataschema"] = f"{DATASCHEMA_BASE}/1.7"
    assert parse_event(doc).data.table == "events"


@pytest.mark.parametrize("payload", [b"not json", b"[1, 2]", "\x00"])
def test_garbage_is_an_event_error_not_a_crash(payload):
    with pytest.raises(EventError):
        parse_event(payload)


def test_a_timestamp_without_an_offset_is_refused():
    with pytest.raises(EventError, match="no UTC offset"):
        parse_event(event(completed_at="2026-10-08T02:00:00"))


def test_the_identifier_and_the_deterministic_id():
    parsed = parse_event(event(namespace=["raw", "telemetry"]))
    assert parsed.data.identifier == "raw.telemetry.events"
    assert parsed.data.expected_id == "acme/raw/telemetry/events@42"


# -- Lakekeeper's own events (#182) -----------------------------------------------------

#: An `updateTable` captured from the dev stack's Lakekeeper (v0.13.1) on
#: 2026-10-14, after one append. tests/test_dev_stack.py checks that a live one
#: still has this shape, so an upgrade that changes it fails a test, not the docs.
LAKEKEEPER_UPDATE_TABLE = (
    Path(__file__).resolve().parent / "fixtures" / "lakekeeper-0.13.1-updateTable.json"
)


def lakekeeper_event() -> dict:
    return json.loads(LAKEKEEPER_UPDATE_TABLE.read_text())


def test_lakekeepers_event_has_the_envelope_the_docs_say_ours_shares():
    """What docs/write-completed.md means by "the envelope and wire shape
    Lakekeeper uses": CloudEvents 1.0 in structured mode -- attributes and
    data in one JSON object -- with a camelCase type. Every attribute ours
    requires is there."""
    doc = lakekeeper_event()
    assert doc["specversion"] == "1.0"
    assert doc["type"] == "updateTable"
    assert doc["datacontenttype"] == "application/json"
    assert isinstance(doc["data"], dict)
    assert set(event()) - {"dataschema"} <= set(doc)


def test_lakekeepers_event_is_the_commit_request_with_a_fresh_id():
    """The data is the committed request -- not the table's resulting metadata
    -- and the id a `Uuid::now_v7()` drawn per publication
    (`CloudEventsPublisher::transaction_committed`). Nothing in the id derives
    from the write, so Lakekeeper's events cannot be deduplicated by it; that is
    why ours is deterministic. One event per commit is checked live, in
    tests/test_dev_stack.py."""
    doc = lakekeeper_event()
    assert {"requirements", "updates"} <= set(doc["data"])
    assert [u["action"] for u in doc["data"]["updates"]] == ["add-snapshot", "set-snapshot-ref"]
    assert uuid.UUID(doc["id"]).version == 7


def test_a_lakekeeper_event_is_refused_not_mistaken_for_ours():
    """One subscriber stack reads both; a consumer of ours never acts on theirs."""
    with pytest.raises(EventError, match="type 'updateTable' is not 'writeCompleted'"):
        parse_event(lakekeeper_event())
    assert not schema_accepts(lakekeeper_event())


# -- the doc ----------------------------------------------------------------------------


def test_the_documented_example_parses_and_validates():
    block = re.search(r"```json\n(.*?)```", DOC.read_text(), re.S)
    assert block, "docs/write-completed.md lost its example event"
    doc = json.loads(block.group(1))
    parsed = parse_event(doc)
    assert schema_accepts(doc)
    assert parsed.id == parsed.data.expected_id, "the example's id is not the deterministic one"


def test_the_doc_documents_every_payload_field_and_its_requiredness():
    text = DOC.read_text()
    spec = get_write_completed_spec()["properties"]["data"]
    rows = dict(re.findall(r"^\| `(\w+)` \| (yes|no) \|", text, re.M))
    for name in spec["properties"]:
        assert name in rows, f"docs/write-completed.md does not document data.{name}"
        expected = "yes" if name in spec["required"] else "no"
        assert rows[name] == expected, f"data.{name}: the doc says {rows[name]!r}"


def test_the_doc_states_the_current_version():
    assert f"**Contract version {CONTRACT_VERSION}.**" in DOC.read_text()


# -- mapping -----------------------------------------------------------------------------


@pytest.fixture
def fleet():
    config = TableConfig(
        warehouse="acme",
        namespaces={
            "raw": NamespaceSettings(tables={"events": TableSettings()}),
            "raw.telemetry": NamespaceSettings(tables={"events": TableSettings()}),
        },
    )
    return FleetConfig(warehouses=(FleetWarehouse("acme", CronSchedule("0 2 * * *"), config),))


def test_an_event_maps_by_warehouse_name_and_identifier(fleet):
    assert map_event(parse_event(event()), fleet) == Mapped("acme", "raw.events")
    nested = parse_event(event(namespace=["raw", "telemetry"]))
    assert map_event(nested, fleet) == Mapped("acme", "raw.telemetry.events")


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"warehouse": "globex"}, "warehouse 'globex' is not in the fleet"),
        ({"table": "other"}, "table 'raw.other' is not in 'acme'"),
        ({"warehouse": "ACME"}, "is not in the fleet"),  # exact match, not case-folded
    ],
)
def test_an_unmanaged_table_is_unmapped_not_an_error(fleet, overrides, reason):
    result = map_event(parse_event(event(**overrides)), fleet)
    assert isinstance(result, Unmapped)
    assert reason in result.reason


def test_informational_fields_never_affect_mapping(fleet):
    """catalog_uri, warehouse_id and table_uuid are never matched."""
    noisy = event(catalog_uri="https://elsewhere", warehouse_id="w-uuid", table_uuid="t-uuid")
    assert map_event(parse_event(noisy), fleet) == Mapped("acme", "raw.events")


def test_parsing_does_not_mutate_its_input():
    doc = event()
    before = copy.deepcopy(doc)
    parse_event(doc)
    assert doc == before
