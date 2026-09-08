# SPDX-License-Identifier: Apache-2.0
"""The shipped JSON Schema for `table-config.json` (ZMBNI-26).

The schema is generated from `zamboni.tableconfig`, so the interesting failures
are not "is this valid JSON Schema" but the two ways a derived artifact goes
wrong: the committed file drifting from the generator, and the generator drifting
from the loader it claims to describe.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from zamboni import get_table_config_spec
from zamboni.tableconfig import SPEC_VERSION, TableConfig, TableConfigError
from zamboni.tableconfig_schema import DIALECT, SPEC_PATH, build_schema

REGENERATE = "uv run scripts/build-table-config-schema.py"
REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def schema() -> dict:
    return get_table_config_spec()


@pytest.fixture(scope="module")
def validator(schema) -> Draft202012Validator:
    return Draft202012Validator(schema)


def objects(node, path="$"):
    """Every object node in the schema, with the path that reaches it."""
    if not isinstance(node, dict):
        return
    if node.get("type") == "object" or "properties" in node:
        yield path, node
    for key, child in (node.get("properties") or {}).items():
        yield from objects(child, f"{path}.{key}")
    if isinstance(node.get("additionalProperties"), dict):
        yield from objects(node["additionalProperties"], f"{path}.*")
    if "items" in node:
        yield from objects(node["items"], f"{path}[]")


# -- the artifact matches the generator -----------------------------------


def test_the_shipped_schema_matches_a_fresh_generation():
    """The one failure a committed generated file is actually prone to.

    A field added to `tableconfig.py` widens the generator and leaves the file
    behind, and nothing else would notice: the loader keeps working, so only a
    caller validating against a stale schema sees it -- as a rejection of a
    config that is perfectly legal.
    """
    assert json.loads(SPEC_PATH.read_text()) == build_schema(), (
        f"{SPEC_PATH.name} is stale. Run `{REGENERATE}` and commit the result."
    )


def test_the_file_is_formatted_as_the_script_writes_it():
    """Byte equality, not just equal JSON, so a hand-edit shows up as a diff."""
    assert SPEC_PATH.read_text() == json.dumps(build_schema(), indent=2) + "\n", (
        f"{SPEC_PATH.name} differs in formatting from `{REGENERATE}`"
    )


def test_the_public_api_serves_the_shipped_file(schema):
    """`get_table_config_spec()` reads the artifact rather than regenerating.

    Which is the point: a caller gets the bytes their wheel shipped, so what they
    validated against is reproducible from the version they installed.
    """
    assert schema == json.loads(SPEC_PATH.read_text())


# -- what the schema declares about itself --------------------------------


def test_it_is_a_valid_schema_in_the_dialect_it_declares(schema):
    assert schema["$schema"] == DIALECT
    Draft202012Validator.check_schema(schema)


def test_the_format_version_is_pinned_not_merely_typed(schema, validator):
    """`validate()` refuses any version but `SPEC_VERSION`, so `{"type":
    "integer"}` would pass a file the loader rejects."""
    assert schema["properties"]["version"]["const"] == SPEC_VERSION

    good = {"warehouse": "w", "version": SPEC_VERSION, "namespaces": {}}
    assert validator.is_valid(good)
    assert not validator.is_valid({**good, "version": SPEC_VERSION + 1})
    # Absent is still fine: `from_dict` defaults it.
    assert validator.is_valid({"warehouse": "w", "namespaces": {}})


def test_every_object_forbids_unknown_keys(schema):
    """The loader rejects unknown keys "so a typo cannot silently change a
    table's layout". A permissive schema would validate files it then refuses,
    which is worse than shipping none.

    Every object node, derived by walking the schema, rather than a list of the
    ones someone remembered.
    """
    permissive = [
        path
        for path, node in objects(schema)
        if node.get("additionalProperties") is not False and "properties" in node
    ]
    assert not permissive, f"objects that would accept an unknown key: {permissive}"


def test_the_walk_actually_reaches_the_nested_blocks(schema):
    """Guard on the guard: an `objects()` that stopped early would make the test
    above vacuously green."""
    reached = {path for path, _ in objects(schema)}
    for expected in (
        "$.defaults.ordering.zorder",
        "$.defaults.retention.expire_snapshots",
        "$.defaults.partition[]",
        "$.defaults.partition_evolution.rules[]",
        "$.namespaces.*.tables.*.retention.metadata",
    ):
        assert expected in reached, f"the walk never reached {expected}"


# -- the generator agrees with the loader ---------------------------------

#: One unknown key per block, and the document that puts it there. The value is
#: the loader's own `allowed: [...]` list, which `_reject_unknown` writes into its
#: message -- so the loader is the oracle and the comparison is not two copies of
#: one literal.
UNKNOWN_KEY_SITES: dict[str, dict] = {
    "$": {"warehouse": "w", "namespaces": {}, "nope": 1},
    "$.defaults": {"warehouse": "w", "namespaces": {}, "defaults": {"nope": 1}},
    "$.namespaces.*": {"warehouse": "w", "namespaces": {"db": {"nope": 1}}},
    "$.namespaces.*.tables.*": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"nope": 1}}}},
    },
    "$.namespaces.*.tables.*.partition[]": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"partition": [{"column": "c", "nope": 1}]}}}},
    },
    "$.namespaces.*.tables.*.partition_evolution": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"partition_evolution": {"nope": 1}}}}},
    },
    "$.namespaces.*.tables.*.partition_evolution.rules[]": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"partition_evolution": {"rules": [{"nope": 1}]}}}}},
    },
    "$.namespaces.*.tables.*.ordering": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"ordering": {"nope": 1}}}}},
    },
    "$.namespaces.*.tables.*.ordering.sort[]": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"ordering": {"sort": [{"nope": 1}]}}}}},
    },
    "$.namespaces.*.tables.*.ordering.zorder": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"ordering": {"zorder": {"nope": 1}}}}}},
    },
    "$.namespaces.*.tables.*.retention": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"retention": {"nope": 1}}}}},
    },
    "$.namespaces.*.tables.*.retention.expire_snapshots": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"retention": {"expire_snapshots": {"nope": 1}}}}}},
    },
    "$.namespaces.*.tables.*.retention.remove_orphan_files": {
        "warehouse": "w",
        "namespaces": {
            "db": {"tables": {"t": {"retention": {"remove_orphan_files": {"nope": 1}}}}}
        },
    },
    "$.namespaces.*.tables.*.retention.remove_dangling_deletes": {
        "warehouse": "w",
        "namespaces": {
            "db": {"tables": {"t": {"retention": {"remove_dangling_deletes": {"nope": 1}}}}}
        },
    },
    "$.namespaces.*.tables.*.retention.rewrite_manifests": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"retention": {"rewrite_manifests": {"nope": 1}}}}}},
    },
    "$.namespaces.*.tables.*.retention.metadata": {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"retention": {"metadata": {"nope": 1}}}}}},
    },
}


def node_at(schema: dict, path: str) -> dict:
    return next(node for p, node in objects(schema) if p == path)


@pytest.mark.parametrize("path", sorted(UNKNOWN_KEY_SITES))
def test_the_schemas_keys_are_the_keys_the_loader_accepts(schema, path):
    """Compared against the loader's own answer, not against a literal.

    `_reject_unknown` writes `allowed: [...]` into its message, so feeding it an
    unknown key at each location makes the loader state its accepted key set and
    the schema's `properties` can be checked against it. A generator that
    invented, dropped or misspelled a key fails here -- which is the failure
    `from`/`to` would have been, had `JSON_NAME` been assumed rather than stated.
    """
    with pytest.raises(TableConfigError) as caught:
        TableConfig.from_dict(UNKNOWN_KEY_SITES[path])

    message = str(caught.value)
    assert "unknown key(s)" in message, f"{path}: not an unknown-key refusal: {message}"
    allowed = set(json.loads(message.split("allowed: ")[1].split(". ")[0].replace("'", '"')))

    assert set(node_at(schema, path)["properties"]) == allowed, (
        f"{path}: the schema and the loader disagree about the accepted keys"
    )


def test_every_object_in_the_schema_has_an_unknown_key_site(schema):
    """So a new block cannot be added without being checked against the loader.

    Paths are compared with `defaults` collapsed onto the table form: the two are
    the same `TableSettings` schema reached by two routes, and checking one checks
    both.
    """
    reached = {path.replace("$.defaults", "$.namespaces.*.tables.*") for path, _ in objects(schema)}
    unchecked = reached - set(UNKNOWN_KEY_SITES) - {"$.namespaces.*.tables", "$.namespaces"}
    assert not unchecked, f"object nodes with no unknown-key site: {sorted(unchecked)}"


# -- the samples in the repository and the docs ---------------------------


def test_the_shipped_example_validates(validator):
    validator.validate(json.loads((REPO / "examples/table-config.json").read_text()))


def test_every_guide_sample_validates(validator):
    """The samples a reader copies whole, checked against the schema as well as
    the loader -- `test_the_documented_configurations_are_valid` does the loader
    half already, and a sample that loads but fails the schema would mean the
    schema is wrong, not the sample."""
    from tests.test_docs import config_samples

    samples = config_samples()
    assert samples, "no sample configurations found; the extraction stopped matching"
    for name, raw in samples:
        errors = sorted(validator.iter_errors(raw), key=str)
        assert not errors, f"{name}: {[e.message for e in errors]}"


def test_the_demos_config_validates(validator):
    """Shipped in the wheel too, so a reader running `zamboni-demo` is running a
    config this schema accepts."""
    validator.validate(json.loads((REPO / "data/healthims/table-config.json").read_text()))


# -- the boundary the schema does not claim to police ---------------------


def test_a_schema_valid_config_can_still_be_refused_by_the_loader(validator):
    """Stated as a test because it is the schema's contract, not a shortcoming.

    Cross-field rules cannot be expressed in JSON Schema, so the schema is a
    shape check and `load()` stays the authority. Pinning it stops someone
    "fixing" the schema by hand-copying the loader's numbers into it, which is
    the drift the generator exists to prevent.
    """
    mode_without_keys = {
        "warehouse": "w",
        "namespaces": {"db": {"tables": {"t": {"ordering": {"mode": "sort"}}}}},
    }
    assert validator.is_valid(mode_without_keys), "the shape is fine, so the schema must pass it"
    with pytest.raises(TableConfigError, match="requires a non-empty"):
        TableConfig.from_dict(mode_without_keys).validate()


def test_the_schema_never_rejects_what_the_loader_accepts(validator):
    """The direction that would make the schema actively harmful.

    A config the loader is happy with must validate, or the schema turns working
    files into apparent errors. Includes the null-for-a-scalar spellings, each
    verified against the loader.
    """
    accepted = {
        "warehouse": "w",
        "version": SPEC_VERSION,
        "defaults": {"target_file_size_bytes": None, "description": None},
        "namespaces": {
            "raw.telemetry": {
                "tables": {
                    "events": {
                        "partition": [
                            {"column": "ts", "transform": "day"},
                            {"column": "id", "transform": "bucket", "num_buckets": 8},
                            {"column": "k", "transform": "truncate", "width": 4, "name": "k4"},
                        ],
                        "partition_evolution": {
                            "enabled": True,
                            "rules": [{"from": "day", "to": "month", "older_than_days": 90}],
                        },
                        "ordering": {"mode": "zorder", "zorder": {"columns": ["a", "b"]}},
                        "retention": {
                            "expire_snapshots": {
                                "enabled": True,
                                "max_snapshot_age_days": 7,
                                "min_snapshots_to_keep": None,
                                "max_ref_age_days": None,
                            },
                            "remove_orphan_files": {"enabled": True, "older_than_days": 3},
                            "remove_dangling_deletes": {"enabled": True},
                            "rewrite_manifests": {"enabled": True, "min_input_manifests": 2},
                            "metadata": {
                                "previous_versions_max": None,
                                "delete_after_commit": None,
                            },
                        },
                    }
                }
            }
        },
    }
    TableConfig.from_dict(accepted).validate()  # the loader accepts it
    errors = sorted(validator.iter_errors(accepted), key=str)
    assert not errors, (
        f"the schema rejects a config the loader accepts: {[e.message for e in errors]}"
    )


# -- null, everywhere (ZMBNI-53) ------------------------------------------


def field_paths(node, path="$"):
    """Every field position in the schema, as a path a document can be built at.

    `*` stands for a mapping key an author chooses, `[]` for a list element.
    """
    if not isinstance(node, dict):
        return
    for key, child in (node.get("properties") or {}).items():
        yield f"{path}.{key}"
        yield from field_paths(child, f"{path}.{key}")
    if isinstance(node.get("additionalProperties"), dict):
        yield from field_paths(node["additionalProperties"], f"{path}.*")
    if "items" in node:
        yield from field_paths(node["items"], f"{path}[]")


def document_with(path: str, value):
    """A document carrying ``value`` at ``path``, built from the path alone.

    Everything above the target is materialised as it goes, so this needs no
    per-location fixture and cannot fall behind a schema that grows a block.
    """
    segments = [s for s in path.removeprefix("$").split(".") if s]
    root: dict = {}
    cursor: Any = root
    for i, segment in enumerate(segments):
        last = i == len(segments) - 1
        listed = segment.endswith("[]")
        name = segment.removesuffix("[]")
        key = "chosen" if name == "*" else name
        placed = value if last else ({} if not listed else [{}])
        if listed and not last:
            cursor[key] = [{}]
            cursor = cursor[key][0]
        elif listed and last:
            cursor[key] = [value]
        else:
            cursor[key] = placed
            cursor = cursor[key]
    # `warehouse` is required, so supply it unless it is the thing under test.
    root.setdefault("warehouse", "w")
    return root


def loader_accepts(document) -> bool:
    try:
        TableConfig.from_dict(document).validate()
    except TableConfigError:
        return False
    return True


@pytest.mark.parametrize("path", sorted(set(field_paths(build_schema()))))
def test_the_schema_and_the_loader_agree_about_null_everywhere(validator, path):
    """A `null` is either accepted by both or refused by both, at every position.

    This is the test that would have caught both halves of ZMBNI-53, and it exists
    because the first version of `test_the_schema_never_rejects_what_the_loader_accepts`
    only spelled `null` on *scalar* fields. Two opposite disagreements were hiding
    in the gap:

    * thirteen nested blocks made the loader raise a bare `TypeError` rather than
      a `TableConfigError`, so it did not "reject" a document so much as crash on
      it;
    * four container fields -- `namespaces`, `partition`,
      `partition_evolution.rules`, `ordering.sort` -- are read as
      `raw.get(key) or <empty>`, so the loader accepted a null the shipped schema
      refused. That is the direction that makes a schema harmful: working files
      reported as errors.

    Swept over every field position derived from the schema, so neither a new
    block nor a new coalescing field can reintroduce either shape. A `TypeError`
    escaping `from_dict` fails here rather than being counted as a rejection,
    which is the whole point of the loader half.
    """
    document = document_with(path, None)

    try:
        loader = loader_accepts(document)
    except TableConfigError:  # pragma: no cover - loader_accepts handles it
        loader = False

    assert validator.is_valid(document) is loader, (
        f"null at {path}: the loader {'accepts' if loader else 'refuses'} it and "
        f"the schema {'accepts' if not loader else 'refuses'} it. Either add the "
        f"field to NULL_MEANS_EMPTY or make the loader refuse it."
    )


@pytest.mark.parametrize("path", sorted(set(field_paths(build_schema()))))
def test_no_document_makes_the_loader_crash_instead_of_refusing(path):
    """`TableConfigError` is the type a caller is told to catch.

    The CLI maps it to exit 2 and the user guide documents it as *the* config
    failure, so a `TypeError` out of `from_dict` is a broken promise rather than
    an ugly message. Asserted for a null and for a string, the two shapes a
    templated or hand-edited file produces: a string did not crash before the fix
    either, which was worse -- `set("day")` is a perfectly good set, so
    `"ordering": "day"` reported its own characters as unknown keys.
    """
    for value in (None, "day"):
        document = document_with(path, value)
        try:
            TableConfig.from_dict(document).validate()
        except TableConfigError:
            pass
        except Exception as exc:
            raise AssertionError(
                f"{value!r} at {path} raised {type(exc).__name__}: {exc}. "
                "A malformed config must raise TableConfigError, naming the path."
            ) from exc


def test_the_sweep_reaches_the_fields_it_claims_to():
    """Guard on the guard: a `field_paths` that stopped early, or a
    `document_with` that built the wrong shape, would make both sweeps above
    vacuously green."""
    paths = set(field_paths(build_schema()))
    for expected in (
        "$.warehouse",
        "$.defaults.ordering.zorder.precision_bits",
        "$.namespaces.*.tables.*.retention.expire_snapshots.enabled",
        "$.namespaces.*.tables.*.partition[].column",
        "$.namespaces.*.tables.*.partition_evolution.rules[].from",
    ):
        assert expected in paths, f"the sweep never reached {expected}"

    assert document_with("$.namespaces.*.tables.*.ordering", None) == {
        "namespaces": {"chosen": {"tables": {"chosen": {"ordering": None}}}},
        "warehouse": "w",
    }
    assert document_with("$.namespaces.*.tables.*.partition[].column", None) == {
        "namespaces": {"chosen": {"tables": {"chosen": {"partition": [{"column": None}]}}}},
        "warehouse": "w",
    }
