# SPDX-License-Identifier: Apache-2.0
"""Generate the JSON Schema for ``table-config.json`` from the dataclasses.

**Derived, never hand-written.** A hand-written schema would make three
descriptions of one format -- `docs/table-config.md`, :mod:`zamboni.tableconfig`
and the schema -- and two of them would drift. This package has killed that
pattern before: :class:`~zamboni.maintainers.LayoutFeature` exists because a
warning string and a capability declaration were two copies of one fact.
:mod:`zamboni.tableconfig` is the executable spec, so the schema comes out of it
and :func:`zamboni.get_table_config_spec` serves the generated file.

What the schema does and does not check
=======================================

It is a **shape** check: keys, types, enumerations, required fields, and
``additionalProperties: false`` everywhere. ``TableConfig.load()`` stays the
authority, because the rules it enforces beyond shape cannot be written in JSON
Schema at all:

* cross-field rules -- ``ordering.mode: 'sort'`` requires a non-empty ``sort``
  list and forbids ``zorder``; ``remove_orphan_files`` may not be enabled while
  ``expire_snapshots`` is not; an evolution rule must name a granularity some
  partition field actually uses.
* ordering rules -- evolution must move to a *coarser* granularity.
* numeric floors -- ``min_snapshots_to_keep >= 1``, ``precision_bits`` in 1..32,
  the 62-bit z-order budget.

Those floors are deliberately **not** encoded, even though a `minimum` could
express some of them. They live as literals inside ``validate()`` method bodies,
so putting them here would be hand-copying numbers out of Python into JSON --
exactly the drift this module exists to prevent. A schema that passes and a
config that then fails to load is the documented, expected relationship; the
reverse is not, and is what the tests pin.

Nulls are rejected for every nested block, which is what the loader does
========================================================================

``"ordering": null`` and its twelve siblings do not merely fail -- they raise a
bare ``TypeError: 'NoneType' object is not iterable`` out of ``_reject_unknown``,
because it calls ``set(raw)`` on whatever it is handed. Measured across every
nested block: only ``namespaces.<ns>.tables`` fails cleanly, and only because a
missing ``tables`` is checked separately.

So the schema types those blocks as ``object`` with no ``null`` alternative. That
matches observed behaviour today and stays correct however the crash is fixed,
since fixing it means rejecting ``null`` cleanly rather than accepting it -- and
a caller who validates first gets a located message instead of a traceback.

Scalar ``X | None`` fields are different and do accept ``null``: ``raw.get(key)``
yields ``None`` either way, so ``"target_file_size_bytes": null`` loads fine and
means "unset". Those are typed as a union with ``"null"``.
"""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from pathlib import Path
from typing import Any, Literal, get_args, get_origin

from . import tableconfig as tc

#: The generated file, shipped in the wheel beside the package. Committed rather
#: than built at import time so an installed wheel carries it without running a
#: generator, and kept current by a test rather than by trust -- the same
#: arrangement as `bin/` and `scripts/build-executable.py`.
SPEC_PATH = Path(__file__).with_name("zamboni-table-config_spec.json")

#: JSON Schema dialect. Declared the way JSON Schema declares it, which is a
#: separate axis from the config format's own ``version`` and must not be
#: conflated with it: this says how to read the schema, ``version`` says which
#: revision of the format a file is written against.
DIALECT = "https://json-schema.org/draft/2020-12/schema"

SCHEMA_ID = (
    "https://github.com/paulcaron16k/Zamboni/blob/main/src/zamboni/zamboni-table-config_spec.json"
)

#: Field name in Python -> key in the file, where they differ.
#:
#: Only ``EvolutionRule`` needs this, and it is the reason the mapping is
#: explicit rather than assumed: the first draft of the user guide's event-data
#: example used ``from_transform``/``to_transform``, the *attribute* names, and
#: was rejected by the loader (see `test_the_documented_configurations_are_valid`).
#: ``from`` is a Python keyword, so the attribute cannot share the file's spelling
#: and a generator that assumed it could would emit a schema that rejects every
#: valid file.
JSON_NAME: dict[tuple[type, str], str] = {
    (tc.EvolutionRule, "from_transform"): "from",
    (tc.EvolutionRule, "to_transform"): "to",
}

#: Fields that exist on the dataclass but are never authored in the file.
#:
#: ``tables`` is ``init=False`` and derived in ``__post_init__``; it is excluded
#: by that flag rather than by name. ``source`` is the path the config was read
#: from, set by the loader, and has no spelling in the file at all -- the root's
#: own ``_reject_unknown`` set is the proof: ``{"version", "warehouse",
#: "defaults", "namespaces"}``.
NOT_AUTHORED: frozenset[tuple[type, str]] = frozenset({(tc.TableConfig, "source")})

#: Fields whose permitted values live in a module constant rather than in a
#: ``Literal``. Referenced, not copied, so adding a transform upstream widens the
#: schema on the next generation instead of leaving it stale.
ENUM_FROM_CONSTANT: dict[tuple[type, str], tuple[str, ...]] = {
    (tc.PartitionField, "transform"): tuple(sorted(tc.PARTITION_TRANSFORMS)),
    (tc.EvolutionRule, "from_transform"): tc.TIME_GRANULARITIES,
    (tc.EvolutionRule, "to_transform"): tc.TIME_GRANULARITIES,
}

#: Prose for the keys where the *why* is not evident from the name. Taken from
#: the dataclass docstrings and comments rather than invented here; a key absent
#: from this map simply carries no description.
DESCRIPTIONS: dict[tuple[type, str], str] = {
    (tc.TableConfig, "warehouse"): (
        "Which warehouse this file describes. Required, and asserted rather than "
        "selected: --warehouse/--db or the profile chooses the warehouse, so a "
        "config copied into the wrong directory is an error instead of a silent "
        "maintenance run against the wrong tenant."
    ),
    (tc.TableConfig, "version"): (
        "Revision of this format. Version 2 states the namespace; version 1 keyed "
        "tables by a dotted string and left the split to be guessed."
    ),
    (tc.TableConfig, "defaults"): "Applied to every table unless the table overrides the section.",
    (tc.EvolutionRule, "older_than_days"): (
        "Measured from the end of the partition window, so a partition is left "
        "alone until every row it could contain is at least this old."
    ),
    (tc.RemoveOrphanFilesSettings, "older_than_days"): (
        "Must exceed the longest write the warehouse performs: a file written but "
        "not yet committed is indistinguishable from an orphan."
    ),
    (tc.ZOrder, "precision_bits"): (
        "Bits of resolution kept per column before interleaving. More bits means "
        "finer locality per column but fewer columns fit the 62-bit ordering key."
    ),
    (tc.MetadataSettings, "previous_versions_max"): (
        "Omit to leave the table property alone. Setting this without "
        "delete_after_commit trims the metadata log but deletes nothing, turning "
        "each dropped entry into an unreferenced file for orphan removal to sweep."
    ),
}


def _is_optional(annotation: Any) -> tuple[bool, Any]:
    """Split ``X | None`` into ``(True, X)``; anything else into ``(False, it)``."""
    if get_origin(annotation) in (types.UnionType, typing.Union):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1 and len(get_args(annotation)) == 2:
            return True, args[0]
    return False, annotation


def _is_dataclass_type(annotation: Any) -> typing.TypeGuard[type]:
    """A dataclass *class*, not an instance of one.

    Both halves are needed: `dataclasses.is_dataclass` answers True for an
    instance too, and every annotation here is a class -- so the isinstance check
    is what lets `for_dataclass(cls: type)` be typed honestly rather than cast to.
    """
    return isinstance(annotation, type) and dataclasses.is_dataclass(annotation)


def _scalar(annotation: Any) -> dict[str, Any] | None:
    return {bool: {"type": "boolean"}, int: {"type": "integer"}, str: {"type": "string"}}.get(
        annotation
    )


def _for_annotation(annotation: Any) -> dict[str, Any]:
    """The schema for one resolved type hint."""
    optional, inner = _is_optional(annotation)

    if get_origin(inner) is Literal:
        node: dict[str, Any] = {"enum": list(get_args(inner))}
    elif (scalar := _scalar(inner)) is not None:
        node = dict(scalar)
    elif get_origin(inner) is tuple:
        # `tuple[X, ...]` is how this format spells a JSON array; the dataclasses
        # are frozen, so the immutable form is the one they can hold.
        (item, _ellipsis) = get_args(inner)
        node = {"type": "array", "items": _for_annotation(item)}
    elif get_origin(inner) is dict:
        _key, value = get_args(inner)
        node = {"type": "object", "additionalProperties": _for_annotation(value)}
    elif _is_dataclass_type(inner):
        node = for_dataclass(inner)
    else:  # pragma: no cover - a new field shape should fail loudly, not silently
        raise TypeError(
            f"tableconfig_schema cannot express {annotation!r}. Teach this generator "
            "the new shape rather than hand-editing the schema, or the shipped file "
            "stops matching a fresh generation."
        )

    # Only scalars and enums gain a null alternative, and the test is what `inner`
    # *is* rather than what the node looks like: a dataclass node carries
    # `"type": "object"`, which is a perfectly good string, so keying off the node
    # silently gave `Ordering.zorder` a null alternative the loader crashes on.
    # `Ordering.zorder` is the only nested block typed `X | None`, so that one
    # field was the whole difference. See the module docstring.
    if optional and not _is_dataclass_type(inner):
        if "enum" in node:
            node["enum"] = [*node["enum"], None]
        elif isinstance(node.get("type"), str):
            node["type"] = [node["type"], "null"]
    return node


def for_dataclass(cls: type) -> dict[str, Any]:
    """An object schema for one dataclass, keyed as the file keys it."""
    hints = typing.get_type_hints(cls)
    properties: dict[str, Any] = {}
    for f in dataclasses.fields(cls):
        if not f.init or (cls, f.name) in NOT_AUTHORED:
            continue
        key = JSON_NAME.get((cls, f.name), f.name)
        if allowed := ENUM_FROM_CONSTANT.get((cls, f.name)):
            prop: dict[str, Any] = {"enum": list(allowed)}
        else:
            prop = _for_annotation(hints[f.name])
        if description := DESCRIPTIONS.get((cls, f.name)):
            prop["description"] = description
        properties[key] = prop

    node: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        # The loader rejects unknown keys "so a typo cannot silently change a
        # table's layout". A permissive schema would validate files it then
        # refuses, which is worse than shipping no schema at all.
        "additionalProperties": False,
    }
    if required := _required(cls):
        node["required"] = required
    return node


def _required(cls: type) -> list[str]:
    """Keys with no default, plus the ones ``validate()`` insists on.

    A dataclass default is the honest signal for "optional", and every field in
    this format has one -- so the list is short and comes from the loader's own
    refusals rather than from the type hints: ``warehouse`` is defaulted to ``""``
    and then rejected as empty, and ``column`` is defaulted to nothing but read
    with an explicit ``'column' is required`` check.
    """
    return {
        tc.TableConfig: ["warehouse"],
        tc.PartitionField: ["column"],
        tc.SortKey: ["column"],
        tc.EvolutionRule: ["from", "to", "older_than_days"],
    }.get(cls, [])


def build_schema() -> dict[str, Any]:
    """The whole schema, generated from :mod:`zamboni.tableconfig`."""
    root = for_dataclass(tc.TableConfig)

    # `version` is pinned rather than merely typed: `TableConfig.validate` refuses
    # any other value, so a schema saying "integer" would pass a file the loader
    # rejects. Absent is still fine -- `from_dict` defaults it.
    root["properties"]["version"] = {
        "const": tc.SPEC_VERSION,
        "description": root["properties"]["version"]["description"],
    }
    # Required *and* non-empty, which is two separate refusals in `validate()`.
    root["properties"]["warehouse"]["minLength"] = 1

    return {
        "$schema": DIALECT,
        "$id": SCHEMA_ID,
        "title": "Zamboni table-config.json",
        "description": (
            "Declarative layout and retention per table, authored as warehouse -> "
            "namespace -> table. Generated from zamboni.tableconfig; do not edit by "
            "hand. A shape check only: keys, types and enumerations. Cross-field and "
            "numeric rules are enforced by TableConfig.load(), which stays the "
            "authority -- so a file may satisfy this schema and still be refused, "
            "with a located message saying why."
        ),
        **root,
    }


def load_schema() -> dict[str, Any]:
    """The shipped schema, read from the file rather than regenerated.

    What :func:`zamboni.get_table_config_spec` serves. Reading the artifact is
    the point: a caller gets exactly the bytes their wheel shipped, so what they
    validated against is reproducible from the version they installed, and the
    generator is a build-time tool rather than an import-time cost.
    """
    return json.loads(SPEC_PATH.read_text())
