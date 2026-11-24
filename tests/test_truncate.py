"""A string upper bound that 0.12 rounds up to a surrogate no longer fails a write (#303).

This checkout resolves PyIceberg from the maintenance fork, which already has
the fix, so the mirror stands down here. To prove it on what a consumer
installs, ``stock_0_12`` puts back 0.12.0's function, copied verbatim, in both
places PyIceberg holds it, and answers the capability from it.
"""

from __future__ import annotations

import random
from dataclasses import replace

import pyarrow as pa
import pytest
from pyiceberg.io import pyarrow as pyarrow_io
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType
from pyiceberg.utils import truncate as library

from zamboni import CompactionConfig, TableCompactor, capabilities, truncate
from zamboni.capabilities import detect
from zamboni.committer import UnsupportedPyIceberg

# Longer than truncate(16), with U+D7FF as the 16th code point.
POISON = "a" * 15 + "\ud7ff" + "tail"
POISON_UPPER = ("a" * 15 + "\ue000").encode()

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
)
ARROW = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())])


def _stock_0_12(value: str, trunc_length: int | None) -> str | None:
    """``pyiceberg.utils.truncate.truncate_upper_bound_text_string`` as 0.12.0 ships it."""
    result = value[:trunc_length]
    if result != value:
        chars = [*result]

        for i in range(-1, -len(result) - 1, -1):
            try:
                to_inc = ord(chars[i])
                # will raise exception if the highest unicode code is reached
                _next = chr(to_inc + 1)
                chars[i] = _next
                return "".join(chars)
            except ValueError:
                pass
        return None  # didn't find a valid upper bound
    return result


@pytest.fixture
def stock_0_12(monkeypatch):
    """0.12.0's function where PyIceberg defines it and where it looks it up.

    ``detect()`` is left alone: :func:`zamboni.truncate.install` must not
    consult it, and the tests below give it the wrong answer on purpose.
    """
    monkeypatch.setattr(library, "truncate_upper_bound_text_string", _stock_0_12)
    monkeypatch.setattr(pyarrow_io, "truncate_upper_bound_text_string", _stock_0_12)
    return capabilities._string_upper_bounds_encodable()


def _upper_bounds(tbl) -> set[bytes]:
    return {
        task.file.upper_bounds[2] for task in tbl.scan().plan_files() if 2 in task.file.upper_bounds
    }


def _rows(tbl) -> list[tuple[int, str]]:
    return sorted((row["id"], row["name"]) for row in tbl.scan().to_arrow().to_pylist())


def test_the_probe_answers_for_the_expected_build():
    """Fork from maintenance/2026-11-23: encodable. Stock 0.12.0: U+D800."""
    from tests.conftest import library_honours_spec_expected

    fork = library_honours_spec_expected()
    assert detect().string_upper_bounds_encodable is fork, (
        "the maintenance fork rounds a truncated string bound past the surrogate "
        "block (#303); stock 0.12.0 does not. This build answered the other way -- "
        "check which PyIceberg the leg resolved, and that the fork pin carries #303."
    )


def test_stock_0_12_is_the_defect(stock_0_12):
    assert stock_0_12 is False
    with pytest.raises(UnicodeEncodeError):
        _stock_0_12(*truncate.PROBE).encode("utf-8")


def test_a_compaction_rewrites_a_file_whose_bound_was_a_surrogate(stock_0_12, session):
    """Files written and then compacted on stock 0.12, through the session's install."""
    tbl = session.catalog.create_table(
        "db.poison", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(3):
        tbl.append(pa.table({"id": [i], "name": [POISON]}, schema=ARROW))
    before = _rows(session.table("db.poison"))

    result = TableCompactor(session, "db.poison", CompactionConfig()).execute()

    tbl = session.table("db.poison")
    assert result.rewritten_data_files == 3
    assert _rows(tbl) == before
    assert _upper_bounds(tbl) == {POISON_UPPER}


def test_without_the_mirror_that_compaction_fails(stock_0_12, session, monkeypatch):
    """What the test above would meet on stock 0.12 with no mirror: the rewrite cannot write."""
    tbl = session.catalog.create_table(
        "db.poison", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(3):
        tbl.append(pa.table({"id": [i], "name": [POISON]}, schema=ARROW))
    monkeypatch.setattr(pyarrow_io, "truncate_upper_bound_text_string", _stock_0_12)

    with pytest.raises(UnicodeEncodeError):
        TableCompactor(session, "db.poison", CompactionConfig()).execute()


def test_the_session_installs_it(stock_0_12, session):
    assert pyarrow_io.truncate_upper_bound_text_string is truncate.truncate_upper_bound_text_string


def test_install_is_idempotent(stock_0_12):
    truncate.install()
    truncate.install()
    assert pyarrow_io.truncate_upper_bound_text_string is truncate.truncate_upper_bound_text_string


def test_install_refuses_a_foreign_replacement(stock_0_12, monkeypatch):
    """Something else replaced the name and still yields a surrogate."""

    def foreign(value, trunc_length):
        return value[:trunc_length] + "\ud800"

    monkeypatch.setattr(pyarrow_io, "truncate_upper_bound_text_string", foreign)

    with pytest.raises(UnsupportedPyIceberg, match="not the stock function"):
        truncate.install()
    assert pyarrow_io.truncate_upper_bound_text_string is foreign


def _fixed(value, trunc_length):
    """Another library's correct mirror, holding the writer's name."""
    return truncate.truncate_upper_bound_text_string(value, trunc_length)


def test_install_stands_down_where_the_writer_already_gets_encodable_bounds(
    stock_0_12, monkeypatch
):
    """A working replacement is left alone, even after detect() answered for stock (p3)."""
    monkeypatch.setattr(
        capabilities, "detect", lambda: replace(detect(), string_upper_bounds_encodable=False)
    )
    monkeypatch.setattr(pyarrow_io, "truncate_upper_bound_text_string", _fixed)

    truncate.install()

    assert pyarrow_io.truncate_upper_bound_text_string is _fixed


def test_a_cached_true_does_not_stand_down_over_the_stock_function(stock_0_12, monkeypatch):
    """A probe cache another process wrote while something else held the name (p1, p2)."""
    monkeypatch.setattr(
        capabilities, "detect", lambda: replace(detect(), string_upper_bounds_encodable=True)
    )

    truncate.install()

    assert pyarrow_io.truncate_upper_bound_text_string is truncate.truncate_upper_bound_text_string


def test_install_refuses_a_foreign_replacement_that_raises(stock_0_12, monkeypatch):
    def raising(value, trunc_length):
        raise TypeError(value)

    monkeypatch.setattr(pyarrow_io, "truncate_upper_bound_text_string", raising)

    with pytest.raises(UnsupportedPyIceberg, match="not the stock function"):
        truncate.install()


def test_the_live_check_asks_the_function_the_writer_calls(stock_0_12, monkeypatch):
    """A fixed definition the writer never looks up does not stand the mirror down."""
    monkeypatch.setattr(
        library, "truncate_upper_bound_text_string", truncate.truncate_upper_bound_text_string
    )
    assert truncate.writer_bounds_are_encodable() is False

    monkeypatch.setattr(pyarrow_io, "truncate_upper_bound_text_string", _fixed)
    monkeypatch.setattr(library, "truncate_upper_bound_text_string", _stock_0_12)
    assert truncate.writer_bounds_are_encodable() is True

    monkeypatch.setattr(
        pyarrow_io, "truncate_upper_bound_text_string", truncate.truncate_upper_bound_text_string
    )
    assert truncate.writer_bounds_are_encodable() is False, "observed the mirror"


def test_the_capability_describes_the_build_not_the_process(stock_0_12, monkeypatch):
    """What the cache stores: the library's definition, whatever holds the writer's name."""
    monkeypatch.setattr(pyarrow_io, "truncate_upper_bound_text_string", _fixed)
    assert capabilities._string_upper_bounds_encodable() is False


def test_the_bound_skips_the_surrogate_block():
    fixed = truncate.truncate_upper_bound_text_string
    assert fixed("aaaa", 2) == "ab"
    assert fixed("ab", 2) == "ab"
    assert fixed("ab", None) == "ab"
    assert fixed("\U0010ffff\U0010ffff\x00", 2) is None
    assert fixed("\ud7ff\ud7ffa", 2) == "\ud7ff\ue000"
    assert fixed("\ud7ff\U0010ffffa", 2) == "\ue000\U0010ffff"


def test_every_bound_encodes_and_bounds_its_value():
    """Around every boundary: the bound is UTF-8 and sorts at or above its value."""
    points = [
        0x0,
        0x41,
        0x7F,
        0x80,
        0x7FF,
        0x800,
        0xD7FE,
        0xD7FF,
        0xE000,
        0xFFFF,
        0x10000,
        0x10FFFF,
    ]
    rng = random.Random(303)
    for _ in range(20_000):
        value = "".join(chr(rng.choice(points)) for _ in range(rng.randint(1, 6)))
        upper = truncate.truncate_upper_bound_text_string(value, rng.randint(1, 5))
        if upper is not None:
            assert upper.encode() >= value.encode(), (value, upper)
