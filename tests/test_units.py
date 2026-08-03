"""The shared byte formatter.

It existed in four copies that had already diverged -- three capped at GiB, one
went to TiB -- so the same byte count formatted differently depending on which
module reported it. Consolidating changed behaviour for three of the four call
sites, which is why it now has tests.
"""

from __future__ import annotations

import pytest

from zamboni.units import human_bytes


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, "0B"),
        (1, "1B"),
        (512, "512B"),
        (1023, "1023B"),
        (1024, "1.0KiB"),
        (1536, "1.5KiB"),
        (1024**2, "1.0MiB"),
        (1024**3, "1.0GiB"),
        # Above GiB is the range where the old copies disagreed.
        (1024**4, "1.0TiB"),
        (1024**5, "1.0PiB"),
    ],
)
def test_formats_each_magnitude(value, expected):
    assert human_bytes(value) == expected


def test_bytes_get_no_false_decimal():
    """`512.0B` claims precision that a byte count does not have."""
    assert "." not in human_bytes(512)
    assert "." in human_bytes(1024)


def test_beyond_the_largest_unit_stays_in_that_unit():
    """The old TiB copy fell through to a bare `f"{n}B"`, mislabelling the unit."""
    huge = 1024**7
    result = human_bytes(huge)
    assert result.endswith("PiB")
    assert "e+" not in result, f"scientific notation leaked into {result}"


def test_negative_sizes_do_not_recurse_forever():
    """A delta can be negative; `abs` is what makes the loop terminate."""
    assert human_bytes(-1024) == "-1.0KiB"
    assert human_bytes(-512) == "-512B"


def test_accepts_a_float_without_lying_about_its_type():
    """One of the four copies declared `int` and then divided in place, so its
    annotation was false for every call above 1 KiB."""
    assert human_bytes(1536.0) == "1.5KiB"
