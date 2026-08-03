"""Byte formatting, in one place.

This existed four times -- in `orphans`, `profile`, `deletes` and
`himsdemo.stats` -- and the copies had already diverged: three stopped at GiB
while one went to TiB, so the same byte count formatted differently depending on
which module happened to report it. Type checking flagged one copy, which is how
the drift surfaced; fixing one of four is how it would have continued.
"""

from __future__ import annotations

UNITS = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")


def human_bytes(n: int | float) -> str:
    """A size for a human, e.g. ``512B``, ``7.0KiB``, ``1.2TiB``.

    Whole bytes get no decimal place because ``512.0B`` reads as false
    precision; everything above does, because ``7KiB`` loses information the
    reader wants.
    """
    value = float(n)
    for unit in UNITS:
        if abs(value) < 1024 or unit == UNITS[-1]:
            return f"{value:.0f}{unit}" if unit == "B" else f"{value:.1f}{unit}"
        value /= 1024.0
    raise AssertionError("unreachable: the loop returns on the last unit")
