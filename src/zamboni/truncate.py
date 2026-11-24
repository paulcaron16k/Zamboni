# SPDX-License-Identifier: Apache-2.0
"""A truncated string upper bound PyIceberg 0.12 can serialize (ZMBNI-303).

A data file's string column statistics are truncated to 16 code points by
default (``truncate(16)``), and the upper bound is then rounded up by
incrementing its last code point that can be. PyIceberg 0.12's
``truncate_upper_bound_text_string`` increments U+D7FF to U+D800, a lone
surrogate, which ``chr()`` accepts and UTF-8 cannot encode, so serializing the
bound raises ``UnicodeEncodeError`` and the write fails. A compaction writes
through ``_dataframe_to_data_files`` and the same statistics path, so a file
group whose string maximum has U+D7FF at the end of its truncated prefix could
never be rewritten: every run failed on it.

The maintenance fork fixes this in the library (tag ``maintenance/2026-11-23``);
``pip install iceberg-zamboni`` gets stock PyIceberg, so the same behaviour is
carried here: the increment steps over the surrogate block to U+E000. UTF-8 byte
order follows code point order over scalar values, so the result is still an
upper bound. target-iceberg carries the same mirror (ELT-1472).

There is no subclass seam: ``StatsAggregator`` is built inside
``data_file_statistics_from_parquet_metadata``, deep in every write. The only
seam is the name ``pyiceberg.io.pyarrow.truncate_upper_bound_text_string``,
which ``StatsAggregator.max_as_bytes`` looks up on each call. :func:`install`
replaces it, refuses rather than override a name that is neither the stock
function nor this module's, and stands down where
:attr:`~zamboni.capabilities.PyIcebergCapabilities.string_upper_bounds_encodable`
holds: the function that name holds already returns an encodable bound.
**Deletes entirely**, probe included, when a release carries the fix.
"""

from __future__ import annotations

from zamboni.committer import UnsupportedPyIceberg

_MIN_SURROGATE = 0xD800
_MAX_SURROGATE = 0xDFFF
_MAX_CODE_POINT = 0x10FFFF

#: A truncated prefix ending in U+D7FF, which 0.12 rounds up to U+D800. Read by
#: :func:`zamboni.capabilities._string_upper_bounds_encodable`.
PROBE = ("퟿퟿a", 2)


def truncate_upper_bound_text_string(value: str, trunc_length: int | None) -> str | None:
    """Truncate ``value`` and round it up to an encodable upper bound, as the fork does.

    Returns the value itself when it is no longer than ``trunc_length``;
    otherwise its prefix with its last incrementable code point incremented,
    skipping the surrogate block; ``None`` when every code point in the prefix
    is U+10FFFF, so no upper bound exists.
    """
    result = value[:trunc_length]
    if result == value:
        return result
    chars = [*result]
    for i in range(len(chars) - 1, -1, -1):
        following = ord(chars[i]) + 1
        if _MIN_SURROGATE <= following <= _MAX_SURROGATE:
            following = _MAX_SURROGATE + 1
        if following <= _MAX_CODE_POINT:
            chars[i] = chr(following)
            return "".join(chars)
    return None


def library_bounds_are_encodable() -> bool:
    """Whether the installed PyIceberg already rounds a bound up past the surrogate block.

    Asked of the library's behaviour by :func:`zamboni.capabilities.detect`, and
    cached with the other probes.
    """
    from .capabilities import detect

    return detect().string_upper_bounds_encodable


def install() -> None:
    """Route PyIceberg's string upper bounds through this module's truncation.

    Calling it again is harmless.

    Raises:
        UnsupportedPyIceberg: If the name ``pyiceberg.io.pyarrow`` looks up is
            neither the stock function nor this module's and still returns a
            bound that cannot be encoded, so whatever replaced it would be
            silently overridden.
    """
    if library_bounds_are_encodable():
        return

    from pyiceberg.io import pyarrow as pyarrow_io
    from pyiceberg.utils import truncate

    current = pyarrow_io.truncate_upper_bound_text_string
    if current not in {truncate.truncate_upper_bound_text_string, truncate_upper_bound_text_string}:
        import pyiceberg

        raise UnsupportedPyIceberg(
            f"pyiceberg {pyiceberg.__version__}: cannot install the truncated upper "
            f"bound fix (ZMBNI-303), because "
            f"pyiceberg.io.pyarrow.truncate_upper_bound_text_string is "
            f"{getattr(current, '__qualname__', current)!r}, not the stock function, "
            f"and replacing it would discard whatever put it there"
        )

    pyarrow_io.truncate_upper_bound_text_string = truncate_upper_bound_text_string
