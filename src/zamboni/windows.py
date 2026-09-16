# SPDX-License-Identifier: Apache-2.0
"""When a time partition's window closes.

Two operations need this and must agree: partition evolution refuses to condense
a partition until every row it could contain has aged past the rule's floor, and
compaction refuses to rewrite one that a loader may still be writing into. Both
turn "how old is this partition" into the same question -- *has its window
ended* -- and answering it twice would let the two drift into meaning different
things under the same configured number of days.

The answer is deliberately the **end** of the window rather than its start. A
`day=2026-09-14` partition is not 1 day old the moment 2026-09-15 begins; rows
timestamped 23:59 on the 14th are still arriving. Measuring from the end is what
makes both callers conservative.
"""

from __future__ import annotations

import datetime as dt

EPOCH = dt.date(1970, 1, 1)

#: Partition transforms that divide time into windows, coarsest last. Anything
#: else -- `identity`, `bucket`, `truncate` -- has no window and cannot be aged.
TEMPORAL = ("hour", "day", "month", "year")


def transform_name(transform) -> str:
    """The transform's configured name, e.g. ``day``."""
    return str(transform)


def is_temporal(transform) -> bool:
    return transform_name(transform) in TEMPORAL


def window_end(granularity: str, value: int) -> dt.date:
    """The first date *after* the partition window, so ageing is conservative."""
    if granularity == "hour":
        return (dt.datetime(1970, 1, 1) + dt.timedelta(hours=value + 1)).date()
    if granularity == "day":
        return EPOCH + dt.timedelta(days=value + 1)
    if granularity == "month":
        year, month = divmod(value, 12)
        return add_month(dt.date(1970 + year, month + 1, 1))
    if granularity == "year":
        return dt.date(1970 + value + 1, 1, 1)
    raise ValueError(f"unsupported granularity {granularity!r}")


def current_index(granularity: str, now: dt.datetime) -> int:
    """Which window ``now`` falls in, on the same scale Iceberg stores.

    A temporal partition value *is* its window index -- ``day`` is days since the
    epoch, ``month`` is months since 1970-01, and so on -- so comparing a stored
    partition value against this needs no date arithmetic at the call site, and
    no timezone handling: both sides are integers on the same axis.
    """
    if granularity == "hour":
        return int((now - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)).total_seconds() // 3600)
    if granularity == "day":
        return (now.date() - EPOCH).days
    if granularity == "month":
        return (now.year - 1970) * 12 + (now.month - 1)
    if granularity == "year":
        return now.year - 1970
    raise ValueError(f"unsupported granularity {granularity!r}")


def windows_since_close(granularity: str, value: int, now: dt.datetime) -> int:
    """How many whole windows have elapsed since this partition's window closed.

    ``-1`` for the window still open. ``0`` for the one that closed most
    recently -- yesterday, on a daily table -- which has closed but whose data
    may still be arriving, because a loader extracting yesterday at 02:00 writes
    into yesterday's partition, not today's. That is why a floor of 1 holds two
    partitions rather than one.
    """
    return current_index(granularity, now) - value - 1


def add_month(d: dt.date) -> dt.date:
    return dt.date(d.year + 1, 1, 1) if d.month == 12 else dt.date(d.year, d.month + 1, 1)
