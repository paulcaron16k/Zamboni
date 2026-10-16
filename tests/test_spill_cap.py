# SPDX-License-Identifier: Apache-2.0
"""DuckDB's spill, capped (ZMBNI-186).

DuckDB caps its spill at 90% of the free disk *it can see*. On Kubernetes that
is the node's disk, not the spill volume's `sizeLimit`, so an oversized sorted
rewrite spills past the limit and the kubelet evicts the pod -- which in IWS
also serves the API. `max_temp_directory_bytes` turns that into a refusal for
one table.

The rewrites here spill for real: the session's DuckDB gets a memory limit
small enough that sorting a few hundred thousand rows has to.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from zamboni import CatalogSession, CompactionConfig, MemoryMode, TableCompactor
from zamboni.backends.base import RewriteContext, SpillLimitExceeded
from zamboni.backends.duckdb_arrow import DuckDBArrowBackend
from zamboni.workdir import WorkspaceUnavailable

from .conftest import ARROW_SCHEMA, SCHEMA

MiB = 1024 * 1024

#: Small enough that sorting `ROWS` rows spills, large enough to pin a block:
#: 16 MiB fails with "failed to pin block" before it spills at all (DuckDB 1.5.5).
MEMORY = 64 * MiB
#: Rows per file, two files per table: about 2 x 4.7 MB of Parquet, measured.
ROWS = 300_000


def setting(con, name: str):
    return con.execute(f"select current_setting('{name}')").fetchone()[0]


def has_spilled(con, elsewhere: Path) -> bool:
    """Whether this connection has used its spill directory.

    DuckDB removes its spill files when a query ends, so there is nothing left
    to look at. What it leaves is the refusal this module's second half is
    about: a connection that has spilled will not move its directory.
    """
    current = setting(con, "temp_directory")
    try:
        con.execute(f"SET temp_directory = '{elsewhere}'")
    except duckdb.NotImplementedException:
        return True
    con.execute(f"SET temp_directory = '{current}'")
    return False


@pytest.fixture
def spilling(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    session = CatalogSession.for_local(
        warehouse_path=str(warehouse), threads=1, memory_limit_bytes=MEMORY
    )
    session.catalog.create_namespace("db")
    yield session
    session.close()


def digest(i: int) -> str:
    return hashlib.md5(str(i).encode()).hexdigest() * 2


def make_table(session: CatalogSession, name: str):
    tbl = session.catalog.create_table(
        f"db.{name}", schema=SCHEMA, properties={"format-version": "2"}
    )
    for part in range(2):
        ids = range(part * ROWS, (part + 1) * ROWS)
        tbl.append(
            pa.table(
                {
                    "id": pa.array(ids, type=pa.int32()),
                    "category": pa.array(["a"] * ROWS),
                    # Hashes, so the sort genuinely reorders everything.
                    "payload": pa.array([digest(i) for i in ids]),
                },
                schema=ARROW_SCHEMA,
            )
        )
    return tbl


def sorted_config(tmp_path: Path, mode: MemoryMode, cap: int | None) -> CompactionConfig:
    return CompactionConfig(
        memory_mode=mode,
        sort_expression="payload",
        temp_directory=str(tmp_path / "spill"),
        max_temp_directory_bytes=cap,
    )


def files_on_disk(tbl) -> set[str]:
    data = Path(tbl.location().removeprefix("file://")) / "data"
    return {str(p) for p in data.rglob("*.parquet")}


# -- the setting -------------------------------------------------------------


@pytest.mark.parametrize("value", [0, -1])
def test_a_non_positive_cap_is_refused(value):
    with pytest.raises(ValueError, match="max_temp_directory_bytes"):
        CompactionConfig(max_temp_directory_bytes=value)


def test_the_cap_reaches_duckdb_and_does_not_outlive_its_rewrite(session, unpartitioned):
    """Read back from DuckDB. And reset, not left: the connection belongs to the
    session and outlives this rewrite, so a cap one table set would otherwise
    bind every table after it."""
    backend = DuckDBArrowBackend(session.con)
    default = setting(duckdb.connect(), "max_temp_directory_size")

    capped = CompactionConfig(max_temp_directory_bytes=MiB)
    backend._configure_duckdb(RewriteContext(table=unpartitioned, config=capped))
    assert setting(session.con, "max_temp_directory_size") == "1.0 MiB"

    backend._configure_duckdb(RewriteContext(table=unpartitioned, config=CompactionConfig()))
    assert setting(session.con, "max_temp_directory_size") == default


# -- a rewrite over the cap --------------------------------------------------


@pytest.mark.parametrize("mode", [MemoryMode.CHUNKED, MemoryMode.IN_MEMORY])
def test_a_rewrite_over_the_cap_is_refused_and_cleaned_up(spilling, tmp_path, mode):
    tbl = make_table(spilling, "t")
    snapshot = tbl.metadata.current_snapshot_id
    on_disk = files_on_disk(tbl)

    with pytest.raises(SpillLimitExceeded) as caught:
        TableCompactor(spilling, "db.t", sorted_config(tmp_path, mode, MiB)).execute()

    message = str(caught.value)
    assert "max_temp_directory_bytes=1,048,576" in message, "it names the cap it hit"
    assert "--max-temp-directory-bytes" in message, "and the flag that raises it"
    assert isinstance(caught.value.__cause__, duckdb.OutOfMemoryException)

    tbl = spilling.table("db.t")
    assert tbl.metadata.current_snapshot_id == snapshot, "nothing was committed"
    assert files_on_disk(tbl) == on_disk, "and nothing it wrote was left behind"


def test_without_the_cap_the_same_rewrite_spills_and_succeeds(spilling, tmp_path):
    """The other half of the test above: what the cap refused, DuckDB's own
    default allows -- so the refusal was the cap, not the data."""
    make_table(spilling, "t")
    TableCompactor(spilling, "db.t", sorted_config(tmp_path, MemoryMode.CHUNKED, None)).execute()

    assert has_spilled(spilling.con, tmp_path / "elsewhere"), "the rewrite never spilled"
    payload = spilling.table("db.t").scan().to_arrow()["payload"].to_pylist()
    assert payload == sorted(payload)


#: DuckDB 1.5.5's two out-of-memory errors, measured: one per limit.
OVER_THE_CAP = (
    "Out of Memory Error: failed to offload data block of size 96.0 KiB "
    "(15.2 MiB/15.2 MiB used).\nThis limit was set by the "
    "'max_temp_directory_size' setting."
)
OVER_MEMORY = "Out of Memory Error: failed to pin block of size 256.0 KiB (15.9 MiB/16.0 MiB used)"


@pytest.mark.parametrize(("message", "translated"), [(OVER_THE_CAP, True), (OVER_MEMORY, False)])
def test_only_the_spill_caps_out_of_memory_error_becomes_a_refusal(
    session, unpartitioned, monkeypatch, message, translated
):
    """`memory_limit` raises the same DuckDB class. That one is not the cap, and
    reporting it as the cap would send an operator to the wrong setting."""

    def fail(self, group, ctx):
        raise duckdb.OutOfMemoryException(message)

    monkeypatch.setattr(DuckDBArrowBackend, "_rewrite", fail)
    expected = SpillLimitExceeded if translated else duckdb.OutOfMemoryException

    with pytest.raises(expected):
        TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()


def test_a_group_that_fails_part_way_removes_the_chunks_it_wrote(
    session, unpartitioned, tmp_path, monkeypatch
):
    """The committer cleans up what a *returned* group wrote. A group that
    fails mid-stream never returns, so the backend removes its own chunks."""
    from zamboni.backends import duckdb_arrow

    on_disk = files_on_disk(unpartitioned)

    def two_chunks(reader, target_bytes):
        rows = reader.read_all()
        half = rows.num_rows // 2
        yield rows.slice(0, half)
        yield rows.slice(half)

    real_write = DuckDBArrowBackend._write
    calls = []

    def write_once(self, data, group, ctx):
        calls.append(1)
        if len(calls) > 1:
            raise RuntimeError("the store went away")
        return real_write(self, data, group, ctx)

    monkeypatch.setattr(duckdb_arrow, "_bin_pack", two_chunks)
    monkeypatch.setattr(DuckDBArrowBackend, "_write", write_once)

    config = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        sort_expression="id",
        temp_directory=str(tmp_path / "spill"),
    )
    with pytest.raises(RuntimeError, match="the store went away"):
        TableCompactor(session, "db.unpartitioned", config).execute()

    assert len(calls) == 2, "the first chunk was written before the failure"
    assert files_on_disk(session.table("db.unpartitioned")) == on_disk


# -- a session after a spill -------------------------------------------------


def test_a_spill_does_not_break_the_next_sorted_rewrite_on_the_session(spilling, tmp_path):
    """Found writing this story, and older than it. Once a connection has
    spilled, DuckDB refuses *any* `SET temp_directory`, even to the path it
    already has. The backend set it before every sorted rewrite, so in a run
    over many tables -- one session -- the first table to spill made every later
    sorted rewrite raise, and `maintain()` does not catch that."""
    config = sorted_config(tmp_path, MemoryMode.CHUNKED, None)
    for name in ("first", "second"):
        make_table(spilling, name)

    TableCompactor(spilling, "db.first", config).execute()
    assert has_spilled(spilling.con, tmp_path / "elsewhere"), "the precondition did not hold"

    TableCompactor(spilling, "db.second", config).execute()
    payload = spilling.table("db.second").scan().to_arrow()["payload"].to_pylist()
    assert payload == sorted(payload)


def test_moving_the_spill_directory_after_a_spill_is_a_workspace_refusal(spilling, tmp_path):
    """A caller that changes `temp_directory` between tables on one session gets
    a refusal naming both paths, which `maintain()` reports as exit 2 -- not
    DuckDB's NotImplementedException escaping the run."""
    make_table(spilling, "first")
    make_table(spilling, "second")
    TableCompactor(
        spilling, "db.first", sorted_config(tmp_path, MemoryMode.CHUNKED, None)
    ).execute()

    moved = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        sort_expression="payload",
        temp_directory=str(tmp_path / "moved"),
    )
    with pytest.raises(WorkspaceUnavailable) as caught:
        TableCompactor(spilling, "db.second", moved).execute()

    assert str(tmp_path / "spill") in str(caught.value)
    assert str(tmp_path / "moved") in str(caught.value)


# -- through maintain() --------------------------------------------------------


def test_a_table_over_the_cap_is_exit_3_and_the_next_table_still_runs(spilling, tmp_path):
    """The acceptance of ZMBNI-186: the cap turns an eviction into one blocked
    table. `big` needs more spill than the cap; `small` sorts in memory and must
    be compacted in the same run, on the same session, after the refusal."""
    import json

    from zamboni import Operation, maintain

    from .conftest import batch

    big = make_table(spilling, "big")
    big_snapshot = big.metadata.current_snapshot_id
    small = spilling.catalog.create_table(
        "db.small", schema=SCHEMA, properties={"format-version": "2"}
    )
    for i in range(3):
        small.append(batch(i * 10, 10))

    sorted_by_payload = {"ordering": {"mode": "sort", "sort": [{"column": "payload"}]}}
    path = tmp_path / "table-config.json"
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "namespaces": {
                    "db": {"tables": {"big": sorted_by_payload, "small": sorted_by_payload}}
                },
            }
        )
    )

    report = maintain(
        spilling,
        table_config=path,
        operations=[Operation.COMPACT],
        commit=True,
        base_config=CompactionConfig(
            memory_mode=MemoryMode.CHUNKED,
            temp_directory=str(tmp_path / "spill"),
            max_temp_directory_bytes=MiB,
        ),
    )

    outcome = {o.table: o for o in report.outcomes}
    assert outcome["db.big"].exit_code == 3, outcome["db.big"].detail
    assert "max_temp_directory_bytes" in (outcome["db.big"].detail or "")
    assert spilling.table("db.big").metadata.current_snapshot_id == big_snapshot

    assert outcome["db.small"].exit_code == 0, outcome["db.small"].detail
    assert outcome["db.small"].result is not None, "the next table was not compacted"
    assert len(list(spilling.table("db.small").scan().plan_files())) == 1
    assert report.exit_code == 3


@pytest.mark.parametrize(
    ("env", "flags", "expected"),
    [
        (None, [], None),
        ("1048576", [], MiB),
        ("1048576", ["--max-temp-directory-bytes", "2097152"], 2 * MiB),
    ],
)
def test_the_cap_reaches_the_run_from_the_environment_or_the_flag(
    monkeypatch, tmp_path, env, flags, expected
):
    """A `serve` worker is a `zamboni maintenance` subprocess, so the pod sets
    the cap once in its environment, beside the volume it is sized from. The
    flag still wins for a one-off run."""
    from zamboni.cli import _build_parser, _operational_config

    if env is None:
        monkeypatch.delenv("ZAMBONI_MAX_TEMP_DIRECTORY_BYTES", raising=False)
    else:
        monkeypatch.setenv("ZAMBONI_MAX_TEMP_DIRECTORY_BYTES", env)

    args = _build_parser().parse_args(["maintenance", "--local-warehouse", str(tmp_path), *flags])
    assert _operational_config(args).max_temp_directory_bytes == expected


def test_a_non_positive_cap_in_the_environment_is_a_usage_error(monkeypatch, tmp_path):
    from zamboni.cli import _build_parser

    monkeypatch.setenv("ZAMBONI_MAX_TEMP_DIRECTORY_BYTES", "0")
    with pytest.raises(SystemExit) as exc:
        _build_parser().parse_args(["maintenance", "--local-warehouse", str(tmp_path)])
    assert exc.value.code == 2
