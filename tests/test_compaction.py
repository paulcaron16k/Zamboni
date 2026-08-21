"""End-to-end compaction behaviour on a local SQL catalog."""

from __future__ import annotations

from dataclasses import replace

import pytest

from zamboni import CompactionConfig, MemoryMode, TableCompactor
from zamboni.profile import profile_table


def live_paths(tbl) -> set[str]:
    from pyiceberg.manifest import DataFileContent

    tbl.refresh()
    snapshot = tbl.current_snapshot()
    paths: list[str] = []
    for manifest in snapshot.manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            if entry.data_file.content == DataFileContent.DATA:
                paths.append(entry.data_file.file_path)
    # A file appearing twice in the live set is the signature of a manifest
    # that was kept verbatim when it should have been rewritten.
    assert len(paths) == len(set(paths)), "a data file is live more than once"
    return set(paths)


def rows(tbl) -> list[tuple]:
    tbl.refresh()
    table = tbl.scan().to_arrow().sort_by("id")
    return list(
        zip(
            table["id"].to_pylist(),
            table["category"].to_pylist(),
            table["payload"].to_pylist(),
            strict=True,
        )
    )


@pytest.mark.parametrize("mode", [MemoryMode.IN_MEMORY, MemoryMode.CHUNKED])
def test_unpartitioned_compaction_preserves_data(session, unpartitioned, mode):
    before_rows = rows(unpartitioned)
    before_files = live_paths(unpartitioned)
    assert len(before_files) == 6

    compactor = TableCompactor(
        session,
        "db.unpartitioned",
        CompactionConfig(memory_mode=mode, target_file_size_bytes=64 * 1024 * 1024),
    )
    result = compactor.execute()

    assert result.rewritten_data_files == 6
    assert result.added_data_files >= 1

    tbl = session.table("db.unpartitioned")
    assert rows(tbl) == before_rows
    after_files = live_paths(tbl)
    assert len(after_files) < len(before_files)
    assert not (after_files & before_files), "compacted output reused a source path"


def test_commit_uses_replace_operation(session, unpartitioned):
    TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()

    tbl = session.table("db.unpartitioned")
    snapshot = tbl.current_snapshot()
    assert snapshot.summary.operation.value == "replace"
    assert snapshot.summary["zamboni.operation"] == "compaction"


def test_partitioned_compaction_is_partition_scoped(session, partitioned):
    before_rows = rows(partitioned)
    profile = profile_table(partitioned)
    assert len(profile.live_files) == 8  # 4 appends x 2 partitions

    result = TableCompactor(session, "db.partitioned", CompactionConfig()).execute()
    assert result.rewritten_data_files == 8
    assert len(result.groups) == 2  # one per partition

    tbl = session.table("db.partitioned")
    assert rows(tbl) == before_rows

    after = profile_table(tbl)
    assert len(after.live_files) == 2
    # Each output file must carry exactly one partition value.
    assert {tuple(f.partition) for f in after.live_files} == {("a",), ("b",)}


def test_the_rust_core_is_installed_however_it_gets_here():
    """`pyiceberg-core` must be present; which side declares it moved in 0.12.

    Non-negotiable half: `pyarrow_transform` -- called for every partition field
    when writing a partitioned table -- delegates to the Rust core for **six**
    transforms. Without it, `bucket`, `truncate`, `year`, `month`, `day` and
    `hour` partitioned writes all raise `NotInstalledError` from inside
    PyIceberg's own writer. So the import has to work, on any line.

    The half that moved: on 0.11.1 `pyiceberg[pyarrow]` requires the core, and
    this test used to assert exactly that, plus that no extra of ours re-declares
    it (ZMBNI-1815 deleted a `bucket` extra that installed what the base install
    already had). **0.12 moved the core out of `[pyarrow]` into an extra of its
    own** while keeping `pyarrow_transform`'s dependency on it, so on 0.12 the
    old assertion is false and `pyiceberg[pyarrow]` alone silently cannot write a
    transformed partition.

    That is this test doing its job rather than breaking: its previous docstring
    said it "fails if upstream moves the dependency out of `[pyarrow]`, rather
    than leaving a user to discover it by writing a table", and that is precisely
    how the 0.12 requirement was found. What it asserts now is the invariant that
    survives the move -- the core is installed, and *somebody* declares it -- with
    the two acceptable providers named, so a build where neither declares it still
    fails.
    """
    import importlib.util
    import tomllib
    from importlib.metadata import requires
    from pathlib import Path

    from packaging.requirements import Requirement

    assert importlib.util.find_spec("pyiceberg_core") is not None, (
        "pyiceberg-core is not installed, so bucket/day/month/year/hour/truncate "
        "partitioned writes cannot work. On 0.11.1 it comes from pyiceberg's "
        "[pyarrow] extra; on 0.12 it must be requested explicitly"
    )

    def is_core(requirement: str) -> bool:
        return Requirement(requirement).name.lower().replace("_", "-") == "pyiceberg-core"

    upstream = [r for r in (requires("pyiceberg") or []) if is_core(r)]
    supplied_by_pyarrow_extra = any('extra == "pyarrow"' in r for r in upstream)

    project = tomllib.loads((Path(__file__).parent.parent / "pyproject.toml").read_text())
    ours = [d for d in project["project"]["dependencies"] if "pyiceberg-core" in d]

    assert supplied_by_pyarrow_extra or ours, (
        "nothing declares pyiceberg-core: pyiceberg[pyarrow] does not supply it on "
        "this build and this project does not ask for it either, so the install that "
        "works here is an accident of what else was resolved"
    )

    if supplied_by_pyarrow_extra:
        # The 0.11.1 shape. Re-declaring what the base install already brings is
        # what ZMBNI-1815 removed, so keep that closed.
        for name, deps in project["project"]["optional-dependencies"].items():
            assert not any("pyiceberg-core" in dep for dep in deps), (
                f"extra {name!r} declares pyiceberg-core, which the base install already has"
            )


def test_bucket_partitioned_table_compacts(session, bucketed):
    """The transform that defeats PyIceberg's add_files-based partition inference.

    We go through _dataframe_to_data_files, which derives the partition key from
    the data rather than from column statistics, so bucket works.
    """
    before_rows = rows(bucketed)
    before = profile_table(bucketed)
    # 4 appends x 2 buckets: every append straddles both partitions.
    assert len(before.live_files) == 8
    assert {tuple(f.partition) for f in before.live_files} == {(0,), (1,)}

    result = TableCompactor(session, "db.bucketed", CompactionConfig()).execute()
    assert result.rewritten_data_files == 8
    assert len(result.groups) == 2  # one per bucket

    tbl = session.table("db.bucketed")
    assert rows(tbl) == before_rows

    after = profile_table(tbl)
    assert len(after.live_files) == 2
    assert {tuple(f.partition) for f in after.live_files} == {(0,), (1,)}


def test_sort_expression_orders_output_but_never_stamps_sort_order(session, unpartitioned):
    """An arbitrary order is applied, but not claimed as the table's order.

    Stamping the table's sort_order_id here would assert the data satisfies an
    order it was never sorted by -- the same defect duckdb-iceberg's native
    compactor has.
    """
    config = CompactionConfig(sort_expression="id DESC", memory_mode=MemoryMode.IN_MEMORY)
    TableCompactor(session, "db.unpartitioned", config).execute()

    tbl = session.table("db.unpartitioned")
    after = profile_table(tbl)
    assert len(after.live_files) == 1

    ids = tbl.scan().to_arrow()["id"].to_pylist()
    assert ids == sorted(ids, reverse=True)
    assert after.live_files[0].data_file.sort_order_id is None


def test_sort_by_table_order_applies_and_stamps_the_declared_order(session, sorted_table):
    config = CompactionConfig(sort_by_table_order=True, memory_mode=MemoryMode.IN_MEMORY)
    TableCompactor(session, "db.sorted", config).execute()

    tbl = session.table("db.sorted")
    after = profile_table(tbl)
    assert len(after.live_files) == 1

    # Declared order is: id DESC NULLS LAST.
    ids = tbl.scan().to_arrow()["id"].to_pylist()
    assert ids == sorted(ids, reverse=True)

    declared = tbl.sort_order()
    assert declared.order_id != 0
    assert after.live_files[0].data_file.sort_order_id == declared.order_id


def test_sort_by_table_order_refuses_an_unsorted_table(session, unpartitioned):
    config = CompactionConfig(sort_by_table_order=True)
    with pytest.raises(ValueError, match="declares no sort order"):
        TableCompactor(session, "db.unpartitioned", config).execute()


def test_sort_options_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        CompactionConfig(sort_by_table_order=True, sort_expression="id")


def test_chunked_mode_splits_output_by_target_size(session, partitioned):
    """A tiny target must produce several output files per partition.

    This is the bin-packing that bounds memory: without it, CHUNKED would
    accumulate the whole partition before writing.
    """
    before_rows = rows(partitioned)
    config = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        target_file_size_bytes=200,  # bytes, so each chunk holds a few rows
        rewrite_all=True,
    )
    TableCompactor(session, "db.partitioned", config).execute()

    tbl = session.table("db.partitioned")
    assert rows(tbl) == before_rows

    after = profile_table(tbl)
    assert len(after.live_files) > 2, "chunking did not split the output"
    assert {tuple(f.partition) for f in after.live_files} == {("a",), ("b",)}


def test_chunked_mode_with_sort_routes_through_duckdb(session, tmp_path, partitioned):
    config = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        sort_expression="id DESC",
        temp_directory=str(tmp_path / "spill"),
    )
    TableCompactor(session, "db.partitioned", config).execute()

    tbl = session.table("db.partitioned")
    for category in ("a", "b"):
        ids = tbl.scan(row_filter=f"category == '{category}'").to_arrow()["id"].to_pylist()
        assert ids == sorted(ids, reverse=True)


def test_no_sort_expression_leaves_sort_order_unset(session, unpartitioned):
    TableCompactor(session, "db.unpartitioned", CompactionConfig()).execute()
    after = profile_table(session.table("db.unpartitioned"))
    assert all(f.data_file.sort_order_id is None for f in after.live_files)


# -- atomic vs partial-progress commits ----------------------------------


def snapshot_ids(tbl) -> list[int]:
    return [s.snapshot_id for s in tbl.metadata.snapshots]


def test_a_multi_partition_rewrite_commits_once_by_default(session, partitioned):
    """Iceberg's default, and now ours: "a single commit when the entire job has
    completed". Two partitions previously produced two snapshots."""
    before = len(snapshot_ids(partitioned))
    rows_before = sorted(partitioned.scan().to_arrow()["id"].to_pylist())

    result = TableCompactor(session, "db.partitioned", CompactionConfig()).execute()

    tbl = session.table("db.partitioned")
    assert len(result.groups) == 2, "expected one group per partition"
    assert len(snapshot_ids(tbl)) == before + 1, "more than one snapshot for one run"
    # Every group reports the same snapshot, because there was only one.
    assert len({g.snapshot_id for g in result.groups}) == 1
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == rows_before


def test_partial_progress_commits_each_group(session, partitioned):
    before = len(snapshot_ids(partitioned))
    rows_before = sorted(partitioned.scan().to_arrow()["id"].to_pylist())

    result = TableCompactor(
        session, "db.partitioned", CompactionConfig(partial_progress=True)
    ).execute()

    tbl = session.table("db.partitioned")
    assert len(snapshot_ids(tbl)) == before + 2, "expected one snapshot per group"
    assert len({g.snapshot_id for g in result.groups}) == 2
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == rows_before


def test_an_atomic_run_that_fails_leaves_the_table_untouched(session, partitioned, monkeypatch):
    """The property the default buys: all or nothing.

    The second group's rewrite fails, so the first group's work must not be
    visible either -- which is the difference from partial progress.
    """
    from zamboni import compactor as compactor_module

    real = compactor_module.DuckDBArrowBackend.rewrite
    calls = {"n": 0}

    def fail_on_second(self, group, ctx):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return real(self, group, ctx)

    monkeypatch.setattr(compactor_module.DuckDBArrowBackend, "rewrite", fail_on_second)

    before = snapshot_ids(partitioned)
    rows_before = sorted(partitioned.scan().to_arrow()["id"].to_pylist())

    with pytest.raises(RuntimeError, match="boom"):
        TableCompactor(session, "db.partitioned", CompactionConfig()).execute()

    tbl = session.table("db.partitioned")
    assert snapshot_ids(tbl) == before, "a failed atomic run committed something"
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == rows_before


def test_partial_progress_keeps_what_already_committed(session, partitioned, monkeypatch):
    """The other side of the trade, asserted rather than assumed.

    Per Iceberg this is not a correctness problem -- "file groups can be
    compacted independently" -- it is a predictability one, so the behaviour is
    worth pinning in both directions.
    """
    from zamboni import compactor as compactor_module

    real = compactor_module.DuckDBArrowBackend.rewrite
    calls = {"n": 0}

    def fail_on_second(self, group, ctx):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return real(self, group, ctx)

    monkeypatch.setattr(compactor_module.DuckDBArrowBackend, "rewrite", fail_on_second)

    before = len(snapshot_ids(partitioned))
    rows_before = sorted(partitioned.scan().to_arrow()["id"].to_pylist())

    with pytest.raises(RuntimeError, match="boom"):
        TableCompactor(session, "db.partitioned", CompactionConfig(partial_progress=True)).execute()

    tbl = session.table("db.partitioned")
    assert len(snapshot_ids(tbl)) == before + 1, "the first group's commit was lost"
    assert sorted(tbl.scan().to_arrow()["id"].to_pylist()) == rows_before, "rows changed"


def test_a_failed_atomic_run_leaves_no_referenced_file_missing(session, partitioned, monkeypatch):
    """Cleanup after a failed atomic run must not touch live files."""
    from zamboni import compactor as compactor_module
    from zamboni.orphans import list_storage, storage_roots
    from zamboni.reachable import reachable_files

    real = compactor_module.DuckDBArrowBackend.rewrite
    calls = {"n": 0}

    def fail_on_second(self, group, ctx):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return real(self, group, ctx)

    monkeypatch.setattr(compactor_module.DuckDBArrowBackend, "rewrite", fail_on_second)

    with pytest.raises(RuntimeError, match="boom"):
        TableCompactor(session, "db.partitioned", CompactionConfig()).execute()

    tbl = session.table("db.partitioned")
    referenced = reachable_files(tbl).paths
    on_disk = set(list_storage(tbl, storage_roots(tbl)))
    assert referenced <= on_disk, (
        f"{len(referenced - on_disk)} referenced file(s) were deleted by the cleanup"
    )


# -- bounded reads (ZMBNI-1906) -------------------------------------------


def test_chunked_reads_one_data_file_at_a_time(session, partitioned, monkeypatch):
    """The property that makes CHUNKED bound anything.

    Handing PyIceberg the whole task list buffers most of the group:
    `ArrowScan.to_record_batches` materialises each data file into a list and
    drives that with `executor.map`, which submits every task at once and
    returns results in order -- so tasks that finish early hold their whole file
    until the consumer catches up. Measured, peak memory scaled with the group;
    reading one task per call makes it scale with the largest *file*.

    Asserted on the call shape rather than on memory, because a memory
    assertion is a flaky assertion. If someone reverts to passing every task,
    this fails; if they keep the property while restructuring, it passes.

    Still one task per call after ZMBNI-1909 added a read-ahead window: the
    window changes how many of these calls are in flight, not how much each one
    is asked for. That distinction is the whole bound.
    """
    from pyiceberg.io.pyarrow import ArrowScan

    task_counts = []
    original = ArrowScan.to_record_batches

    def counting(self, tasks):
        tasks = list(tasks)
        task_counts.append(len(tasks))
        return original(self, tasks)

    monkeypatch.setattr(ArrowScan, "to_record_batches", counting)

    config = CompactionConfig(memory_mode=MemoryMode.CHUNKED, rewrite_all=True)
    TableCompactor(session, "db.partitioned", config).execute()

    assert task_counts, "the chunked path did not stream at all"
    assert set(task_counts) == {1}, (
        f"expected one task per read call, got {sorted(set(task_counts))} -- "
        "passing the whole list back to PyIceberg reinstates the buffering"
    )


def test_chunked_still_sorts_across_the_whole_group(session, partitioned):
    """Why this fix was preferred over capping group size.

    Bounding the *read* leaves the sort seeing every row in the group, because
    DuckDB consumes the stream and spills to disk. Capping the group would not:
    N sub-groups sort independently and produce N overlapping ranges, which is
    precisely what makes Z-order work less well. A per-file read that quietly
    sorted per file would have the same defect, so the ordering is asserted
    across the partition rather than within a file.
    """
    before_rows = rows(partitioned)
    config = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        sort_expression="id DESC",
        target_file_size_bytes=200,  # force several output files per partition
        rewrite_all=True,
    )
    TableCompactor(session, "db.partitioned", config).execute()

    tbl = session.table("db.partitioned")
    assert rows(tbl) == before_rows
    for category in ("a", "b"):
        ids = tbl.scan(row_filter=f"category == '{category}'").to_arrow()["id"].to_pylist()
        assert ids == sorted(ids, reverse=True), "the group was sorted in pieces, not as a whole"


def _concurrency_probe(monkeypatch):
    """Record the high-water mark of overlapping reads."""
    import threading

    from pyiceberg.io.pyarrow import ArrowScan

    state = {"live": 0, "peak": 0, "calls": 0}
    lock = threading.Lock()
    original = ArrowScan.to_record_batches

    def tracked(self, tasks):
        with lock:
            state["live"] += 1
            state["calls"] += 1
            state["peak"] = max(state["peak"], state["live"])
        try:
            # An iterator, not a list: `to_table` calls next() on the result,
            # and this probe also sits under that path.
            return iter(list(original(self, tasks)))
        finally:
            with lock:
                state["live"] -= 1

    monkeypatch.setattr(ArrowScan, "to_record_batches", tracked)
    return state


def test_read_ahead_zero_reads_strictly_one_file_at_a_time(session, partitioned, monkeypatch):
    """The ZMBNI-1906 behaviour, still reachable.

    It is the floor the window is measured against, and the setting to reach for
    on a host where even one extra file in flight is too much.
    """
    state = _concurrency_probe(monkeypatch)

    TableCompactor(
        session,
        "db.partitioned",
        CompactionConfig(memory_mode=MemoryMode.CHUNKED, rewrite_all=True, read_ahead_bytes=0),
    ).execute()

    assert state["calls"] > 1, "nothing was read; the probe is not on the read path"
    assert state["peak"] == 1, f"reads overlapped with the window disabled: {state['peak']}"


def test_read_ahead_overlaps_reads_without_unbounding_them(session, partitioned, monkeypatch):
    """The point of ZMBNI-1909: concurrency comes back, the bound does not go.

    Measured on object storage, serialising the reads cost 1.12x-1.39x as RTT
    rose 0 to 30ms, because it serialised the round trips too.
    """
    state = _concurrency_probe(monkeypatch)
    before_rows = rows(partitioned)

    TableCompactor(
        session,
        "db.partitioned",
        CompactionConfig(
            memory_mode=MemoryMode.CHUNKED,
            rewrite_all=True,
            read_ahead_bytes=64 * 1024 * 1024,  # far larger than these tiny files
            max_read_ahead_files=4,
        ),
    ).execute()

    assert state["peak"] > 1, "the window admitted nothing; reads stayed serial"
    assert state["peak"] <= 4, f"more files in flight than the cap allows: {state['peak']}"
    assert rows(session.table("db.partitioned")) == before_rows


def test_a_file_larger_than_the_window_still_makes_progress(session, partitioned):
    """A window smaller than one file must admit that file anyway.

    The obvious loop -- refuse to submit while queued bytes exceed the window --
    never submits anything when the first file is already over, and the run
    hangs rather than failing. One byte is the smallest way to say that.
    """
    before_rows = rows(partitioned)

    TableCompactor(
        session,
        "db.partitioned",
        CompactionConfig(memory_mode=MemoryMode.CHUNKED, rewrite_all=True, read_ahead_bytes=1),
    ).execute()

    assert rows(session.table("db.partitioned")) == before_rows


def test_read_ahead_preserves_row_order_across_files(session, partitioned):
    """Concurrent reads, ordered results.

    Futures are drained in submission order, so a fast third file cannot
    overtake a slow first one. Asserted through a sort, which would expose any
    reordering as a broken sequence.
    """
    config = CompactionConfig(
        memory_mode=MemoryMode.CHUNKED,
        sort_expression="id DESC",
        target_file_size_bytes=200,
        rewrite_all=True,
        read_ahead_bytes=64 * 1024 * 1024,
    )
    TableCompactor(session, "db.partitioned", config).execute()

    tbl = session.table("db.partitioned")
    for category in ("a", "b"):
        ids = tbl.scan(row_filter=f"category == '{category}'").to_arrow()["id"].to_pylist()
        assert ids == sorted(ids, reverse=True)


@pytest.mark.parametrize(
    ("field", "value"),
    [("read_ahead_bytes", -1), ("max_read_ahead_files", 0)],
)
def test_the_read_ahead_settings_are_validated(field, value):
    with pytest.raises(ValueError, match=field):
        CompactionConfig(**{field: value})


# -- ZMBNI-1104: the streaming write path ---------------------------------


@pytest.mark.parametrize("streaming", [True, False])
def test_unpartitioned_chunked_output_is_correct_either_way(
    session, unpartitioned, monkeypatch, streaming
):
    """Both branches of the chunked write, forced, on whatever is installed.

    An unpartitioned CHUNKED rewrite either hands the reader to PyIceberg's
    streaming writer or bin-packs locally, decided by
    `capabilities.streaming_write_supported`. Until PyIceberg 0.12 that probe was
    always False, so the streaming branch had **never executed** -- and both
    existing chunked tests use a partitioned table, which takes the local branch
    regardless. It fires for real on 0.12, so it needs pinning rather than
    inheriting whichever answer the installed build happens to give.
    """
    from zamboni.backends import duckdb_arrow
    from zamboni.capabilities import detect

    if streaming and not detect().streaming_write_supported:
        # Forcing the probe True on a build with no streaming writer is not a
        # configuration that can exist -- PyIceberg raises AttributeError inside
        # its own writer. Skipping proves the probe is load-bearing rather than
        # decorative: this branch is unreachable before 0.12.
        pytest.skip("installed PyIceberg has no streaming write path")

    probes = replace(detect(), streaming_write_supported=streaming)
    monkeypatch.setattr(duckdb_arrow, "detect", lambda: probes)

    before = rows(unpartitioned)
    TableCompactor(
        session,
        "db.unpartitioned",
        CompactionConfig(
            memory_mode=MemoryMode.CHUNKED, target_file_size_bytes=200, rewrite_all=True
        ),
    ).execute()

    tbl = session.table("db.unpartitioned")
    assert rows(tbl) == before, "the streaming path must not change the data"
    assert profile_table(tbl).live_files, "nothing was written"


def test_streaming_is_only_used_where_pyiceberg_supports_it(session, partitioned, monkeypatch):
    """Partitioned streaming is unsupported upstream (apache/iceberg-python#2152).

    So the local bin-packer cannot be retired even on 0.12 -- which is the
    answer to ZMBNI-1104's "measure before deleting". Pinned because the
    temptation on seeing the probe go True is to delete `_bin_pack` entirely.
    """
    from zamboni.backends import duckdb_arrow
    from zamboni.capabilities import detect

    used_local_binpack = False
    original = duckdb_arrow._bin_pack

    def spy(reader, target_bytes):
        nonlocal used_local_binpack
        used_local_binpack = True
        yield from original(reader, target_bytes)

    if not detect().streaming_write_supported:
        pytest.skip("installed PyIceberg has no streaming write path")

    monkeypatch.setattr(
        duckdb_arrow, "detect", lambda: replace(detect(), streaming_write_supported=True)
    )
    monkeypatch.setattr(duckdb_arrow, "_bin_pack", spy)

    before = rows(partitioned)
    TableCompactor(
        session,
        "db.partitioned",
        CompactionConfig(
            memory_mode=MemoryMode.CHUNKED, target_file_size_bytes=200, rewrite_all=True
        ),
    ).execute()

    assert used_local_binpack, "a partitioned table must still bin-pack locally"
    assert rows(session.table("db.partitioned")) == before
