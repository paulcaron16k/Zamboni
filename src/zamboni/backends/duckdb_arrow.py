"""Default rewrite backend: PyIceberg reads, DuckDB sorts, PyIceberg writes.

Reads go through :class:`pyiceberg.io.pyarrow.ArrowScan` over a hand-filtered
list of scan tasks. That is the same code path ``DataScan.to_arrow`` uses, so
position deletes and V3 deletion vectors are materialised for us; filtering the
task list is what scopes the read to exactly this rewrite group's files.

Writes go through ``_dataframe_to_data_files``, PyIceberg's own writer. Reusing
it -- rather than writing Parquet ourselves and registering it with
``add_files`` -- matters for two reasons:

* it computes the partition key from the data, so non-order-preserving
  transforms such as ``bucket`` work. ``add_files`` infers partition values from
  column statistics and raises ``ValueError`` on any transform where
  ``preserves_order`` is false (``pyiceberg/io/fileformat.py``).
* it produces the same column statistics, bounds, and split offsets a normal
  write would, so compaction does not silently degrade pruning.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pyarrow as pa
from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files
from pyiceberg.manifest import DataFile
from pyiceberg.table import FileScanTask
from pyiceberg.table.sorting import NullOrder, SortDirection
from pyiceberg.transforms import IdentityTransform

from ..capabilities import detect
from ..config import MemoryMode
from ..planner import FileGroup
from ..zorder import build_zorder_sql
from .base import RewriteBackend, RewriteContext, RewriteOutput


class DuckDBArrowBackend(RewriteBackend):
    """Bounded-memory rewrite using Arrow as the transport."""

    name = "duckdb_arrow"

    def __init__(self, con) -> None:
        self._con = con
        self._plan_cache: list[FileScanTask] = []
        self._plan_cache_key: tuple[object, int | None] | None = None

    def supports(self, ctx: RewriteContext) -> tuple[bool, str]:
        if ctx.table.format_version < 2:
            return False, "format version 1 is not supported"
        return True, ""

    def rewrite(self, group: FileGroup, ctx: RewriteContext) -> RewriteOutput:
        mode = _resolve_mode(group, ctx)
        tasks = self._scan_tasks(group, ctx)
        source_live_rows = self._live_row_count(tasks, ctx)

        if mode is MemoryMode.IN_MEMORY:
            table = self._read_table(tasks, ctx)
            table = self._sort(table, ctx)
            return RewriteOutput(self._write(table, group, ctx), source_live_rows)

        with self._stream(tasks, ctx) as stream:
            # PyIceberg gained a streaming write path after 0.11.1 that bin-packs
            # a RecordBatchReader itself -- but only for unpartitioned tables
            # (partitioned streaming is apache/iceberg-python#2152). Use it when
            # the installed build has it; otherwise bin-pack here.
            if detect().streaming_write_supported and _is_unpartitioned(ctx):
                return RewriteOutput(self._write(stream, group, ctx), source_live_rows)

            written: list[DataFile] = []
            for chunk in _bin_pack(stream, group.target_file_size_bytes):
                written.extend(self._write(chunk, group, ctx))
            return RewriteOutput(written, source_live_rows)

    # -- read ------------------------------------------------------------

    def _scan_tasks(self, group: FileGroup, ctx: RewriteContext) -> list[FileScanTask]:
        """Plan the table once per snapshot, then keep only this group's files.

        Delete files are attached to each task by PyIceberg's own delete index
        rather than by us, which is why this goes through ``plan_files`` instead
        of building tasks from the group's DataFiles directly. The result is
        memoised because a table with many partitions would otherwise re-walk
        every manifest once per group.
        """
        wanted = set(group.paths)
        tasks = [t for t in self._planned(ctx) if t.file.file_path in wanted]

        found = {t.file.file_path for t in tasks}
        missing = wanted - found
        if missing:
            raise RuntimeError(
                f"{len(missing)} planned file(s) are no longer live in the current "
                f"snapshot; the table changed under the plan. First: {sorted(missing)[0]}"
            )
        return tasks

    def _planned(self, ctx: RewriteContext) -> list[FileScanTask]:
        # Keyed on table UUID rather than id(), which CPython recycles after a
        # table object is collected and could alias a different table holding
        # the same snapshot id.
        key = (ctx.table.metadata.table_uuid, ctx.table.metadata.current_snapshot_id)
        if self._plan_cache_key != key:
            self._plan_cache = list(ctx.table.scan().plan_files())
            self._plan_cache_key = key
        return self._plan_cache

    def _live_row_count(self, tasks: list[FileScanTask], ctx: RewriteContext) -> int:
        """Live rows in ``tasks`` -- physical rows minus deleted ones.

        Mirrors ``DataScan.count()``: a task with no delete files contributes
        its manifest ``record_count`` for free; a task with deletes has to be
        read to know how many rows survive.

        For a delete-bearing task this means reading it twice, once to count and
        once to rewrite. That is the price of an *independent* expectation: a
        count taken from the rewrite's own output would agree with itself even
        if the read dropped rows, which is precisely the failure the check
        exists to catch. Delete-free tasks -- every copy-on-write table -- cost
        nothing extra.
        """
        total = 0
        for task in tasks:
            if not task.delete_files:
                total += task.file.record_count
            else:
                total += len(self._arrow_scan(ctx).to_table([task]))
        return total

    def _arrow_scan(self, ctx: RewriteContext) -> ArrowScan:
        scan = ctx.table.scan()
        return ArrowScan(
            ctx.table.metadata,
            ctx.table.io,
            scan.projection(),
            scan.row_filter,
            scan.case_sensitive,
            None,
        )

    def _read_table(self, tasks: list[FileScanTask], ctx: RewriteContext) -> pa.Table:
        return self._arrow_scan(ctx).to_table(tasks)

    def _read_batches(self, tasks: list[FileScanTask], ctx: RewriteContext) -> pa.RecordBatchReader:
        # ArrowScan emits large_string/large_binary where schema_to_pyarrow
        # declares string/binary, so the cast is load-bearing, not cosmetic.
        # DataScan.to_arrow_batch_reader does the same thing upstream.
        target_schema = _projected_arrow_schema(ctx)
        return pa.RecordBatchReader.from_batches(
            target_schema, self._batches_one_file_at_a_time(tasks, ctx)
        ).cast(target_schema)

    def _batches_one_file_at_a_time(
        self, tasks: list[FileScanTask], ctx: RewriteContext
    ) -> Iterator[pa.RecordBatch]:
        """One ``to_record_batches`` call per task, not one for all of them.

        This is what makes CHUNKED bound anything, and it was measured rather
        than reasoned about (ZMBNI-1906). Handing PyIceberg the whole task list
        buffers most of the group, for two compounding reasons in
        ``ArrowScan.to_record_batches``:

        * ``batches_for_task`` materialises **an entire data file** into a list
          before yielding any of it, deliberately -- the comment says so -- to
          keep the work inside the executor.
        * it drives that with ``executor.map(batches_for_task, tasks)``, which
          submits *every* task immediately and returns results in order. Tasks
          that finish early hold their whole materialised file until the
          consumer reaches them.

        So peak memory scaled with the group. Reading one task at a time makes
        it scale with the largest **file** instead. Measured with file size held
        at ~28MB while the group grew 4x:

        ====== ============== ==========
        group  all tasks      per task
        ====== ============== ==========
        224MB  +822MB         +392MB
        447MB  +1088MB        +408MB
        894MB  +1111MB        +434MB
        ====== ============== ==========

        Flat, which is the property that matters; the absolute figure is one
        file plus Arrow allocator retention.

        **This costs throughput** -- around 1.5x on the same measurements, since
        the files are now read in series. That is why it lives on the CHUNKED
        path only: AUTO uses CHUNKED for groups above ``memory_budget_bytes``,
        where bounded memory is the whole point, and leaves small groups on the
        materialising path where speed is.

        **And it does not cost Z-order anything**, which is the reason to prefer
        it over capping group size. DuckDB still receives the entire group as
        one stream and spills its sort to disk, so the ordering sees every row
        it would have seen. A capped group cannot say that: N sub-groups sort
        independently and produce N overlapping ranges.
        """
        for task in tasks:
            yield from self._arrow_scan(ctx).to_record_batches([task])

    # -- sort ------------------------------------------------------------

    def _sort(self, table: pa.Table, ctx: RewriteContext) -> pa.Table:
        sql = ordering_sql("_zamboni_src", list(table.schema.names), ctx)
        if not sql:
            return table
        self._configure_duckdb(ctx)
        self._con.register("_zamboni_src", table)
        try:
            # .arrow() returns a RecordBatchReader in duckdb 1.5; we want the
            # materialised table here because the caller is in IN_MEMORY mode.
            return self._con.execute(sql).to_arrow_table()
        finally:
            self._con.unregister("_zamboni_src")

    # -- chunked path ----------------------------------------------------

    @contextmanager
    def _stream(
        self, tasks: list[FileScanTask], ctx: RewriteContext
    ) -> Iterator[pa.RecordBatchReader]:
        """A record-batch stream of the group's rows, sorted if asked.

        A sort is executed by DuckDB, whose ORDER BY spills to
        ``temp_directory`` rather than holding the whole group in memory.

        This is a context manager because the DuckDB result is lazy: the
        registered relation has to outlive consumption of the reader, so
        unregistering can only happen once the caller is done.
        """
        reader = self._read_batches(tasks, ctx)
        sql = ordering_sql("_zamboni_stream", list(reader.schema.names), ctx)
        if not sql:
            yield reader
            return

        self._configure_duckdb(ctx)
        self._con.register("_zamboni_stream", reader)
        try:
            yield _to_arrow_reader(self._con.execute(sql))
        finally:
            self._con.unregister("_zamboni_stream")

    def _configure_duckdb(self, ctx: RewriteContext) -> None:
        if ctx.config.temp_directory:
            self._con.execute(f"SET temp_directory = '{ctx.config.temp_directory}'")

    # -- write -----------------------------------------------------------

    def _write(
        self, data: pa.Table | pa.RecordBatchReader, group: FileGroup, ctx: RewriteContext
    ) -> list[DataFile]:
        if isinstance(data, pa.Table) and data.num_rows == 0:
            return []
        files = list(
            _dataframe_to_data_files(
                table_metadata=_write_metadata(ctx),
                df=data,
                io=ctx.table.io,
            )
        )
        # `spec_id` is deliberately not part of the data-file struct -- on read
        # it comes from the manifest -- so a freshly written DataFile has none
        # at all. The committer needs it to decide which manifest each file
        # belongs in, so set it explicitly whenever we are not writing under the
        # table default.
        if ctx.write_spec_id is not None:
            for data_file in files:
                data_file.spec_id = ctx.write_spec_id

        # PyIceberg hardcodes sort_order_id=None on every file it writes. Stamp
        # an id only when we ordered by the very sort order that id names --
        # duckdb-iceberg's native compactor stamps unconditionally while never
        # sorting at all, and an arbitrary sort_expression is not the declared
        # order either. Both would make the metadata a lie.
        if ctx.config.sort_by_table_order:
            sort_order_id = ctx.table.sort_order().order_id
            for data_file in files:
                _set_sort_order_id(data_file, ctx.table.format_version, sort_order_id)
        return files


def _write_metadata(ctx: RewriteContext):
    """Table metadata as the writer should see it.

    A pydantic copy with a different ``default_spec_id`` makes PyIceberg's
    writer partition the output by the target spec and stamp that spec id on
    each file. Nothing is committed from this view -- the real table metadata is
    untouched -- so the table's actual default spec never changes.
    """
    if ctx.write_spec_id is None:
        return ctx.table.metadata
    if ctx.write_spec_id not in ctx.table.metadata.specs():
        raise ValueError(
            f"write_spec_id {ctx.write_spec_id} is not registered on the table; "
            "add the spec before rewriting under it"
        )
    return ctx.table.metadata.model_copy(update={"default_spec_id": ctx.write_spec_id})


def _is_unpartitioned(ctx: RewriteContext) -> bool:
    spec_id = ctx.write_spec_id
    spec = ctx.table.spec() if spec_id is None else ctx.table.metadata.specs()[spec_id]
    return len(spec.fields) == 0


def ordering_sql(relation: str, columns: list[str], ctx: RewriteContext) -> str | None:
    """The full SELECT that orders ``relation``, or ``None`` for no ordering.

    Z-order needs a projection (ranking windows feeding an interleave), not just
    an ``ORDER BY`` tail, so every ordering mode returns a whole statement
    rather than a clause.
    """
    if ctx.config.zorder_columns:
        missing = [c for c in ctx.config.zorder_columns if c not in columns]
        if missing:
            raise ValueError(
                f"z-order column(s) {missing} are not in the table's projection {columns}"
            )
        z = build_zorder_sql(list(ctx.config.zorder_columns), ctx.config.zorder_precision_bits)
        return z.select(relation, columns)

    order_by = None
    if ctx.config.sort_expression:
        order_by = ctx.config.sort_expression
    elif ctx.config.sort_by_table_order:
        order_by = _table_order_sql(ctx.table)
    if not order_by:
        return None

    projected = ", ".join(f'"{c}"' for c in columns)
    return f"SELECT {projected} FROM {relation} ORDER BY {order_by}"


def _table_order_sql(table) -> str | None:
    """Render the table's declared sort order as DuckDB ``ORDER BY`` SQL.

    Only identity-transform sort fields can be rendered: a bucket or truncate
    sort field would need the transform applied to the column, and ordering by
    the raw column instead would produce data that does not satisfy the order
    whose id we are about to stamp. Refuse rather than mislabel.
    """
    sort_order = table.sort_order()
    if not sort_order.fields:
        raise ValueError(
            f"sort_by_table_order was requested but {'.'.join(table.name())} declares no "
            "sort order (order id 0 is 'unsorted'). Set one with ALTER TABLE ... WRITE "
            "ORDERED BY, or use sort_expression."
        )

    schema = table.schema()
    terms = []
    for field in sort_order.fields:
        if not isinstance(field.transform, IdentityTransform):
            raise ValueError(
                f"sort_by_table_order cannot render sort field on source id "
                f"{field.source_id}: transform {field.transform} is not identity. "
                "Use sort_expression and accept sort_order_id = None."
            )
        column = schema.find_column_name(field.source_id)
        if column is None:
            raise ValueError(f"sort order references unknown source id {field.source_id}")
        direction = "ASC" if field.direction == SortDirection.ASC else "DESC"
        nulls = "NULLS FIRST" if field.null_order == NullOrder.NULLS_FIRST else "NULLS LAST"
        terms.append(f'"{column}" {direction} {nulls}')
    return ", ".join(terms)


def _set_sort_order_id(data_file: DataFile, format_version: int, value: int | None) -> None:
    """Set ``sort_order_id`` on a written DataFile.

    ``DataFile`` is a positional ``Record``; the attribute is read-only and the
    field's position moves between format versions (14 in V1, 15 in V2/V3), so
    resolve it from the schema rather than hardcoding an index.
    """
    from pyiceberg.manifest import DATA_FILE_TYPE

    names = [f.name for f in DATA_FILE_TYPE[format_version].fields]
    data_file[names.index("sort_order_id")] = value


def _resolve_mode(group: FileGroup, ctx: RewriteContext) -> MemoryMode:
    mode = ctx.config.memory_mode
    if mode is not MemoryMode.AUTO:
        return mode
    return (
        MemoryMode.IN_MEMORY
        if group.input_bytes <= ctx.config.memory_budget_bytes
        else MemoryMode.CHUNKED
    )


def _bin_pack(reader: pa.RecordBatchReader, target_bytes: int) -> Iterator[pa.Table]:
    """Accumulate record batches into tables of roughly ``target_bytes``.

    PyIceberg 0.11.1 has no ``bin_pack_record_batches``, so this stands in for
    it. The weight is uncompressed in-memory Arrow bytes, which overshoots the
    compressed Parquet size -- the same approximation upstream makes.
    """
    schema = reader.schema
    batch_buffer: list[pa.RecordBatch] = []
    buffered = 0
    for batch in reader:
        if batch.num_rows == 0:
            continue
        batch_buffer.append(batch)
        buffered += batch.nbytes
        if buffered >= target_bytes:
            yield pa.Table.from_batches(batch_buffer, schema=schema)
            batch_buffer, buffered = [], 0
    if batch_buffer:
        yield pa.Table.from_batches(batch_buffer, schema=schema)


def _to_arrow_reader(result) -> pa.RecordBatchReader:
    # to_arrow_reader() replaced fetch_record_batch() in duckdb 1.5; keep both
    # working so the package is not pinned to one point release.
    if hasattr(result, "to_arrow_reader"):
        return result.to_arrow_reader()
    return result.fetch_record_batch()  # pragma: no cover


def _projected_arrow_schema(ctx: RewriteContext) -> pa.Schema:
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    return schema_to_pyarrow(ctx.table.scan().projection())
