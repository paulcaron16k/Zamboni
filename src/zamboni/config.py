"""Configuration for compaction runs."""

from __future__ import annotations

import enum
from dataclasses import dataclass

# Matches DEFAULT_TARGET_FILE_SIZE_BYTES in duckdb-iceberg's rewrite planner
# (src/maintenance/rewrite_data_files_planner.cpp), so plans line up if you ever
# cross-check this against the native iceberg_rewrite_data_files function.
DEFAULT_TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024
MIN_TARGET_FILE_SIZE_BYTES = 100

# Table properties consulted for the target size, in precedence order.
TARGET_FILE_SIZE_PROPERTIES = (
    "write.parquet.target-file-size-bytes",
    "write.target-file-size-bytes",
)


class MemoryMode(enum.Enum):
    """How a single rewrite group is moved from the source table to new files.

    IN_MEMORY materialises the whole group as one ``pa.Table`` and hands it to
    PyIceberg's writer in a single call. Simplest, and fine for small groups.

    CHUNKED streams the group as Arrow record batches and accumulates them into
    bounded slices, writing one slice at a time. Any ``ORDER BY`` is executed by
    DuckDB, which spills its sort to ``temp_directory`` on disk -- that is where
    the "batch/chunk via local temp files" behaviour actually comes from.

    **CHUNKED does not currently bound peak memory, and this docstring used to
    claim it did** ("peak memory is roughly one output file"). Measured on an
    unpartitioned table with a 64MB budget, peak RSS grows linearly with the
    group and is indistinguishable from IN_MEMORY:

    ====== ============= ============== =========
    data   CHUNKED peak  IN_MEMORY peak ratio
    ====== ============= ============== =========
    226MB  +538MB        --             2.4x
    450MB  +975MB        +985MB         2.2x
    889MB  +1840MB       --             2.1x
    ====== ============= ============== =========

    Most of it is upstream: consuming ``ArrowScan.to_record_batches(tasks)`` and
    discarding every batch immediately still grows ~1.3x the group's on-disk
    size, because PyIceberg reads the tasks concurrently and buffers them. The
    bin-packing here adds the rest.

    So the number to budget against is **~2x the largest partition**, not the
    output file size, and the lever that actually works today is partitioning
    the table -- the planner makes one group per (spec, partition), with no cap
    on group size. Tracked as ZMBNI-1906.

    AUTO picks per group by comparing the group's on-disk size against
    ``memory_budget_bytes``.

    Note: PyIceberg 0.11.1 has no streaming write path of its own --
    ``_dataframe_to_data_files`` accepts a ``pa.Table`` only, and
    ``bin_pack_record_batches`` does not exist in this release -- so CHUNKED
    does the bin-packing itself before calling into the writer.
    """

    AUTO = "auto"
    IN_MEMORY = "in_memory"
    CHUNKED = "chunked"


@dataclass(frozen=True)
class CompactionConfig:
    """Knobs for a compaction run.

    Attributes:
        target_file_size_bytes: Desired output file size. ``None`` resolves from
            table properties, then ``DEFAULT_TARGET_FILE_SIZE_BYTES``.
        min_input_files: A partition is only compacted when it has at least this
            many rewrite candidates. Ignored when ``rewrite_all`` is set.
        rewrite_all: Rewrite every live data file, including files that already
            meet the target size.
        memory_mode: See :class:`MemoryMode`.
        memory_budget_bytes: Group size above which ``AUTO`` chooses CHUNKED.
        temp_directory: Where DuckDB spills sorts and hash tables. ``None``
            leaves DuckDB's default in place.
        sort_by_table_order: Order rewritten rows by the table's declared sort
            order, and stamp that order's id onto the output files. This is the
            only case where stamping is truthful, because the ordering we apply
            is by construction the one the id names.
        sort_expression: An arbitrary DuckDB ``ORDER BY`` expression list, e.g.
            ``"customer_id, event_ts"``. Iceberg has no clustering or z-order
            concept, so a z-order is expressed here as a hand-rolled
            bit-interleaving expression. Output files are left with
            ``sort_order_id = None``: the rows are ordered, but not by any order
            the table declares, and claiming otherwise would be the same lie
            duckdb-iceberg's native compactor tells.
        partial_progress: Commit each rewrite group as it completes, rather than
            the whole run in one snapshot. Off by default, matching Iceberg's own
            ``partial-progress.enabled``, whose documentation is worth quoting:
            "This will produce additional commits but allow for progress even if
            some groups fail to commit. This setting will not change the
            correctness of the rewrite operation as file groups can be compacted
            independently." So this is a predictability choice, not a
            correctness one -- with it off, a failure leaves the table exactly as
            it was; with it on, earlier groups stay compacted and later ones do
            not, which is preferable on a table too large to redo.
        branch: Target branch for the compaction snapshot.
        snapshot_operation: ``"replace"`` (spec-correct for compaction; needs a
            small PyIceberg subclass, see :mod:`zamboni.committer`) or
            ``"overwrite"`` (no private-API subclassing, less precise label).
        dangling_delete_policy: What to do when a rewrite group's source files
            have delete files attached. ``"report"`` proceeds and counts them;
            ``"block"`` refuses the group.
    """

    target_file_size_bytes: int | None = None
    min_input_files: int = 2
    rewrite_all: bool = False
    memory_mode: MemoryMode = MemoryMode.AUTO
    memory_budget_bytes: int = 1 * 1024 * 1024 * 1024
    temp_directory: str | None = None
    sort_by_table_order: bool = False
    sort_expression: str | None = None
    zorder_columns: tuple[str, ...] | None = None
    zorder_precision_bits: int = 16
    branch: str = "main"
    snapshot_operation: str = "replace"
    partial_progress: bool = False
    dangling_delete_policy: str = "report"

    def __post_init__(self) -> None:
        if (
            self.target_file_size_bytes is not None
            and self.target_file_size_bytes < MIN_TARGET_FILE_SIZE_BYTES
        ):
            raise ValueError(
                f"target_file_size_bytes must be >= {MIN_TARGET_FILE_SIZE_BYTES}, "
                f"got {self.target_file_size_bytes}"
            )
        if self.min_input_files < 1:
            raise ValueError(f"min_input_files must be >= 1, got {self.min_input_files}")
        chosen = [
            name
            for name, on in (
                ("sort_by_table_order", self.sort_by_table_order),
                ("sort_expression", bool(self.sort_expression)),
                ("zorder_columns", bool(self.zorder_columns)),
            )
            if on
        ]
        if len(chosen) > 1:
            raise ValueError(f"{chosen} are mutually exclusive; a rewrite produces one row order")
        if self.zorder_columns is not None and len(self.zorder_columns) < 2:
            raise ValueError("zorder_columns needs at least 2 columns; use a sort for one key")
        if self.snapshot_operation not in ("replace", "overwrite"):
            raise ValueError(
                "snapshot_operation must be 'replace' or 'overwrite', "
                f"got {self.snapshot_operation!r}"
            )
        if self.dangling_delete_policy not in ("report", "block"):
            raise ValueError(
                "dangling_delete_policy must be 'report' or 'block', "
                f"got {self.dangling_delete_policy!r}"
            )


def resolve_target_file_size(config: CompactionConfig, table_properties: dict[str, str]) -> int:
    """Resolve the output file size: explicit config, then properties, then default."""
    if config.target_file_size_bytes is not None:
        return config.target_file_size_bytes

    for prop in TARGET_FILE_SIZE_PROPERTIES:
        raw = table_properties.get(prop)
        if raw is None:
            continue
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ValueError(f"Table property {prop!r} is not an integer: {raw!r}") from None
        if value < MIN_TARGET_FILE_SIZE_BYTES:
            raise ValueError(
                f"Table property {prop!r} must be >= {MIN_TARGET_FILE_SIZE_BYTES}, got {value}"
            )
        return value

    return DEFAULT_TARGET_FILE_SIZE_BYTES


def config_from_table_settings(settings, base: CompactionConfig | None = None) -> CompactionConfig:
    """Turn a declarative :class:`~zamboni.tableconfig.TableSettings` into a run config.

    Only the layout-target fields come from the table config; operational
    concerns (memory mode, temp directory, branch) stay on ``base`` so an
    operator can tune how a run executes without editing a file that analysts
    own.
    """
    from dataclasses import replace

    base = base or CompactionConfig()
    ordering = settings.ordering

    sort_expression = None
    zorder_columns = None
    zorder_bits = base.zorder_precision_bits
    if ordering.mode == "sort":
        sort_expression = ", ".join(
            f'"{k.column}" {k.direction.upper()} NULLS {k.nulls.upper()}' for k in ordering.sort
        )
    elif ordering.mode == "zorder" and ordering.zorder:
        zorder_columns = tuple(ordering.zorder.columns)
        zorder_bits = ordering.zorder.precision_bits

    return replace(
        base,
        target_file_size_bytes=settings.target_file_size_bytes,
        min_input_files=settings.min_input_files,
        sort_by_table_order=False,
        sort_expression=sort_expression,
        zorder_columns=zorder_columns,
        zorder_precision_bits=zorder_bits,
    )
