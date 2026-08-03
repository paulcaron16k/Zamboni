"""Helpers for producing table states PyIceberg cannot write on its own.

**Not part of the maintenance API.** Nothing in `zamboni` calls this at
runtime; it exists so tests and the demo can construct merge-on-read tables.

PyIceberg's `delete()` and `upsert()` are both copy-on-write, so there is no
supported way to make a table with position delete files in it. Without one,
the merge-on-read path -- where a manifest's physical `record_count` and the
live row count diverge -- cannot be exercised at all outside Spark or Flink.
So the delete file is written and registered directly here.

Anything built with this is *simulating* what a merge-on-read engine would
emit. Callers that surface it to a user should say so.

Simulating it *faithfully* takes one more step than it looks. Appending the
delete file through ``_FastAppendFiles`` puts it in a manifest that
``ManifestWriterV2`` labels ``content: data``, because that writer returns
``ManifestContent.DATA`` unconditionally and there is no delete-manifest writer
in PyIceberg. The spec requires delete files to be tracked in delete manifests,
so the table would be readable-but-wrong: PyIceberg and DuckDB both dispatch on
the *entry's* content and cope, but an engine that prunes on manifest content
would miss every delete. :class:`_DeleteManifestWriter` supplies the missing
writer so the simulation is what it claims to be.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriterV2,
)
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.update.snapshot import _FastAppendFiles
from pyiceberg.typedef import Record

#: Iceberg's reserved field ids for the positional delete schema.
DELETE_FILE_PATH_FIELD_ID = 2147483546
DELETE_POS_FIELD_ID = 2147483545

POSITION_DELETE_SCHEMA = pa.schema(
    [
        pa.field(
            "file_path",
            pa.string(),
            nullable=False,
            metadata={b"PARQUET:field_id": str(DELETE_FILE_PATH_FIELD_ID).encode()},
        ),
        pa.field(
            "pos",
            pa.int64(),
            nullable=False,
            metadata={b"PARQUET:field_id": str(DELETE_POS_FIELD_ID).encode()},
        ),
    ]
)


def already_deleted(tbl: Table) -> dict[str, set[int]]:
    """Positions already covered by the table's live position delete files."""
    covered: dict[str, set[int]] = {}
    snapshot = tbl.current_snapshot()
    if snapshot is None:
        return covered

    for manifest in snapshot.manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            data_file = entry.data_file
            if data_file.content != DataFileContent.POSITION_DELETES:
                continue
            with tbl.io.new_input(data_file.file_path).open() as fh:
                deletes = pq.read_table(fh, columns=["file_path", "pos"])
            for path, pos in zip(
                deletes["file_path"].to_pylist(), deletes["pos"].to_pylist(), strict=True
            ):
                covered.setdefault(path, set()).add(pos)
    return covered


def locate_rows(tbl: Table, key_column: str, keys: set[str]) -> dict[str, list[int]]:
    """Find the (file, position) of every *live* row whose key is in ``keys``.

    Position delete files address rows by physical offset, and nothing in the
    Iceberg metadata records which offset holds which key -- so the data files
    have to be read. Only the key column is read back.

    Positions already covered by an existing delete file are excluded. Without
    that, superseding a row for the fifth time re-deletes its four earlier
    versions as well: harmless, since deleting an absent row is a no-op, but it
    inflates the delete files a real engine would never write and so misstates
    what merge-on-read costs.

    Returns a mapping of data file path to the sorted positions to delete.
    """
    covered = already_deleted(tbl)
    found: dict[str, list[int]] = {}
    for task in tbl.scan().plan_files():
        path = task.file.file_path
        seen = covered.get(path, frozenset())
        with tbl.io.new_input(path).open() as fh:
            column = pq.read_table(fh, columns=[key_column])[key_column].to_pylist()
        positions = [i for i, value in enumerate(column) if value in keys and i not in seen]
        if positions:
            found[path] = positions
    return found


def data_file_partitions(tbl: Table) -> dict[str, tuple[int, Record]]:
    """Map each live data file path to its ``(spec_id, partition)``."""
    located: dict[str, tuple[int, Record]] = {}
    snapshot = tbl.current_snapshot()
    if snapshot is None:
        return located
    for manifest in snapshot.manifests(io=tbl.io):
        for entry in manifest.fetch_manifest_entry(io=tbl.io, discard_deleted=True):
            data_file = entry.data_file
            if data_file.content == DataFileContent.DATA:
                located[data_file.file_path] = (data_file.spec_id, data_file.partition)
    return located


def write_position_deletes(
    tbl: Table, deletes: dict[str, list[int]], *, name_hint: str = "sim"
) -> int:
    """Register position delete files covering ``deletes`` and commit them.

    One delete file is written **per partition**. A delete file is itself a
    partitioned file: it carries the partition tuple of the data it targets, and
    a single file covering rows from two partitions is not representable. On an
    unpartitioned table that collapses to one file with an empty tuple.

    Args:
        deletes: data file path -> row positions to mark deleted.
        name_hint: included in the file name so a reader can tell where it came
            from.

    Returns the number of delete files written.
    """
    if not deletes:
        return 0

    partitions = data_file_partitions(tbl)
    grouped: dict[tuple[int, Record], list[tuple[str, int]]] = {}
    for path, positions in deletes.items():
        spec_id, partition = partitions.get(path, (tbl.metadata.default_spec_id, Record()))
        grouped.setdefault((spec_id, partition), []).extend((path, pos) for pos in positions)

    snapshot_id = tbl.metadata.current_snapshot_id or 0
    written: list[DataFile] = []

    for index, ((spec_id, partition), rows) in enumerate(sorted(grouped.items(), key=_group_key)):
        # Ordering by (file_path, pos) is required by the spec and lets readers
        # binary-search the delete file instead of scanning it.
        rows.sort()
        table = pa.table(
            {"file_path": [p for p, _ in rows], "pos": [i for _, i in rows]},
            schema=POSITION_DELETE_SCHEMA,
        )
        path = (
            f"{tbl.location()}/data/position-deletes-{name_hint}"
            f"-{snapshot_id}-{index}-{len(rows)}.parquet"
        )
        with (
            tbl.io.new_output(path).create(overwrite=True) as fh,
            pq.ParquetWriter(fh, POSITION_DELETE_SCHEMA) as writer,
        ):
            writer.write_table(table)

        # spec_id is deliberately not stored in the manifest entry -- the reader
        # derives it from the manifest. from_args therefore does not set the
        # backing attribute, so assign it explicitly for our own grouping below.
        delete_file = DataFile.from_args(
            _table_format_version=tbl.format_version,
            content=DataFileContent.POSITION_DELETES,
            file_path=path,
            file_format=FileFormat.PARQUET,
            partition=partition,
            file_size_in_bytes=len(tbl.io.new_input(path)),
            record_count=len(rows),
            spec_id=spec_id,
            equality_ids=None,
            key_metadata=None,
        )
        delete_file.spec_id = spec_id
        written.append(delete_file)

    with tbl.transaction() as txn:
        producer = _AppendDeleteFiles(operation=Operation.DELETE, transaction=txn, io=tbl.io)
        with producer as update:
            for data_file in written:
                update.append_data_file(data_file)

    tbl.refresh()
    return len(written)


class _DeleteManifestWriter(ManifestWriterV2):
    """The delete-manifest writer PyIceberg does not have.

    ``ManifestWriterV2`` hardcodes ``content()`` to ``DATA`` and writes
    ``content: data`` into the avro metadata. Both have to change together --
    the manifest-list entry and the file itself must agree, or a reader that
    trusts one over the other sees a different table.
    """

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


class _AppendDeleteFiles(_FastAppendFiles):
    """Append delete files into a properly-labelled delete manifest."""

    def _manifests(self) -> list[ManifestFile]:
        by_spec: dict[int, list[DataFile]] = {}
        for data_file in self._added_data_files:
            by_spec.setdefault(data_file.spec_id, []).append(data_file)

        added = []
        for spec_id, files in sorted(by_spec.items()):
            writer = _DeleteManifestWriter(
                spec=self.spec(spec_id),
                schema=self._transaction.table_metadata.schema(),
                output_file=self.new_manifest_output(),
                snapshot_id=self._snapshot_id,
                avro_compression=self._compression,
            )
            with writer:
                for data_file in files:
                    writer.add(
                        ManifestEntry.from_args(
                            status=ManifestEntryStatus.ADDED,
                            snapshot_id=self._snapshot_id,
                            sequence_number=None,
                            file_sequence_number=None,
                            data_file=data_file,
                        )
                    )
            added.append(writer.to_manifest_file())

        return added + self._existing_manifests()


def _group_key(item: tuple[tuple[int, Record], list]) -> tuple[int, str]:
    (spec_id, partition), _ = item
    return spec_id, str(partition)


def add_position_deletes(
    tbl: Table, target_path: str, positions: list[int], *, name_hint: str = "test"
) -> Table:
    """Convenience wrapper: delete ``positions`` from a single data file."""
    write_position_deletes(tbl, {target_path: positions}, name_hint=name_hint)
    return tbl
