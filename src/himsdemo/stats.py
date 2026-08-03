"""Iceberg metadata and statistics, for the `status` command.

Metadata metrics lead, wall-clock follows. At this demo's scale -- roughly 630
event rows -- compaction cannot move query time out of the noise floor, but it
moves file counts, manifest counts and metadata size dramatically and
truthfully. Reporting the honest number first is the difference between a demo
and a sales pitch.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.table import Table

from zamboni.orphans import list_storage, storage_roots
from zamboni.profile import TableProfile, profile_table
from zamboni.reachable import reachable_files
from zamboni.tableconfig import TableConfig
from zamboni.units import human_bytes


@dataclass
class OnDisk:
    """Parquet files physically present, whatever the table thinks of them.

    Deliberately counted the same way `data files` is -- parquet only -- so the
    two lines are directly comparable. Without this, compaction looks like it
    frees storage when it does not: the superseded files stay on disk, live and
    time-travellable, until a snapshot expires.
    """

    files: int = 0
    total_bytes: int = 0
    #: On disk and still referenced by a snapshot, but not by the current one.
    superseded: int = 0
    #: On disk and referenced by nothing at all -- orphan removal's territory.
    unreferenced: int = 0


@dataclass
class TableStats:
    identifier: str
    profile: TableProfile
    snapshots: int
    manifests: int
    metadata_bytes: int
    live_rows: int
    declared_layout: str
    live_specs: str
    on_disk: OnDisk

    @property
    def data_files(self) -> int:
        return len(self.profile.live_files)

    @property
    def total_bytes(self) -> int:
        return self.profile.total_bytes

    @property
    def average_bytes(self) -> int:
        return self.total_bytes // self.data_files if self.data_files else 0


def collect(tbl: Table, config: TableConfig) -> TableStats:
    identifier = ".".join(tbl.name())
    profile = profile_table(tbl)
    snapshot = tbl.current_snapshot()

    manifests = len(snapshot.manifests(io=tbl.io)) if snapshot else 0

    # Metadata size is what a query planner actually has to read before it can
    # touch any data, so it is the metric small files punish first.
    metadata_bytes = _metadata_bytes(tbl)

    # The table knows its own identifier. Deriving one by matching config keys
    # on suffix would pick the wrong namespace as soon as two of them share a
    # table name.
    settings = config.for_table(identifier)
    if settings.ordering.mode == "zorder" and settings.ordering.zorder:
        layout = f"z-order({', '.join(settings.ordering.zorder.columns)})"
    elif settings.ordering.mode == "sort":
        layout = f"sort({', '.join(k.column for k in settings.ordering.sort)})"
    else:
        layout = "unordered"
    parts = ", ".join(f"{p.column}:{p.transform}" for p in settings.partition) or "unpartitioned"

    return TableStats(
        identifier=identifier,
        profile=profile,
        snapshots=len(tbl.metadata.snapshots),
        manifests=manifests,
        metadata_bytes=metadata_bytes,
        live_rows=tbl.scan().count() if snapshot else 0,
        declared_layout=f"[{parts}] {layout}",
        live_specs=_live_specs(tbl, profile),
        on_disk=_on_disk(tbl),
    )


def _on_disk(tbl: Table) -> OnDisk:
    """What storage actually holds, against what the table references.

    Read-only: the same listing and reachability code the reclaim operations
    use, with nothing deleted. Reporting it here is what made the gap between
    "1 live file" and "120 files on disk" visible in the first place.
    """
    from zamboni.reachable import canonical

    storage = list_storage(tbl, storage_roots(tbl))
    referenced = reachable_files(tbl).paths
    # The current snapshot's files, including delete files -- `profile.live_files`
    # is data only, and a live position-delete parquet must not be reported as
    # superseded.
    live = (
        {canonical(p) for p in tbl.inspect.files()["file_path"].to_pylist()}
        if tbl.current_snapshot()
        else set()
    )

    disk = OnDisk()
    for key, entry in storage.items():
        if not key.endswith(".parquet"):
            continue
        disk.files += 1
        disk.total_bytes += entry.size
        if key in live:
            continue
        if key in referenced:
            disk.superseded += 1
        else:
            disk.unreferenced += 1
    return disk


def _live_specs(tbl: Table, profile: TableProfile) -> str:
    """Which partition specs the live files actually sit under.

    After day-to-month evolution a table carries more than one spec, and which
    one a file uses is the visible outcome of that feature -- so report it
    rather than only reporting spec ids.
    """
    specs = tbl.metadata.specs()
    described = []
    for spec_id in sorted(profile.spec_ids):
        spec = specs.get(spec_id)
        if spec is None or not spec.fields:
            continue
        fields = ", ".join(f"{f.name}" for f in spec.fields)
        count = sum(1 for f in profile.live_files if f.spec_id == spec_id)
        described.append(f"{fields} [spec {spec_id}, {count} file(s)]")
    return "  +  ".join(described)


def _metadata_bytes(tbl: Table) -> int:
    """Bytes of Iceberg metadata: manifest lists, manifests, and metadata.json.

    Walks the current snapshot only. Older snapshots' metadata is still on disk
    but a reader never touches it, so counting it would overstate the cost.
    """
    total = _size_of(tbl, tbl.metadata_location)

    snapshot = tbl.current_snapshot()
    if snapshot is None:
        return total

    total += _size_of(tbl, snapshot.manifest_list)
    for manifest in snapshot.manifests(io=tbl.io):
        total += manifest.manifest_length or _size_of(tbl, manifest.manifest_path)
    return total


def _size_of(tbl: Table, path: str) -> int:
    try:
        return len(tbl.io.new_input(path))
    except Exception:  # pragma: no cover - a missing metadata file is not fatal here
        return 0


def render(stats: list[TableStats], *, write_mode: str, days_ingested: int) -> str:
    from .state import TOTAL_DAYS

    lines = [
        "",
        f"  Iceberg status   mode={write_mode}   days ingested={days_ingested}/{TOTAL_DAYS}",
        "  " + "─" * 74,
    ]
    for s in stats:
        deletes = s.profile.position_delete_files
        lines += [
            f"  {s.identifier}",
            f"      layout        {s.declared_layout}",
            f"      rows          {s.live_rows}",
            (
                f"      data files    {s.data_files:<6} total {human_bytes(s.total_bytes):>9}"
                f"   avg {human_bytes(s.average_bytes):>9}"
            ),
            (
                f"      on disk       {s.on_disk.files:<6} "
                f"total {human_bytes(s.on_disk.total_bytes):>9}"
                f"   {s.on_disk.superseded} superseded, {s.on_disk.unreferenced} unreferenced"
            ),
            (
                f"      manifests     {s.manifests:<6} metadata {human_bytes(s.metadata_bytes):>8}"
                f"   snapshots {s.snapshots}"
            ),
        ]
        if deletes:
            lines.append(
                f"      delete files  {deletes:<6} (position deletes -- simulated, see below)"
            )
        if s.data_files:
            lines.append(f"      size spread   {s.profile.size_histogram()}")
        if s.live_specs:
            lines.append(f"      partitioned by {s.live_specs}")
        for finding in s.profile.findings:
            lines.append(f"      ! {finding.code}")
        lines.append("")

    if any(s.profile.position_delete_files for s in stats):
        lines += [
            "  Note: PyIceberg cannot write position deletes -- its delete() and upsert()",
            "  are both copy-on-write. The delete files above were written directly to",
            "  simulate what Spark or Flink would emit. See zamboni/testing.py.",
            "",
        ]
    return "\n".join(lines)
