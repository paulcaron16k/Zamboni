"""What can this PyIceberg build actually do?

Every version-dependent decision in this package routes through here rather than
through a version comparison or a comment. The reason is concrete: the
behaviours that matter are split across a release boundary that is currently
open. PyIceberg 0.11.1 is the latest release; unreleased main (heading for 0.12)
already changes three of the seven probes below -- streaming writes, manifest
pruning and delete-predicate derivation all go False -> True, the last two
together, which is what keeps ``manifest_pruning_is_safe`` true across the
boundary. Pinning to "0.11.1 behaves like X" would silently rot the day 0.12
ships. See docs/roadmap.md RM-1 for the full delta.

Probes are structural -- they ask whether a function exists or what a parameter
set contains -- so they answer for whatever build is installed, including a
checkout of main installed with ``uv pip install -e``.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class PyIcebergCapabilities:
    version: str

    #: ``_SnapshotProducer`` accepts the snapshot operation as a constructor
    #: argument, which is how a compaction gets labelled ``replace``.
    operation_is_injectable: bool

    #: ``update_snapshot_summaries`` accepts ``Operation.REPLACE`` directly.
    #: False in 0.11.1 and on main, which is why :class:`_ReplaceFiles` exists.
    replace_summary_supported: bool

    #: ``_dataframe_to_data_files`` accepts a ``pa.RecordBatchReader``, so the
    #: writer bin-packs a stream itself and we do not have to.
    streaming_write_supported: bool

    #: ``_OverwriteFiles._existing_manifests`` prunes manifests with a partition
    #: predicate evaluator instead of scanning all of them.
    prunes_manifests_by_predicate: bool

    #: The producer derives that predicate from the removed data files. Required
    #: whenever ``prunes_manifests_by_predicate`` is true, otherwise manifests
    #: holding removed files are kept verbatim and their rows count twice.
    derives_delete_predicate: bool

    #: Scan planning can materialise equality deletes.
    equality_deletes_readable: bool

    #: A *delete* manifest can be written. ``ManifestWriterV2.content`` returns
    #: ``ManifestContent.DATA`` unconditionally in 0.11.1 and on main, and the
    #: avro metadata it writes says ``content: data`` too -- so rewriting a
    #: delete manifest through it produces a manifest that claims to hold data
    #: files. Dangling-delete removal is therefore limited to dropping whole
    #: delete manifests; see :mod:`zamboni.deletes`.
    delete_manifests_writable: bool

    @property
    def manifest_pruning_is_safe(self) -> bool:
        return self.derives_delete_predicate or not self.prunes_manifests_by_predicate

    def unsupported_reason(self) -> str | None:
        """Why this build cannot be used, or ``None`` if it can."""
        if not self.operation_is_injectable:
            return (
                "_SnapshotProducer does not accept an 'operation' argument, so a "
                "compaction cannot be labelled as a replace snapshot."
            )
        if not self.manifest_pruning_is_safe:
            return (
                "_OverwriteFiles prunes manifests by predicate but the producer "
                "does not derive that predicate from the removed data files. "
                "Manifests holding removed files would be kept verbatim and "
                "their rows counted twice."
            )
        return None

    def describe(self) -> str:
        rows = [
            ("pyiceberg", self.version),
            ("operation injectable", self.operation_is_injectable),
            ("REPLACE summary native", self.replace_summary_supported),
            ("streaming writes", self.streaming_write_supported),
            ("manifest predicate pruning", self.prunes_manifests_by_predicate),
            ("derives delete predicate", self.derives_delete_predicate),
            ("equality deletes readable", self.equality_deletes_readable),
            ("delete manifests writable", self.delete_manifests_writable),
        ]
        return "\n".join(f"  {name:<28} {value}" for name, value in rows)


@lru_cache(maxsize=1)
def detect() -> PyIcebergCapabilities:
    from importlib.metadata import version

    from pyiceberg.table.snapshots import Operation
    from pyiceberg.table.update.snapshot import _OverwriteFiles, _SnapshotProducer

    return PyIcebergCapabilities(
        version=version("pyiceberg"),
        operation_is_injectable="operation"
        in inspect.signature(_SnapshotProducer.__init__).parameters,
        replace_summary_supported=_replace_summary_supported(Operation),
        streaming_write_supported=_streaming_write_supported(),
        # Unknown -> assume it does prune. Combined with the reliable
        # `derives_delete_predicate` hasattr probe, that makes an
        # uninspectable build refuse rather than risk double-counted rows.
        prunes_manifests_by_predicate=_mentions(
            _OverwriteFiles._existing_manifests, "manifest_evaluator", if_unavailable=True
        ),
        derives_delete_predicate=_derives_delete_predicate(_SnapshotProducer),
        # Unknown -> assume the "unsupported" guard is present, i.e. NOT
        # readable. Failing the other way would drop the equality-delete
        # blocker and let compaction resurrect deleted rows.
        equality_deletes_readable=not _guard_anywhere_in_scan_planning(),
        # Unknown -> assume NOT writable, which limits dangling-delete removal
        # to whole manifests. Guessing the other way would let us rewrite a
        # delete manifest into one labelled as data.
        delete_manifests_writable=_delete_manifests_writable(),
    )


#: Names PyIceberg has given the "derive a predicate from the removed files"
#: step. It was ``_build_delete_files_partition_predicate`` when pruning first
#: appeared; the fix for the 0.12 upsert regression renames it to
#: ``_build_delete_files_partition_filters``, because the predicate moved from
#: the source-column domain to the partition-field domain -- which is what makes
#: manifest pruning correct across transforms.
DERIVATION_METHODS = (
    "_build_delete_files_partition_filters",
    "_build_delete_files_partition_predicate",
)


def _derives_delete_predicate(producer: type) -> bool:
    """Does the producer derive its pruning predicate from the removed files?

    Matching a *set* of names rather than one, because the single-name version
    turned a rename into "this build is unusable". That is the safe direction --
    Zamboni refuses rather than risking double-counted rows -- but it is still
    wrong, and it would have refused every build carrying the fix for
    apache/iceberg-python#3758.
    """
    return any(hasattr(producer, name) for name in DERIVATION_METHODS)


def _guard_anywhere_in_scan_planning() -> bool:
    """Does *any* of scan planning still refuse equality deletes?

    This looks at the whole ``pyiceberg.table`` module rather than at one
    function, and that is the second version of this probe. The first inspected
    ``DataScan._plan_files_local`` alone, which held the guard inline in 0.11.1.
    On unreleased main the planner was extracted (``BaseScan`` /
    ``ManifestGroupPlanner``) and that method became a five-line delegation, so
    the probe found no guard and reported equality deletes as **readable** --
    while the refusal was alive and well one call deeper. A false positive on
    the probe whose whole job is preventing compaction from resurrecting
    deleted rows.

    Searching the module is cruder and far harder to break: the guard has to
    disappear from the file entirely before this says "readable". The failure
    mode it leaves is the safe one -- a stray mention in a comment would make us
    refuse a table we could have compacted, which costs a skipped optimisation
    rather than data.
    """
    needle = "does not yet support equality deletes"
    try:
        import pyiceberg.table as table_module

        return needle in inspect.getsource(table_module)
    except (OSError, TypeError):  # pragma: no cover - source unavailable
        # Frozen, vendored or zipapp: assume the guard is there.
        return True


def _delete_manifests_writable() -> bool:
    """Can this build write a manifest that declares itself a delete manifest?

    Two ways it could: ``write_manifest`` growing a content argument, or a
    writer class whose ``content()`` returns ``DELETES``. Neither exists today.
    """
    from pyiceberg.manifest import ManifestContent, ManifestWriterV2, write_manifest

    if "content" in inspect.signature(write_manifest).parameters:
        return True
    try:
        return ManifestWriterV2.content(None) is ManifestContent.DELETES  # type: ignore[arg-type]
    except Exception:
        return False


def _replace_summary_supported(operation_enum) -> bool:
    """Ask ``update_snapshot_summaries`` rather than guess from a version."""
    from pyiceberg.table.snapshots import Summary, update_snapshot_summaries

    try:
        update_snapshot_summaries(Summary(operation=operation_enum.REPLACE))
    except ValueError:
        return False
    except Exception:
        # Some other failure means the probe is inconclusive; assume the
        # relabelling path is still needed rather than risking a hard error at
        # commit time.
        return False
    return True


def _streaming_write_supported() -> bool:
    import pyarrow as pa
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    annotation = inspect.signature(_dataframe_to_data_files).parameters["df"].annotation
    if "RecordBatchReader" in str(annotation):
        return True
    # Fall back to the helper the streaming path is built on.
    import pyiceberg.io.pyarrow as module

    return hasattr(module, "bin_pack_record_batches") and hasattr(pa, "RecordBatchReader")


def _mentions(func, needle: str, *, if_unavailable: bool) -> bool:
    """Whether ``func``'s source mentions ``needle``.

    Source is not always inspectable -- a frozen, vendored or zipapp PyIceberg
    has no ``.py`` on disk. Each caller must say which answer is *safe* when
    that happens, because the safe direction differs per probe and defaulting
    to one of them silently inverts the other.
    """
    try:
        return needle in inspect.getsource(func)
    except (OSError, TypeError):  # pragma: no cover - source unavailable
        return if_unavailable
