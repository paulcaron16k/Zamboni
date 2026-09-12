# SPDX-License-Identifier: Apache-2.0
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
import logging
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)


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

    #: Added data files are written under the partition spec they were written
    #: for, rather than under the table default. Partition evolution is the only
    #: thing that needs this -- it is definitionally the case where added files
    #: span two specs -- so a false answer withdraws that one layout feature and
    #: leaves the six operations alone.
    added_files_honour_spec: bool

    #: Scan planning can materialise equality deletes.
    equality_deletes_readable: bool

    #: A *delete* manifest can be written. ``ManifestWriterV2.content`` returns
    #: ``ManifestContent.DATA`` unconditionally in 0.11.1 and on main, and the
    #: avro metadata it writes says ``content: data`` too -- so rewriting a
    #: delete manifest through it produces a manifest that claims to hold data
    #: files. Dangling-delete removal is therefore limited to dropping whole
    #: delete manifests; see :mod:`zamboni.deletes`.
    delete_manifests_writable: bool

    #: *How* ``derives_delete_predicate`` was established. Reported by
    #: ``zamboni doctor``, because "we proved it" and "we recognised a name" are
    #: different confidences, and an operator deciding whether to trust a new
    #: PyIceberg deserves to know which one they have.
    pruning_evidence: str = "not applicable"

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
            # Says what was measured, not what we infer the cause to be. The
            # previous wording asserted a mechanism -- "the producer does not
            # derive that predicate" -- which the probe no longer checks
            # directly, and which was guesswork about somebody else's code.
            return (
                "this build prunes manifests by predicate and does so "
                f"incorrectly: {self.pruning_evidence}. A manifest holding "
                "replaced files is kept verbatim, so their rows would be "
                "counted twice. An early 0.12 release candidate was such a build."
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
            ("  established by", self.pruning_evidence),
            ("added files honour their spec", self.added_files_honour_spec),
            ("equality deletes readable", self.equality_deletes_readable),
            ("delete manifests writable", self.delete_manifests_writable),
        ]
        return "\n".join(f"  {name:<28} {value}" for name, value in rows)


@lru_cache(maxsize=1)
def detect() -> PyIcebergCapabilities:
    from importlib.metadata import version

    from pyiceberg.table.snapshots import Operation
    from pyiceberg.table.update.snapshot import _OverwriteFiles, _SnapshotProducer

    prunes = _mentions(
        _OverwriteFiles._existing_manifests, "manifest_evaluator", if_unavailable=True
    )
    derives, evidence = _derivation_is_correct(prunes)

    return PyIcebergCapabilities(
        version=version("pyiceberg"),
        operation_is_injectable="operation"
        in inspect.signature(_SnapshotProducer.__init__).parameters,
        replace_summary_supported=_replace_summary_supported(Operation),
        streaming_write_supported=_streaming_write_supported(),
        # Unknown -> assume it does prune, so an uninspectable build has to
        # earn its answer below rather than being taken on trust.
        prunes_manifests_by_predicate=prunes,
        derives_delete_predicate=derives,
        pruning_evidence=evidence,
        # Unknown -> assume the "unsupported" guard is present, i.e. NOT
        # readable. Failing the other way would drop the equality-delete
        # blocker and let compaction resurrect deleted rows.
        # Unknown -> assume absent. Declaring a layout feature we could not
        # demonstrate is how a configured evolution gets silently ignored.
        added_files_honour_spec=_added_files_honour_their_spec() is True,
        equality_deletes_readable=not _guard_anywhere_in_scan_planning(),
        # Unknown -> assume NOT writable, which limits dangling-delete removal
        # to whole manifests. Guessing the other way would let us rewrite a
        # delete manifest into one labelled as data.
        delete_manifests_writable=_delete_manifests_writable(),
    )


def _derivation_is_correct(prunes: bool) -> tuple[bool, str]:
    """Is this build's manifest pruning safe, and how do we know?

    **By running it.** Not by looking for a symbol, and the reason is stronger
    than fragility -- a name cannot answer this question at all.

    ZMBNI-1109 began as a rename: the probe asked
    ``hasattr(_SnapshotProducer, "_build_delete_files_partition_predicate")``,
    that method appeared to move, and the answer flipped to False on a build
    where the property held. 83 of 491 tests failed against a PyIceberg that
    passes all 491. The obvious repair was to recognise the new name too.

    Then the symbols were actually enumerated across three builds, and the
    repair collapsed: ``_build_delete_files_partition_predicate`` is present on
    **an early 0.12 release candidate that corrupts data**, and on both attempted
    fixes for it. The
    method's *existence* was never the property. Its *behaviour* changed while
    its name did not, so any name-based probe declares the corrupting build
    safe. A second name in the list would not have helped; it would have made
    the wrong answer arrive faster.

    So the structural check is kept only for the cheap half -- does this build
    prune at all -- and the expensive half is settled by
    :func:`_pruning_behaves`, which does the operation and looks at the result.

    * Does not prune -> nothing to be unsafe about, and no cost. This is
      0.11.1, which is every current user.
    * Prunes -> run the probe once per process (~150ms warm, ~600ms cold).

    A build that changes behaviour without changing a name is exactly what
    happened, and it is the only kind of check that catches it.
    """
    if not prunes:
        return True, "not applicable -- this build does not prune"

    observed = _pruning_behaves()
    if observed is True:
        return True, "observed -- an overwrite on a transformed partition kept the right rows"
    if observed is False:
        return False, "observed -- an overwrite on a transformed partition kept a replaced row"
    return False, "unknown -- the behavioural probe could not run; assuming unsafe"


def _pruning_behaves() -> bool | None:
    """Do the smallest thing that would go wrong, and look at what survived.

    Two rows in a day-partitioned table; replace one; count. A build that prunes
    with a predicate derived from a *source column* keeps the manifest holding
    the replaced row verbatim, so the old row survives beside its replacement
    and three rows come back where two should.

    The transform has to be non-identity. That is the whole shape of the bug:
    a data file records its partition values already transformed, so comparing
    a source column against a partition value only holds for identity.

    Returns None when the probe could not run -- no catalog available, no
    writable temp directory. That is not the same as a bad answer, and the
    caller treats it as "assume unsafe" rather than as "unsafe".
    """
    try:
        import datetime as dt
        import tempfile

        import pyarrow as pa
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import DayTransform
        from pyiceberg.types import IntegerType, NestedField, TimestampType
    except ImportError:  # pragma: no cover - depends on the install
        # `sql` is an optional extra; without it there is no catalog to build a
        # table in, and the question cannot be settled here.
        return None

    try:
        with tempfile.TemporaryDirectory(prefix="zamboni-probe-") as root:
            catalog = SqlCatalog(
                "zamboni_probe", uri=f"sqlite:///{root}/c.db", warehouse=f"file://{root}"
            )
            catalog.create_namespace("probe")
            table = catalog.create_table(
                "probe.t",
                schema=Schema(
                    NestedField(1, "k", IntegerType(), required=False),
                    NestedField(2, "ts", TimestampType(), required=False),
                ),
                partition_spec=PartitionSpec(
                    PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="d")
                ),
                properties={"format-version": "2"},
            )

            arrow = pa.schema([pa.field("k", pa.int32()), pa.field("ts", pa.timestamp("us"))])
            when = dt.datetime(2026, 1, 6, 12)

            def rows(keys: list[int]) -> pa.Table:
                return pa.table(
                    {"k": pa.array(keys, type=pa.int32()), "ts": [when] * len(keys)},
                    schema=arrow,
                )

            table.append(rows([1, 2]))
            table.refresh()
            table.overwrite(rows([1]), overwrite_filter="k == 1")
            table.refresh()

            return sorted(table.scan().to_arrow()["k"].to_pylist()) == [1, 2]
    except Exception:  # pragma: no cover - any failure means "could not establish"
        logger.debug("manifest-pruning behavioural probe did not complete", exc_info=True)
        return None


def _added_files_honour_their_spec() -> bool | None:
    """Does the build write an added file under the spec it was written for?

    Behavioural, because the question is what the library *does* with a file it
    is handed, and the shape of the failure is not something a signature shows.

    The smallest thing that would go wrong: a day-partitioned table, evolved to
    add a second field, then an overwrite that adds a file still belonging to
    the old spec. `_write_added_manifest` declares one manifest under
    `table_metadata.spec()` -- the table default -- for every added file, while
    `_write_delete_manifest` groups by each file's own `spec_id`. A partition
    Record has the arity of the spec that produced it, so a one-field record
    written into a two-field manifest makes the Avro writer index past the end:

        IndexError: list index out of range   (pyiceberg/typedef.py)

    So the probe does not compare labels; it just tries, and a build that cannot
    do this fails loudly enough to catch. That also means it answers correctly
    for a build that fixes it some other way than ours.

    Partition evolution is the only thing that needs this -- it is definitionally
    the case where added files span two specs -- so a False answer withdraws that
    one layout feature and leaves the six operations alone.

    Returns None when the probe could not run: no `sql` extra, no writable temp
    directory. Treated by the caller as "assume absent", because declaring a
    feature we cannot demonstrate is how a config gets silently ignored.
    """
    try:
        import datetime as dt
        import tempfile
        import uuid

        import pyarrow as pa
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.io.pyarrow import _dataframe_to_data_files
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import DayTransform
        from pyiceberg.types import NestedField, StringType, TimestampType
    except ImportError:  # pragma: no cover - depends on the install
        return None

    try:
        with tempfile.TemporaryDirectory(prefix="zamboni-probe-") as root:
            catalog = SqlCatalog(
                "zamboni_probe", uri=f"sqlite:///{root}/c.db", warehouse=f"file://{root}"
            )
            catalog.create_namespace("probe")
            table = catalog.create_table(
                "probe.evolved",
                schema=Schema(
                    NestedField(1, "ts", TimestampType(), required=False),
                    NestedField(2, "region", StringType(), required=False),
                ),
                partition_spec=PartitionSpec(
                    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="d")
                ),
                properties={"format-version": "2"},
            )
            rows = pa.table(
                {
                    "ts": pa.array([dt.datetime(2026, 1, 3, 1)], type=pa.timestamp("us")),
                    "region": pa.array(["eu"]),
                }
            )
            table.append(rows)
            spec0_metadata = table.metadata

            table.update_spec().add_identity("region").commit()
            table = catalog.load_table("probe.evolved")

            # Written for the *old* spec while the table default is the new one,
            # which is exactly what evolving a partition produces.
            replacement = list(
                _dataframe_to_data_files(
                    table_metadata=spec0_metadata, df=rows, io=table.io, write_uuid=uuid.uuid4()
                )
            )
            for data_file in replacement:
                data_file.spec_id = 0

            doomed = [task.file for task in table.scan().plan_files()]
            try:
                with table.transaction() as tx, tx.update_snapshot().overwrite() as overwrite:
                    for data_file in doomed:
                        overwrite.delete_data_file(data_file)
                    for data_file in replacement:
                        overwrite.append_data_file(data_file)
            except Exception:
                # The commit itself failing *is* the answer, not a probe that
                # could not run: a build that writes the file into a manifest
                # for the wrong spec raises from the Avro writer. Distinguished
                # from the setup failures below, which return None, because
                # "cannot do this" and "could not find out" lead an operator to
                # different places.
                logger.debug("added-file spec probe: the commit was refused", exc_info=True)
                return False

            # It committed. Confirm the manifest agrees with its entries rather
            # than trusting that no exception means success.
            committed = catalog.load_table("probe.evolved")
            snapshot = committed.current_snapshot()
            if snapshot is None:
                return None
            for manifest in snapshot.manifests(io=committed.io):
                entries = manifest.fetch_manifest_entry(io=committed.io, discard_deleted=False)
                if any(e.data_file.spec_id != manifest.partition_spec_id for e in entries):
                    return False
            return True
    except Exception:  # pragma: no cover - any failure means "could not establish"
        logger.debug("added-file spec behavioural probe did not complete", exc_info=True)
        return None


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
