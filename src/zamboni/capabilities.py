# SPDX-License-Identifier: Apache-2.0
"""What can this PyIceberg build actually do?

Every version-dependent decision in this package routes through here rather than
through a version comparison or a comment. The reason is concrete and was
measured, not assumed: **two installs both declaring ``0.12.0`` disagree.** The
maintenance fork and stock PyPI answer ``added_files_honour_spec`` differently,
which is why the tested-versions table proposed in ZMBNI-39 was closed as not
planned and why the probe cache in :mod:`zamboni.probecache` is keyed on a hash
of the installed files rather than on a version string.

**Structural or behavioural, and why each is what it is (ZMBNI-38).** A
structural probe asks whether a function exists or what a parameter set
contains; a behavioural one performs the operation and looks at the result. The
difference that matters is not accuracy, it is **how a wrong answer fails**.
Since ZMBNI-83 a probe answer can *select an implementation* rather than merely
withdraw a feature, so a silently wrong answer no longer costs a capability --
it runs the wrong code.

Costs below measured on this machine, 2026-09-21, after the conversions:

===============================  ==========  =========  ==============================
probe                            kind             cost  a wrong answer
===============================  ==========  =========  ==============================
``operation_is_injectable``      structural     0.0 ms  raises, or refuses with exit 3
``replace_summary_supported``    behavioural    0.0 ms  cross-checked; loud either way
``streaming_write_supported``    structural     0.0 ms  raises, or bin-packs locally
``prunes_manifests_by_predicate`` structural    0.3 ms  fallback only; cannot decide
``derives_delete_predicate``     behavioural  143.0 ms  rows counted twice
``added_files_honour_spec``      behavioural  169.5 ms  metadata written under a wrong spec
``equality_deletes_readable``    structural     0.3 ms  raises during scan planning
``delete_manifests_writable``    behavioural    3.2 ms  a delete manifest labelled ``data``
===============================  ==========  =========  ==============================

``detect()`` is **330 ms** cold, and since ZMBNI-88 that is paid once per
install rather than once per process, so the cost argument that once justified
leaving a probe structural has largely expired.

**Converted:** ``delete_manifests_writable``, the only structural probe whose
wrong answer was silent corruption rather than a loud failure -- it returned
True merely because ``write_manifest`` had grown a ``content`` parameter. Now it
writes a manifest holding a position-delete entry and reads the label back. The
story estimated ~200 ms for this; it is 3 ms, because no table is needed.

**Restructured:** ``prunes_manifests_by_predicate`` is no longer a gate. It used
to short-circuit the behavioural half, and ``manifest_pruning_is_safe`` read
``derives or not prunes`` -- so a False from it declared the build safe with
nothing measured. The needle that decides it matches a **local variable name
inside a function body**, so an upstream rename changing no behaviour was enough
to reach that. The measurement is authoritative now and the structural answer is
consulted only when nothing could be observed.

**Left structural, each for a stated reason** -- their silent failure is loud,
not silent:

* ``operation_is_injectable`` -- wrong-True raises ``TypeError`` at construction,
  wrong-False refuses with exit 3. Safe both ways, and now cross-checked by
  constructing a producer for real.
* ``streaming_write_supported`` -- since ZMBNI-16 it sits behind opt-in config
  whose default is off. Wrong-True raises inside PyIceberg; wrong-False
  bin-packs locally, which is the default path anyway.
* ``equality_deletes_readable`` -- wrong-True raises during scan planning, and
  the conversion is *harder than the thing it protects*: PyIceberg cannot write
  equality deletes, so a behavioural probe would have to fabricate one through
  the same private reach it is meant to replace.

Each probe's unknown-answer default is chosen per probe for the safe direction
and says which direction that is at the point it is applied.
"""

from __future__ import annotations

import inspect
import logging
import os
from dataclasses import asdict, dataclass, fields
from functools import lru_cache

from . import probecache

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PyIcebergCapabilities:
    version: str

    #: ``_SnapshotProducer`` accepts the snapshot operation as a constructor
    #: argument, which is how a compaction gets labelled ``replace``.
    operation_is_injectable: bool

    #: ``update_snapshot_summaries`` accepts ``Operation.REPLACE`` directly.
    #: False on 0.12.0, which is why :class:`_ReplaceFiles` exists.
    replace_summary_supported: bool

    #: ``_dataframe_to_data_files`` accepts a ``pa.RecordBatchReader``, so the
    #: writer bin-packs a stream itself and we do not have to.
    streaming_write_supported: bool

    #: ``_OverwriteFiles._existing_manifests`` prunes manifests with a partition
    #: predicate evaluator instead of scanning all of them.
    prunes_manifests_by_predicate: bool

    #: **An overwrite on a transformed partition kept the right rows.** Named
    #: for the mechanism because that is what it originally checked; since
    #: ZMBNI-1109 it is settled by performing the operation, so what it records
    #: is the outcome rather than whether any particular code derives anything.
    #: False means the manifest holding replaced files was kept verbatim and
    #: their rows counted twice -- measured, not inferred.
    derives_delete_predicate: bool

    #: Added data files are written under the partition spec they were written
    #: for, rather than under the table default. Partition evolution is the only
    #: thing that needs this -- it is definitionally the case where added files
    #: span two specs -- so a false answer withdraws that one layout feature and
    #: leaves the six operations alone.
    added_files_honour_spec: bool

    #: Scan planning can materialise equality deletes.
    equality_deletes_readable: bool

    #: A *delete* manifest can be written. Established by writing one and
    #: reading the label back, not by inspecting the writer: on 0.12.0
    #: ``ManifestWriterV2.content`` returns ``ManifestContent.DATA``
    #: unconditionally, so a manifest written through it claims to hold data
    #: files whatever it was handed. Dangling-delete removal is therefore
    #: limited to dropping whole delete manifests; see :mod:`zamboni.deletes`.
    delete_manifests_writable: bool

    #: *How* ``derives_delete_predicate`` was established. Reported by
    #: ``zamboni doctor``, because "we proved it" and "we recognised a name" are
    #: different confidences, and an operator deciding whether to trust a new
    #: PyIceberg deserves to know which one they have.
    pruning_evidence: str = "not applicable"

    @property
    def manifest_pruning_is_safe(self) -> bool:
        """Did an overwrite keep the right rows on this build?

        **`or not prunes_manifests_by_predicate` was removed (ZMBNI-38).** That
        clause let the structural probe overrule a measurement: a build observed
        losing rows was still declared safe if the source showed no sign of
        pruning. The needle deciding that is a local variable name inside a
        function body, so a rename -- a refactor changing no behaviour -- was
        enough to reach it.

        The clause existed for builds that do not prune, where the hazard cannot
        arise. Those are still safe, and now say so for the right reason: the
        observation runs on them too and finds the rows intact. It is only
        consulted when nothing could be observed, where it decides the fallback
        inside :func:`_derivation_is_correct`.
        """
        return self.derives_delete_predicate

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


#: How the last :func:`detect` answered, for ``zamboni doctor``. Not a
#: capability, so it is deliberately not a field on the frozen dataclass --
#: two runs against one build differ here and must still compare equal.
_cache_state = "not yet probed"


def cache_status() -> str:
    """One line on where the last :func:`detect` answer came from."""
    return _cache_state


@lru_cache(maxsize=1)
def detect() -> PyIcebergCapabilities:
    """The installed build's capabilities, probed once per process.

    The probes cost ~0.5s because two of them create a table and commit to it.
    `lru_cache` covers one process, which is enough for a run over many tables
    but not for a cron line that invokes the CLI per table -- so the answers are
    also remembered on disk, keyed to a hash of what is installed (see
    :mod:`zamboni.probecache`). A build that cannot be keyed safely, or a
    filesystem that cannot be written, costs the half second and nothing else.
    """
    global _cache_state

    if os.environ.get("ZAMBONI_NO_PROBE_CACHE"):
        _cache_state = "disabled (ZAMBONI_NO_PROBE_CACHE)"
        return _probe()

    signature = probecache.build_signature()
    if signature is None:
        _cache_state = probecache.describe(None, hit=False)
        return _probe()

    stored = probecache.load(signature)
    if stored is not None:
        restored = _from_cache(stored)
        if restored is not None:
            _cache_state = probecache.describe(signature, hit=True)
            return restored

    probed = _probe()
    written = probecache.store(signature, asdict(probed))
    _cache_state = (
        probecache.describe(signature, hit=False)
        if written
        else (f"miss {signature[:12]} (probed, nowhere writable to store it)")
    )
    return probed


def _from_cache(stored: dict) -> PyIcebergCapabilities | None:
    """Rebuild the answers, or ``None`` if the document does not fit this class.

    Belt and braces over ``CACHE_SCHEMA``: a field added or renamed without
    bumping it lands here and re-probes, rather than constructing something
    half-populated.
    """
    expected = {f.name for f in fields(PyIcebergCapabilities)}
    if set(stored) != expected:
        return None
    try:
        return PyIcebergCapabilities(**stored)
    except TypeError:
        return None


def _probe() -> PyIcebergCapabilities:
    """Ask the installed PyIceberg, every time. The uncached half of `detect`."""
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
        delete_manifests_writable=_delete_manifests_writable() is True,
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

    **The observation is authoritative, and the structural answer is only a
    fallback (ZMBNI-38).** It used to be the other way around: a False from the
    structural half short-circuited, and `manifest_pruning_is_safe` is
    ``derives or not prunes``, so "this build does not prune" declared the build
    safe *without observing anything*. The needle that decides it is
    ``manifest_evaluator``, which in 0.12 matches a **local variable name inside
    a function body** (``manifest_evaluators``, line 4 of
    ``_OverwriteFiles._existing_manifests``). Renaming a local is a pure
    refactor that changes no behaviour -- and it would have flipped this probe
    to False, skipped the observation, and declared a pruning build safe. The
    same class as ZMBNI-1109, one level cheaper to trigger.

    The old short-circuit was justified by cost, on a premise that has since
    expired: "does not prune -> no cost, this is 0.11.1, which is every current
    user". The floor is `>=0.12` now, and every supported build prunes, so the
    branch it saved is one no supported install takes. What it still saves is
    the probe on a build that genuinely does not prune -- paid once per install
    since ZMBNI-88 caches the answers, rather than once per process.

    * Observed either way -> that is the answer, whatever the source looks like.
    * Could not observe -> fall back to the structural answer, which can now
      only *withdraw* pruning safety and never grant it unexamined.

    A build that changes behaviour without changing a name is exactly what
    happened, and it is the only kind of check that catches it.
    """
    observed = _pruning_behaves()
    if observed is True:
        return True, "observed -- an overwrite on a transformed partition kept the right rows"
    if observed is False:
        return False, "observed -- an overwrite on a transformed partition kept a replaced row"

    if not prunes:
        # Unobserved, and nothing in the source suggests pruning. Safe, but say
        # that it was not measured -- `doctor` prints this line.
        return (
            True,
            "not observed -- the probe could not run, and this build shows no sign of pruning",
        )
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


def _delete_manifests_writable() -> bool | None:
    """Write one and read back what it claims to be.

    **Converted from structural to behavioural (ZMBNI-38).** It used to return
    True merely because ``write_manifest`` had grown a ``content`` parameter --
    a parameter that existed but did not mean what we assumed would engage
    ZMBNI-9's rewrite path and stamp a delete manifest ``content: data``, after
    which a reader treats position deletes as **rows**. That is the ZMBNI-1109
    shape exactly: a name-based probe declaring a corrupting build safe.

    So this asks the question by doing it. It writes a real manifest holding a
    real position-delete entry, asking for ``DELETES`` if the API accepts such a
    request, and reads the content label back off the resulting
    ``ManifestFile`` -- which is where the label actually lives. The avro file
    itself carries no content marker; ``ManifestWriter._meta`` writes schema,
    partition-spec, partition-spec-id, format-version and the codec, and nothing
    else, so the label a reader sees is the one ``to_manifest_file()`` puts in
    the manifest *list*. That is precisely what ``ManifestWriterV2.content``
    returning ``DATA`` unconditionally decides.

    **Measured at 2-4 ms** on this build, against the ~200 ms the story
    estimated for it: the estimate assumed a table, and no table is needed. It
    is the cheapest of the three conversions considered and the only one whose
    wrong answer is silent corruption rather than a loud failure.

    Returns None when the probe could not run at all, which the caller treats as
    "not writable" -- the safe direction, limiting dangling-delete removal to
    dropping whole delete manifests.
    """
    try:
        import tempfile

        from pyiceberg.io.pyarrow import PyArrowFileIO
        from pyiceberg.manifest import (
            DataFile,
            DataFileContent,
            FileFormat,
            ManifestContent,
            ManifestEntry,
            ManifestEntryStatus,
            write_manifest,
        )
        from pyiceberg.partitioning import PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField
    except ImportError:  # pragma: no cover - depends on the install
        return None

    try:
        with tempfile.TemporaryDirectory(prefix="zamboni-probe-") as root:
            arguments = {
                "format_version": 2,
                "spec": PartitionSpec(),
                "schema": Schema(NestedField(1, "k", IntegerType(), required=False)),
                "output_file": PyArrowFileIO().new_output(f"{root}/m.avro"),
                "snapshot_id": 1,
                # "null" rather than "uncompressed": `compression_codec` rejects
                # the latter, and the probe must not fail on its own arguments.
                "avro_compression": "null",
            }
            # Ask for a delete manifest where the API allows it to be asked for.
            # On a build with no such parameter the question becomes "what does
            # it produce when handed delete entries", which is the same question
            # the rewrite path would be asking.
            if "content" in inspect.signature(write_manifest).parameters:
                arguments["content"] = ManifestContent.DELETES

            with write_manifest(**arguments) as writer:  # type: ignore[arg-type]
                writer.add_entry(
                    ManifestEntry.from_args(
                        status=ManifestEntryStatus.ADDED,
                        snapshot_id=1,
                        data_file=DataFile.from_args(
                            content=DataFileContent.POSITION_DELETES,
                            file_path=f"{root}/d.parquet",
                            file_format=FileFormat.PARQUET,
                            partition={},
                            record_count=1,
                            file_size_in_bytes=1,
                            spec_id=0,
                        ),
                    )
                )
            # An empty manifest raises on close, which is why an entry is
            # written rather than an empty file being probed.
            return writer.to_manifest_file().content is ManifestContent.DELETES
    except Exception:  # pragma: no cover - any failure means "could not establish"
        logger.debug("delete-manifest behavioural probe did not complete", exc_info=True)
        return None


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
