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

``rewrite_ignores_unrelated_append`` was added later (#239) and measured on its
own, 2026-10-27, warm: 179-191 ms on stock 0.12.0 (two runs; it refuses) and
257-301 ms on the fork (five runs; it commits and reads the table back). It is
not in the 330 ms below, which predates it. A wrong True defers to the library,
which can only over-refuse (exit 3, nothing written); a wrong False runs the
fallback, which skips the one validator the fork skips.

``s3_signer_registered_once`` (#245) was measured the same way, 2026-10-28,
with s3fs and botocore already imported: 817 ms for the first run in a process
(botocore loading its S3 model) and 219-246 ms after, on the fork (five runs);
840 ms then 1 ms on stock 0.12.0, where every run after the first reuses one
shared client. A wrong True stands the signer mirror down on a build that
still swaps signers; a wrong False installs a redundant but correct mirror.

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
import uuid
from dataclasses import asdict, dataclass, fields
from functools import lru_cache
from typing import NamedTuple

from . import probecache

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CouldNotRun:
    """A behavioural probe that could not run, and why.

    Distinct from a False answer: "this build cannot do it" and "we could not
    find out" lead an operator to different places. Before ZMBNI-213 this was a
    bare ``None``, so `doctor` said "could not run" and never what was missing --
    on a plain install, SQLAlchemy, which made compaction refuse with a reason
    naming nothing an operator could act on.
    """

    why: str

    @classmethod
    def from_exception(cls, exc: BaseException) -> CouldNotRun:
        if isinstance(exc, ImportError) and exc.name:
            return cls(f"{exc.name} is not installed")
        return cls(f"{type(exc).__name__}: {exc}")


#: How pruning evidence that was actually measured begins. `_probe` reads it to
#: tell a measurement from a fallback; tests assert on it.
OBSERVED = "observed"
#: How it begins when nothing could be measured and the build is assumed unsafe.
#: `unsupported_reason` reads it so as not to claim a defect nobody observed.
UNKNOWN = "unknown"


class Probed(NamedTuple):
    """What `_probe` found, and which probes could not run.

    The gaps are kept beside the capabilities rather than as a field on them,
    for the reason `_cache_state` is: they describe this environment, not the
    build, and two probes of one build must compare equal.
    """

    capabilities: PyIcebergCapabilities
    unobserved: tuple[str, ...] = ()


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

    #: A file rewrite commits over an append that landed while it ran, in a
    #: partition it never touched. Established by doing it through PyIceberg's
    #: own overwrite producer -- not ours, or the probe would observe our
    #: fallback. False on stock 0.12.0, which refuses with "Added data files
    #: were found matching the filter"; True on the maintenance fork (ZMBNI-79).
    #: False switches on ``_ReplaceFiles._validate_concurrency``'s fallback (#239).
    rewrite_ignores_unrelated_append: bool

    #: Scan planning can materialise equality deletes.
    equality_deletes_readable: bool

    #: A *delete* manifest can be written. Established by writing one and
    #: reading the label back, not by inspecting the writer: on 0.12.0
    #: ``ManifestWriterV2.content`` returns ``ManifestContent.DATA``
    #: unconditionally, so a manifest written through it claims to hold data
    #: files whatever it was handed. Dangling-delete removal is therefore
    #: limited to dropping whole delete manifests; see :mod:`zamboni.deletes`.
    delete_manifests_writable: bool

    #: Every S3 filesystem PyIceberg builds is signed by its own request signer,
    #: and keeps it when s3fs recreates the client. Established by building two
    #: with different sign endpoints and presigning a request through each, not
    #: by looking for a name (#245). False on stock 0.12.0, whose ``_s3`` swaps
    #: the signer on a client every table shares (#51); True on the maintenance
    #: fork. False installs :mod:`zamboni.signer`'s mirror.
    s3_signer_registered_once: bool

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
            if self.pruning_evidence.startswith(UNKNOWN):
                # Nothing was measured, so do not claim the defect: say what
                # stopped the measurement, which is the thing to fix (ZMBNI-213).
                return (
                    "this build's manifest pruning could not be verified: "
                    f"{self.pruning_evidence}. Reinstall iceberg-zamboni so its "
                    "dependencies are complete, then run `zamboni doctor` again."
                )
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
            ("rewrite ignores unrelated append", self.rewrite_ignores_unrelated_append),
            ("equality deletes readable", self.equality_deletes_readable),
            ("delete manifests writable", self.delete_manifests_writable),
            ("S3 signer registered once", self.s3_signer_registered_once),
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
        return _probe().capabilities

    signature = probecache.build_signature()
    if signature is None:
        _cache_state = probecache.describe(None, hit=False)
        return _probe().capabilities

    stored = probecache.load(signature)
    if stored is not None:
        restored = _from_cache(stored)
        if restored is not None:
            _cache_state = probecache.describe(signature, hit=True)
            return restored

    probed, unobserved = _probe()
    if unobserved:
        # Not stored. A probe that could not run is a fact about this
        # environment -- a missing module, an unwritable temp directory -- and
        # the key covers only PyIceberg and Zamboni, so a stored "unknown" would
        # outlive the fix: install the module and the cache still says unsafe
        # (ZMBNI-213). `doctor` prints this line.
        _cache_state = f"not stored -- {'; '.join(unobserved)}"
        return probed
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


def _probe() -> Probed:
    """Ask the installed PyIceberg, every time. The uncached half of `detect`."""
    from importlib.metadata import version

    from pyiceberg.table.snapshots import Operation
    from pyiceberg.table.update.snapshot import _OverwriteFiles, _SnapshotProducer

    prunes = _mentions(
        _OverwriteFiles._existing_manifests, "manifest_evaluator", if_unavailable=True
    )
    derives, evidence = _derivation_is_correct(prunes)
    added_files = _added_files_honour_their_spec()
    unrelated_append = _rewrite_ignores_unrelated_append()
    delete_manifests = _delete_manifests_writable()
    signs_once = _s3_signer_registered_once()

    unobserved = tuple(
        f"{name}: {answer.why}"
        for name, answer in (
            ("added-file spec probe", added_files),
            ("concurrent-append probe", unrelated_append),
            ("delete-manifest probe", delete_manifests),
            ("S3 signer probe", signs_once),
        )
        if isinstance(answer, CouldNotRun)
    )
    if not evidence.startswith(OBSERVED):
        unobserved = (f"manifest-pruning probe: {evidence}", *unobserved)

    capabilities = PyIcebergCapabilities(
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
        added_files_honour_spec=added_files is True,
        # Unknown -> assume the library already behaves, so its own validation
        # runs. That direction can only over-refuse (exit 3, nothing written);
        # the other would skip a library check on a build nobody observed.
        rewrite_ignores_unrelated_append=unrelated_append is not False,
        equality_deletes_readable=not _guard_anywhere_in_scan_planning(),
        # Unknown -> assume NOT writable, which limits dangling-delete removal
        # to whole manifests. Guessing the other way would let us rewrite a
        # delete manifest into one labelled as data.
        delete_manifests_writable=delete_manifests is True,
        # Unknown -> assume NOT, so the mirror installs. On a library that does
        # sign once it is redundant but correct; standing down on one that does
        # not would bring back unsigned requests and the last table loaded
        # signing for all of them.
        s3_signer_registered_once=signs_once is True,
    )
    return Probed(capabilities, unobserved)


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
        return True, f"{OBSERVED} -- an overwrite on a transformed partition kept the right rows"
    if observed is False:
        return False, f"{OBSERVED} -- an overwrite on a transformed partition kept a replaced row"

    if not prunes:
        # Unobserved, and nothing in the source suggests pruning. Safe, but say
        # that it was not measured -- `doctor` prints this line.
        return (
            True,
            (
                f"not observed -- the probe could not run ({observed.why}), and this "
                "build shows no sign of pruning"
            ),
        )
    return (
        False,
        f"{UNKNOWN} -- the behavioural probe could not run ({observed.why}); assuming unsafe",
    )


def _pruning_behaves() -> bool | CouldNotRun:
    """Do the smallest thing that would go wrong, and look at what survived.

    Two rows in a day-partitioned table; replace one; count. A build that prunes
    with a predicate derived from a *source column* keeps the manifest holding
    the replaced row verbatim, so the old row survives beside its replacement
    and three rows come back where two should.

    The transform has to be non-identity. That is the whole shape of the bug:
    a data file records its partition values already transformed, so comparing
    a source column against a partition value only holds for identity.

    Returns :class:`CouldNotRun` when the probe could not run -- no catalog
    available, no writable temp directory. That is not the same as a bad answer,
    and the caller treats it as "assume unsafe" rather than as "unsafe".
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
    except ImportError as exc:  # pragma: no cover - depends on the install
        # Without SQLAlchemy there is no catalog to build a table in, and the
        # question cannot be settled here. A base dependency since ZMBNI-213, so
        # this now means a broken install rather than a missing extra.
        return CouldNotRun.from_exception(exc)

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
    except Exception as exc:  # pragma: no cover - any failure means "could not establish"
        logger.debug("manifest-pruning behavioural probe did not complete", exc_info=True)
        return CouldNotRun.from_exception(exc)


def _added_files_honour_their_spec() -> bool | CouldNotRun:
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

    Returns :class:`CouldNotRun` when the probe could not run: SQLAlchemy
    missing, no writable temp directory. Treated by the caller as "assume
    absent", because declaring a feature we cannot demonstrate is how a config
    gets silently ignored.
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
    except ImportError as exc:  # pragma: no cover - depends on the install
        return CouldNotRun.from_exception(exc)

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
                # from the setup failures below, which return CouldNotRun, because
                # "cannot do this" and "could not find out" lead an operator to
                # different places.
                logger.debug("added-file spec probe: the commit was refused", exc_info=True)
                return False

            # It committed. Confirm the manifest agrees with its entries rather
            # than trusting that no exception means success.
            committed = catalog.load_table("probe.evolved")
            snapshot = committed.current_snapshot()
            if snapshot is None:
                return CouldNotRun("the probe's commit left no current snapshot")
            for manifest in snapshot.manifests(io=committed.io):
                entries = manifest.fetch_manifest_entry(io=committed.io, discard_deleted=False)
                if any(e.data_file.spec_id != manifest.partition_spec_id for e in entries):
                    return False
            return True
    except Exception as exc:  # pragma: no cover - any failure means "could not establish"
        logger.debug("added-file spec behavioural probe did not complete", exc_info=True)
        return CouldNotRun.from_exception(exc)


def _rewrite_ignores_unrelated_append() -> bool | CouldNotRun:
    """Does a rewrite commit over an append that landed while it ran?

    Behavioural, for the reason every probe here that selects an implementation
    is: the answer decides which code runs (#239). Stock 0.12.0 and the
    maintenance fork share every name involved; they differ in one condition
    inside ``_SnapshotProducer._validate_concurrency``.

    The smallest thing that goes wrong: a table with one file; a handle loaded
    from it; a second writer appends; the first handle rewrites its file. Its
    commit fails the ref check, PyIceberg refreshes, and validates the retry
    against the append. A build that runs ``_validate_added_data_files``
    without a filter refuses with ``ValidationException``; one that does not
    commits. Through ``update_snapshot().overwrite()`` -- PyIceberg's producer,
    so what is measured is the library, never our fallback.

    Returns :class:`CouldNotRun` when the probe could not run; the caller then
    defers to the library's own validation, which can only over-refuse.
    """
    try:
        import tempfile

        import pyarrow as pa
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.exceptions import ValidationException
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField
    except ImportError as exc:  # pragma: no cover - depends on the install
        return CouldNotRun.from_exception(exc)

    try:
        with tempfile.TemporaryDirectory(prefix="zamboni-probe-") as root:
            catalog = SqlCatalog(
                "zamboni_probe", uri=f"sqlite:///{root}/c.db", warehouse=f"file://{root}"
            )
            catalog.create_namespace("probe")
            catalog.create_table(
                "probe.t",
                schema=Schema(NestedField(1, "k", IntegerType(), required=False)),
                properties={"format-version": "2"},
            )
            rows = pa.table({"k": pa.array([1], type=pa.int32())})
            catalog.load_table("probe.t").append(rows)

            planned = catalog.load_table("probe.t")
            (doomed,) = [task.file for task in planned.scan().plan_files()]
            catalog.load_table("probe.t").append(rows)  # lands while the rewrite runs

            # The retry is the point of the probe, so PyIceberg's "Commit failed
            # due to a concurrent update, retrying" warning is not news; left on,
            # it printed in `zamboni doctor` and in the first run after install.
            retry_log = logging.getLogger("pyiceberg.table")
            level = retry_log.level
            retry_log.setLevel(logging.ERROR)
            try:
                with planned.transaction() as tx, tx.update_snapshot().overwrite() as rewrite:
                    rewrite.delete_data_file(doomed)
                    rewrite.append_data_file(doomed)
            except ValidationException:
                # The refusal *is* the answer, as in the added-file probe.
                logger.debug("concurrent-append probe: the rewrite was refused", exc_info=True)
                return False
            finally:
                retry_log.setLevel(level)

            committed = catalog.load_table("probe.t")
            return committed.scan().to_arrow().num_rows == 2
    except Exception as exc:  # pragma: no cover - any failure means "could not establish"
        logger.debug("concurrent-append behavioural probe did not complete", exc_info=True)
        return CouldNotRun.from_exception(exc)


def _s3_signer_registered_once() -> bool | CouldNotRun:
    """Is each S3 filesystem signed by its own signer, and still after a reconnect?

    Behavioural, because the answer selects an implementation: false installs
    :mod:`zamboni.signer`'s replacement for PyIceberg's S3 factory. It replaced
    ``hasattr(fsspec, "_signed_s3_filesystem")`` (#245), a name that proves
    nothing -- a build carrying it without a working fix would have made the
    mirror stand down and left #51's bug in place.

    It asks ``fsspec._s3``, the factory :func:`zamboni.signer.install` would
    replace, not ``SCHEME_TO_FS`` -- which holds the mirror once it is
    installed, so asking it would observe our own code. A library that moved
    the fix to a different factory answers false here and then meets
    ``install``'s refusal to overwrite an entry it did not expect: loud, not
    silent.

    A recording signer is registered under a key of its own in
    ``fsspec.SIGNERS`` for the duration, and two filesystems are built with
    different sign endpoints. A presigned URL is generated through each --
    botocore's own signing path, which emits ``before-sign`` with every argument
    its handlers expect, and sends nothing -- and each must be signed by exactly
    its own signer; then once more after ``connect(refresh=True)``, which is how
    s3fs recreates a client. Measured on the 0.12.0 wheel, 2026-10-28: the
    first filesystem is signed by the *second* one's signer, and after the
    reconnect by none. On the fork: each by its own, before and after.

    The explicit credentials keep botocore from walking its credential chain
    (environment, profile, instance metadata), which a client otherwise does
    when it is created; the signer replaces AWS signing, so they are never
    used. The probe's filesystems stay in fsspec's instance cache, keyed on an
    endpoint nothing real uses: two per run on the fork, one on stock, and a run
    is once per install unless the cache is off.

    Without s3fs nothing can remote-sign, so the answer is a stored false rather
    than "could not run": ``install`` costs nothing then, and an unstored answer
    would keep every other probe out of the cache too. Installing s3fs later
    leaves that false standing, which is the safe direction.
    """
    try:
        from pyiceberg.io import fsspec
    except ImportError as exc:  # pragma: no cover - depends on the install
        return CouldNotRun.from_exception(exc)
    try:
        import s3fs  # noqa: F401
        from fsspec.asyn import sync
    except ImportError:
        return False

    key = "zamboni-capability-probe"
    # Fresh endpoints every run: the fork caches a filesystem per set of signer
    # properties, so a second probe in one process -- after detect.cache_clear()
    # -- was handed the first run's filesystem, signing through the first run's
    # recorder, and answered False on the fork (found by the test suite).
    run = uuid.uuid4().hex
    first_endpoint, second_endpoint = f"first-{run}", f"second-{run}"
    signed_by: list[str] = []

    class RecordingSigner(fsspec.S3RequestSigner):
        def __call__(self, request: object, **_: object) -> None:
            signed_by.append(self.properties["s3.signer.endpoint"])

    def build(endpoint: str):
        return fsspec._s3(
            {
                "s3.signer": key,
                "s3.signer.endpoint": endpoint,
                "uri": "http://zamboni-probe.invalid",
                "s3.endpoint": "http://zamboni-probe.invalid",
                "s3.region": "us-east-1",
                "s3.access-key-id": "zamboni-probe",
                "s3.secret-access-key": "zamboni-probe",
            }
        )

    def signers_of(fs) -> list[str]:
        signed_by.clear()
        sync(
            fs.loop, fs.s3.generate_presigned_url, "put_object", Params={"Bucket": "b", "Key": "k"}
        )
        return list(signed_by)

    try:
        fsspec.SIGNERS[key] = RecordingSigner
        try:
            first, second = build(first_endpoint), build(second_endpoint)
            if signers_of(first) != [first_endpoint] or signers_of(second) != [second_endpoint]:
                return False
            first.connect(refresh=True)
            return signers_of(first) == [first_endpoint]
        finally:
            fsspec.SIGNERS.pop(key, None)
    except Exception as exc:  # pragma: no cover - any failure means "could not establish"
        logger.debug("S3 signer behavioural probe did not complete", exc_info=True)
        return CouldNotRun.from_exception(exc)


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


def _delete_manifests_writable() -> bool | CouldNotRun:
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

    Returns :class:`CouldNotRun` when the probe could not run at all, which the
    caller treats as "not writable" -- the safe direction, limiting
    dangling-delete removal to dropping whole delete manifests.
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
    except ImportError as exc:  # pragma: no cover - depends on the install
        return CouldNotRun.from_exception(exc)

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
    except Exception as exc:  # pragma: no cover - any failure means "could not establish"
        logger.debug("delete-manifest behavioural probe did not complete", exc_info=True)
        return CouldNotRun.from_exception(exc)


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
