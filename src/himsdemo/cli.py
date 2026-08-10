# SPDX-License-Identifier: Apache-2.0
"""`./bin/demo` -- the HIMS discharge maintenance demo.

The intended arc is ingest -> status -> query -> maintenance -> status -> query,
with the developer free to inspect the catalog in between. Commands that change
state say so; `status` and `query` never do.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from zamboni import CompactionConfig, MemoryMode, version_banner
from zamboni.tableconfig import TableConfig

from . import catalogs, queries, stats
from .ingest import ingest_day
from .schema import SchemaDocument, create_tables, load_tables
from .state import MODES, TOTAL_DAYS, DemoState

DEFAULT_ROOT = Path(__file__).resolve().parent.parent.parent / "data" / "healthims"


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    state = DemoState.load(args.root)
    for warning in state.warnings:
        print(f"warning: {warning}", file=sys.stderr)

    try:
        args.demo_catalog = catalogs.build(catalogs.resolve_backend(args.catalog), state)
    except catalogs.CatalogUnavailable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    handlers = {
        "clear": _clear,
        "reset": _clear,
        "mode": _mode,
        "next-day": _next_day,
        "status": _status,
        "maintenance": _maintenance,
        "query": _query,
    }
    return handlers[args.command](state, args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="demo", description=__doc__.splitlines()[0])
    # Same banner as `zamboni --version`: the demo ships from the same wheel, and
    # what it demonstrates depends on the same probed PyIceberg.
    parser.add_argument("--version", action="version", version=version_banner())
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help=argparse.SUPPRESS)
    parser.add_argument(
        "--catalog",
        choices=catalogs.BACKENDS,
        help=(
            "where the tables live. 'sqlite' (default) needs nothing running; "
            "'lakekeeper' uses the dev stack and reads ZAMBONI_URI / "
            f"ZAMBONI_WAREHOUSE. Also settable via {catalogs.ENV_BACKEND}."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("clear", help="drop the catalog and warehouse, back to day 0")
    sub.add_parser("reset", help="alias for clear")

    mode = sub.add_parser("mode", help="show or set the write mode")
    mode.add_argument("value", nargs="?", choices=MODES)

    sub.add_parser("next-day", help="ingest the next day, then print status")
    sub.add_parser("status", help="Iceberg metadata and statistics")
    maintenance = sub.add_parser("maintenance", help="run zamboni, then print status")
    maintenance.add_argument(
        "--reclaim-now",
        action="store_true",
        help=(
            "expire down to the minimum snapshot count and ignore the orphan age guard, "
            "so storage falls within one run. Unsafe against a concurrent writer."
        ),
    )
    sub.add_parser("query", help="run the demo queries")
    return parser


# -- session plumbing ----------------------------------------------------


def _open(state: DemoState, catalog, *, create: bool):
    """Catalog session, schema document, layout config, and the tables.

    ``create=False`` is what read-only commands use: they must not bring a
    catalog into existence just by being run.
    """
    session = catalogs.open_session(catalog, state, create=create)
    schema = SchemaDocument.load(state.schema_path)
    config = TableConfig.load(state.table_config_path)
    tables = (
        create_tables(session.catalog, schema, config)
        if create
        else load_tables(session.catalog, schema)
    )
    return session, schema, config, tables


def _has_catalog(state: DemoState, catalog) -> bool:
    """Whether the demo has anything in this catalog yet.

    Checked before opening, not after: for SQLite, SQLAlchemy creates the file
    on connect, so opening one to look would itself be the mutation a read-only
    command must not make. See :mod:`himsdemo.catalogs`.
    """
    return catalogs.exists(catalog, state)


def _print_status(state: DemoState, catalog) -> None:
    if not _has_catalog(state, catalog):
        print("\n  No tables yet -- run './bin/demo next-day'.\n")
        return

    session, schema, config, tables = _open(state, catalog, create=False)
    try:
        if not tables:
            print("\n  No tables yet -- run './bin/demo next-day'.\n")
            return
        collected = [
            stats.collect(tables[d.name], config) for d in schema.tables if d.name in tables
        ]
        print(
            stats.render(
                collected,
                write_mode=state.write_mode,
                days_ingested=state.days_ingested,
            )
        )
    finally:
        session.close()


# -- commands ------------------------------------------------------------


def _clear(state: DemoState, args: argparse.Namespace) -> int:
    catalog = args.demo_catalog
    from .schema import SchemaDocument

    names = [d.name for d in SchemaDocument.load(state.schema_path).tables]
    detail = catalogs.clear(catalog, state, names)
    state.reset_counters()
    print(f"cleared. mode={state.write_mode}, days ingested=0")
    print(f"  {detail}")
    return 0


def _mode(state: DemoState, args: argparse.Namespace) -> int:
    if args.value is None:
        print(f"mode={state.write_mode}  (days ingested={state.days_ingested})")
        return 0

    if args.value == state.write_mode:
        print(f"mode already {state.write_mode}")
        return 0

    # Half a run in one mode and half in another compares nothing.
    if state.days_ingested > 0:
        print(
            f"refusing to switch to {args.value}: {state.days_ingested} day(s) already "
            f"ingested as {state.write_mode}.\nRun './bin/demo clear' first.",
            file=sys.stderr,
        )
        return 2

    state.write_mode = args.value
    state.save()
    print(f"mode={state.write_mode}")
    return 0


def _next_day(state: DemoState, args: argparse.Namespace) -> int:
    catalog = args.demo_catalog
    if not state.has_more_days:
        print("No More Data")
        return 0

    day_no = state.days_ingested + 1
    if state.ingesting_day is not None:
        # Batches commit independently, so the tables already hold part of that
        # day. Replaying it would re-append and re-delete identical rows, and
        # the resulting file counts would measure the crash, not the write mode.
        print(
            f"Day {state.ingesting_day} was interrupted mid-ingest and is partly loaded.\n"
            "Run './bin/demo clear' and start again -- replaying it would distort "
            "the file counts this demo reports.",
            file=sys.stderr,
        )
        return 2

    state.ingesting_day = day_no
    state.save()

    session, schema, _config, tables = _open(state, catalog, create=True)
    try:
        result = ingest_day(tables, schema, state.day_dir(day_no), day_no, state.write_mode)
        print(result.describe())
    finally:
        session.close()

    state.days_ingested = day_no
    state.ingesting_day = None
    state.save()
    _print_status(state, args.demo_catalog)
    return 0


def _status(state: DemoState, args: argparse.Namespace) -> int:
    _print_status(state, args.demo_catalog)
    return 0


def _maintenance(state: DemoState, args: argparse.Namespace) -> int:
    catalog = args.demo_catalog
    if state.days_ingested == 0 or not _has_catalog(state, catalog):
        print("nothing ingested yet -- run './bin/demo next-day' first")
        return 0

    from zamboni import TableCompactor

    session, schema, config, _tables = _open(state, catalog, create=True)
    try:
        # Layout comes from table-config.json; memory mode and temp directory
        # are operational choices and stay here.
        base = CompactionConfig(
            memory_mode=MemoryMode.AUTO,
            temp_directory=str(state.spill_path),
        )
        print("\n  Running zamboni")
        print("  " + "─" * 74)
        if args.reclaim_now:
            print(
                "  --reclaim-now: expiring to the minimum and ignoring the orphan age\n"
                "  guard. Safe here because nothing else writes this warehouse; against a\n"
                "  live writer it can delete a file that has been written but not committed.\n"
            )
        for definition in schema.tables:
            compactor = TableCompactor.from_table_config(
                session, definition.identifier, config, base=base
            )
            _indent(compactor.execute().describe(), 2)
            _reclaim(session, definition.identifier, config, reclaim_now=args.reclaim_now)
    finally:
        session.close()

    _print_status(state, args.demo_catalog)
    return 0


def _reclaim(session, identifier: str, config: TableConfig, *, reclaim_now: bool) -> None:
    """Drop dangling deletes, expire snapshots, then delete what that unreferenced.

    The order is fixed. Compaction alone frees nothing, because every superseded
    file is still referenced by the snapshot it was compacted out of. Dropping
    dangling delete files unreferences those too. Expiry then removes the old
    snapshots and deletes what it can prove it orphaned; orphan removal sweeps
    the rest -- writes that were made and then abandoned, and metadata versions
    dropped from the log.
    """
    from zamboni.deletes import DanglingDeleteCleaner
    from zamboni.expire import RetentionPolicy, SnapshotExpirer
    from zamboni.manifests import ManifestRewriter
    from zamboni.orphans import OrphanCleaner
    from zamboni.properties import apply_metadata_properties

    retention = config.for_table(identifier).retention

    # Declared table properties first: where a `retention.metadata` block sets
    # previous-versions-max, trimming the log is what makes the stale
    # metadata.json files unreferenced for orphan removal to find. The demo
    # declares none, so this is a no-op here and the leaked files come from
    # ordinary commits instead.
    applied = apply_metadata_properties(session.table(identifier), retention.metadata)
    if applied.changes:
        _indent(applied.describe(), 6)

    # Before expiry, not after: dropping a dangling delete is what makes the
    # file unreferenced in the first place, so running it second would leave
    # the reclaim a full cycle behind.
    if retention.remove_dangling_deletes.enabled:
        result = DanglingDeleteCleaner().run(session.table(identifier))
        if result.report.delete_files:
            _indent(result.describe(), 6)

    # After the delete removal, so the manifests it emptied are gone before we
    # decide how to regroup what remains.
    if retention.rewrite_manifests.enabled:
        rewrite = ManifestRewriter(
            min_input_manifests=retention.rewrite_manifests.min_input_manifests
        ).run(session.table(identifier))
        if rewrite.manifests_before:
            _indent(rewrite.describe(), 6)

    if retention.expire_snapshots.enabled:
        settings = retention.expire_snapshots
        tbl = session.table(identifier)
        policy = RetentionPolicy.resolve(
            tbl.properties,
            # Demo files are written now regardless of their partition values, so
            # the stock 5-day age expires nothing. Zero keeps min_snapshots_to_keep
            # in charge, which retains the current snapshot and no more.
            max_snapshot_age_days=0 if reclaim_now else settings.max_snapshot_age_days,
            min_snapshots_to_keep=settings.min_snapshots_to_keep,
            max_ref_age_days=settings.max_ref_age_days,
        )
        _indent(SnapshotExpirer(policy).run(tbl).describe(), 6)

    if retention.remove_orphan_files.enabled:
        days = 0 if reclaim_now else retention.remove_orphan_files.older_than_days
        _indent(OrphanCleaner(older_than_days=days).run(session.table(identifier)).describe(), 6)


def _indent(text: str, width: int) -> None:
    for line in text.splitlines():
        print(" " * width + line)


def _query(state: DemoState, args: argparse.Namespace) -> int:
    catalog = args.demo_catalog
    if state.days_ingested == 0 or not _has_catalog(state, catalog):
        print("nothing ingested yet -- run './bin/demo next-day' first")
        return 0

    session, _schema, _config, tables = _open(state, catalog, create=False)
    try:
        print("")
        print(
            f"  Queries   mode={state.write_mode}   "
            f"days ingested={state.days_ingested}/{TOTAL_DAYS}"
        )
        print("  " + "─" * 74)
        for result in queries.run_all(session.con, tables):
            print(result.render())
    finally:
        session.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
