"""Command-line entry point.

Three verbs, deliberately ordered by how much they change:

* ``doctor``   -- reports the installed PyIceberg's capabilities. Changes nothing.
* ``describe`` -- profiles a table: layout, blockers, warnings. Changes nothing.
* ``plan``     -- shows what would be rewritten and what is skipped. Changes nothing.
* ``compact``  -- rewrites and commits. Requires ``--yes``.

Connection details come from environment variables or flags so the same
invocation works from a shell, a cron entry, or a container.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import replace

from .capabilities import detect
from .catalog_import import config_from_catalog, load_catalog
from .compactor import CompactionBlocked, TableCompactor
from .config import CompactionConfig, MemoryMode
from .session import CatalogSession, S3Settings
from .tableconfig import DEFAULT_SETTINGS, PartitionEvolution, TableConfig


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if args.command == "doctor":
        caps = detect()
        print(caps.describe())
        reason = caps.unsupported_reason()
        print(f"\nusable: {reason is None}" + (f"\nreason: {reason}" if reason else ""))
        return 0 if reason is None else 1

    # These two need no catalog connection: they operate on files.
    if args.command == "from-catalog":
        return _from_catalog(args)
    if args.command == "validate-config":
        return _validate_config(args)

    try:
        session = _session_from(args)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    try:
        # The reclaim verbs take no compaction arguments, so they dispatch
        # before a compactor is built from flags they do not have.
        if args.command == "expire":
            return _expire(session, args)

        if args.command == "remove-orphans":
            return _remove_orphans(session, args)

        if args.command == "remove-dangling-deletes":
            return _remove_dangling_deletes(session, args)

        if args.command == "rewrite-manifests":
            return _rewrite_manifests(session, args)

        if args.command == "apply-properties":
            return _apply_properties(session, args)

        compactor = _compactor_for(session, args)

        if args.command == "describe":
            print(compactor.describe().summary())
            return 0

        if args.command == "plan":
            print(compactor.plan().describe())
            return 0

        if args.command == "compact":
            if not args.yes and not args.dry_run:
                print(
                    "compact rewrites data files and commits a snapshot. "
                    "Re-run with --yes, or use --dry-run to see the plan.",
                    file=sys.stderr,
                )
                return 2
            result = compactor.execute(dry_run=args.dry_run)
            print(result.describe())
            return 0
    except CompactionBlocked as exc:
        print(str(exc), file=sys.stderr)
        return 3
    finally:
        session.close()

    parser.error(f"unknown command {args.command}")
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icemaint", description=__doc__.splitlines()[0])
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="report the installed PyIceberg's capabilities")

    fc = sub.add_parser(
        "from-catalog", help="generate table-config.json from a Meltano/Singer catalog"
    )
    fc.add_argument("catalog", help="path to the Singer catalog JSON")
    fc.add_argument("-o", "--output", default="table-config.json")
    fc.add_argument("--namespace", help="Iceberg namespace for streams that do not name one")
    fc.add_argument(
        "--no-evolution",
        action="store_true",
        help="generate with days-to-months partition evolution disabled",
    )

    vc = sub.add_parser("validate-config", help="check a table-config.json")
    vc.add_argument("config", help="path to table-config.json")

    for name, help_text in [
        ("describe", "profile a table without changing it"),
        ("plan", "show what compaction would rewrite"),
        ("compact", "rewrite data files and commit"),
    ]:
        p = sub.add_parser(name, help=help_text)
        p.add_argument("table", help="table identifier, e.g. default.events")
        p.add_argument(
            "--table-config",
            help="path to table-config.json; supplies partitioning, ordering and "
            "evolution intent, overriding the individual flags below",
        )
        _add_catalog_args(p)
        _add_config_args(p)
        if name == "compact":
            p.add_argument("--yes", action="store_true", help="actually commit")
            p.add_argument("--dry-run", action="store_true", help="plan only")

    ex = sub.add_parser("expire", help="apply the retention policy and delete the files it orphans")
    ex.add_argument("table")
    ex.add_argument("--table-config", help="path to table-config.json")
    ex.add_argument("--yes", action="store_true", help="actually expire and delete")
    ex.add_argument("--max-snapshot-age-days", type=int)
    ex.add_argument("--min-snapshots-to-keep", type=int)
    _add_catalog_args(ex)

    ro = sub.add_parser("remove-orphans", help="delete unreferenced files under the table location")
    ro.add_argument("table")
    ro.add_argument("--table-config", help="path to table-config.json")
    ro.add_argument("--yes", action="store_true", help="actually delete")
    ro.add_argument(
        "--older-than-days",
        type=int,
        help="age guard. Must exceed the longest write the warehouse performs, since a "
        "file written but not yet committed looks exactly like an orphan. Default 3.",
    )
    _add_catalog_args(ro)

    dd = sub.add_parser(
        "remove-dangling-deletes",
        help="drop delete files that no longer apply to any live data file",
    )
    dd.add_argument("table")
    dd.add_argument("--table-config", help="path to table-config.json")
    dd.add_argument("--yes", action="store_true", help="actually commit the removal")
    _add_catalog_args(dd)

    rm = sub.add_parser(
        "rewrite-manifests", help="regroup manifest entries by partition so predicates prune"
    )
    rm.add_argument("table")
    rm.add_argument("--table-config", help="path to table-config.json")
    rm.add_argument("--yes", action="store_true", help="actually commit the rewrite")
    rm.add_argument("--min-input-manifests", type=int)
    _add_catalog_args(rm)

    ap = sub.add_parser(
        "apply-properties",
        help="set the metadata-retention table properties declared in table-config.json",
    )
    ap.add_argument("table")
    ap.add_argument("--table-config", help="path to table-config.json")
    ap.add_argument("--yes", action="store_true", help="actually set them")
    _add_catalog_args(ap)

    return parser


def _add_catalog_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("catalog")
    g.add_argument("--uri", default=os.environ.get("ICEMAINT_URI"), help="REST catalog endpoint")
    g.add_argument("--warehouse", default=os.environ.get("ICEMAINT_WAREHOUSE"))
    g.add_argument("--credential", default=os.environ.get("ICEMAINT_CREDENTIAL"))
    g.add_argument("--token", default=os.environ.get("ICEMAINT_TOKEN"))
    g.add_argument("--oauth2-server-uri", default=os.environ.get("ICEMAINT_OAUTH2_SERVER_URI"))
    g.add_argument("--scope", default=os.environ.get("ICEMAINT_SCOPE"))
    g.add_argument(
        "--local-warehouse",
        default=os.environ.get("ICEMAINT_LOCAL_WAREHOUSE"),
        help="path to a filesystem warehouse with a SQL catalog, instead of --uri",
    )

    s = p.add_argument_group("s3 / minio")
    s.add_argument("--s3-endpoint", default=os.environ.get("ICEMAINT_S3_ENDPOINT"))
    s.add_argument("--s3-access-key-id", default=os.environ.get("ICEMAINT_S3_ACCESS_KEY_ID"))
    s.add_argument(
        "--s3-secret-access-key", default=os.environ.get("ICEMAINT_S3_SECRET_ACCESS_KEY")
    )
    s.add_argument("--s3-region", default=os.environ.get("ICEMAINT_S3_REGION", "us-east-1"))


def _add_config_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("compaction")
    g.add_argument("--target-file-size-bytes", type=int)
    g.add_argument("--min-input-files", type=int, default=2)
    g.add_argument("--rewrite-all", action="store_true")
    g.add_argument(
        "--memory-mode", choices=[m.value for m in MemoryMode], default=MemoryMode.AUTO.value
    )
    g.add_argument("--memory-budget-bytes", type=int, default=1 << 30)
    g.add_argument("--temp-directory")
    g.add_argument(
        "--sort-by",
        dest="sort_expression",
        help="arbitrary DuckDB ORDER BY expression; output keeps sort_order_id = None",
    )
    g.add_argument(
        "--sort-by-table-order",
        action="store_true",
        help="order by the table's declared sort order and stamp its id",
    )
    g.add_argument("--branch", default="main")
    g.add_argument("--snapshot-operation", choices=["replace", "overwrite"], default="replace")
    g.add_argument("--dangling-delete-policy", choices=["report", "block"], default="report")


def _session_from(args: argparse.Namespace) -> CatalogSession:
    if args.local_warehouse:
        return CatalogSession.for_local(warehouse_path=args.local_warehouse)

    if not args.uri or not args.warehouse:
        raise ValueError(
            "need --uri and --warehouse (or ICEMAINT_URI / ICEMAINT_WAREHOUSE), "
            "or --local-warehouse for a filesystem catalog"
        )

    s3 = None
    if args.s3_endpoint:
        if not (args.s3_access_key_id and args.s3_secret_access_key):
            raise ValueError(
                "--s3-endpoint also needs --s3-access-key-id and --s3-secret-access-key"
            )
        s3 = S3Settings(
            endpoint=args.s3_endpoint,
            access_key_id=args.s3_access_key_id,
            secret_access_key=args.s3_secret_access_key,
            region=args.s3_region,
        )

    return CatalogSession.for_lakekeeper(
        uri=args.uri,
        warehouse=args.warehouse,
        credential=args.credential,
        token=args.token,
        oauth2_server_uri=args.oauth2_server_uri,
        scope=args.scope,
        s3=s3,
    )


def _operational_config(args: argparse.Namespace) -> CompactionConfig:
    """How the run executes, with no layout opinions -- those come from the file."""
    return CompactionConfig(
        rewrite_all=args.rewrite_all,
        memory_mode=MemoryMode(args.memory_mode),
        memory_budget_bytes=args.memory_budget_bytes,
        temp_directory=args.temp_directory,
        branch=args.branch,
        snapshot_operation=args.snapshot_operation,
        dangling_delete_policy=args.dangling_delete_policy,
    )


def _config_from(args: argparse.Namespace) -> CompactionConfig:
    return CompactionConfig(
        target_file_size_bytes=args.target_file_size_bytes,
        min_input_files=args.min_input_files,
        rewrite_all=args.rewrite_all,
        memory_mode=MemoryMode(args.memory_mode),
        memory_budget_bytes=args.memory_budget_bytes,
        temp_directory=args.temp_directory,
        sort_by_table_order=args.sort_by_table_order,
        sort_expression=args.sort_expression,
        branch=args.branch,
        snapshot_operation=args.snapshot_operation,
        dangling_delete_policy=args.dangling_delete_policy,
    )


def _compactor_for(session: CatalogSession, args: argparse.Namespace) -> TableCompactor:
    """A compactor driven by table-config.json when one is supplied."""
    if not args.table_config:
        return TableCompactor(session, args.table, _config_from(args))

    table_config = TableConfig.load(args.table_config)
    # The file owns layout; the flags still own how the run executes.
    return TableCompactor.from_table_config(
        session, args.table, table_config, base=_operational_config(args)
    )


def _retention_for(args: argparse.Namespace):
    """Retention settings from table-config.json, or the built-in defaults."""
    from .tableconfig import Retention

    if not args.table_config:
        return Retention()
    return TableConfig.load(args.table_config).for_table(args.table).retention


def _expire(session: CatalogSession, args: argparse.Namespace) -> int:
    from .expire import ExpiryAborted, RetentionPolicy, SnapshotExpirer

    settings = _retention_for(args).expire_snapshots
    if not settings.enabled:
        print(f"{args.table}: expire_snapshots is disabled in the table config")
        return 0

    tbl = session.table(args.table)
    policy = RetentionPolicy.resolve(
        dict(tbl.properties),
        max_snapshot_age_days=(
            args.max_snapshot_age_days
            if args.max_snapshot_age_days is not None
            else settings.max_snapshot_age_days
        ),
        min_snapshots_to_keep=(
            args.min_snapshots_to_keep
            if args.min_snapshots_to_keep is not None
            else settings.min_snapshots_to_keep
        ),
        max_ref_age_days=settings.max_ref_age_days,
    )

    try:
        result = SnapshotExpirer(policy, dry_run=not args.yes).run(tbl)
    except ExpiryAborted as exc:
        # The post-commit check found a doomed file still referenced. Snapshots
        # are gone, but no file was touched -- same exit code as the orphan
        # abort, because the operator response is identical: stop and look.
        print(f"aborted, nothing deleted: {exc}", file=sys.stderr)
        return 4

    print(result.describe())
    if not args.yes:
        print("\n  dry run -- re-run with --yes to expire and delete.")
    return 0


def _remove_orphans(session: CatalogSession, args: argparse.Namespace) -> int:
    from .orphans import OrphanCleaner, OrphanCleanupAborted

    settings = _retention_for(args).remove_orphan_files
    if not settings.enabled:
        print(f"{args.table}: remove_orphan_files is disabled in the table config")
        return 0

    older_than = (
        args.older_than_days if args.older_than_days is not None else settings.older_than_days
    )
    if older_than == 0:
        print(
            "warning: a zero-day age guard will delete files a writer may still be "
            "committing. Only safe when nothing else is writing.",
            file=sys.stderr,
        )

    try:
        cleaner = OrphanCleaner(older_than_days=older_than, dry_run=not args.yes)
    except ValueError as exc:
        # A bad guard is a usage error, not a crash. argparse accepts negative
        # ints happily, so this is the only place it can be caught.
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        result = cleaner.run(session.table(args.table))
    except OrphanCleanupAborted as exc:
        print(f"aborted, nothing deleted: {exc}", file=sys.stderr)
        return 4

    print(result.describe())
    if not args.yes:
        print("\n  dry run -- re-run with --yes to delete.")
    return 0


def _remove_dangling_deletes(session: CatalogSession, args: argparse.Namespace) -> int:
    from .deletes import DanglingDeleteCleaner, DanglingDeleteError

    settings = _retention_for(args).remove_dangling_deletes
    if not settings.enabled:
        print(f"{args.table}: remove_dangling_deletes is disabled in the table config")
        return 0

    try:
        result = DanglingDeleteCleaner(dry_run=not args.yes).run(session.table(args.table))
    except DanglingDeleteError as exc:
        print(f"aborted, nothing removed: {exc}", file=sys.stderr)
        return 4

    print(result.describe())
    if not args.yes and result.removed:
        print("\n  dry run -- re-run with --yes to commit the removal.")
    return 0


def _rewrite_manifests(session: CatalogSession, args: argparse.Namespace) -> int:
    from .manifests import ManifestRewriteError, ManifestRewriter

    settings = _retention_for(args).rewrite_manifests
    if not settings.enabled:
        print(f"{args.table}: rewrite_manifests is disabled in the table config")
        return 0

    minimum = (
        args.min_input_manifests
        if args.min_input_manifests is not None
        else settings.min_input_manifests
    )
    try:
        result = ManifestRewriter(min_input_manifests=minimum, dry_run=not args.yes).run(
            session.table(args.table)
        )
    except ManifestRewriteError as exc:
        print(f"aborted, nothing rewritten: {exc}", file=sys.stderr)
        return 4

    print(result.describe())
    if not args.yes and result.plan.worth_doing:
        print("\n  dry run -- re-run with --yes to commit the rewrite.")
    return 0


def _apply_properties(session: CatalogSession, args: argparse.Namespace) -> int:
    from .properties import apply_metadata_properties, unreferenced_metadata_files

    settings = _retention_for(args).metadata
    tbl = session.table(args.table)
    result = apply_metadata_properties(tbl, settings, dry_run=not args.yes)
    print(result.describe())

    if settings.previous_versions_max is not None and not settings.delete_after_commit:
        # The trap this verb exists to make visible: trimming the log without
        # enabling deletion manufactures orphans rather than reclaiming bytes.
        stranded = unreferenced_metadata_files(session.table(args.table))
        if stranded:
            print(
                f"  {stranded} metadata.json file(s) are already unreferenced. "
                "Trimming the log does not delete them -- run remove-orphans, or set "
                "delete_after_commit if a single process writes this table."
            )

    if not args.yes and result.changes:
        print("\n  dry run -- re-run with --yes to set them.")
    return 0


def _from_catalog(args: argparse.Namespace) -> int:
    catalog = load_catalog(args.catalog)
    defaults = None
    if args.no_evolution:
        defaults = replace(
            DEFAULT_SETTINGS, partition_evolution=PartitionEvolution(enabled=False, rules=())
        )

    config, report = config_from_catalog(
        catalog, namespace=args.namespace, defaults=defaults, source=args.catalog
    )
    config.dump(args.output)
    print(report.describe())
    print(f"wrote {args.output}")
    return 0


def _validate_config(args: argparse.Namespace) -> int:
    config = TableConfig.load(args.config)
    print(f"{args.config}: valid (version {config.version}, {len(config.tables)} table(s))")
    for identifier in sorted(config.tables):
        settings = config.for_table(identifier)
        parts = (
            ", ".join(f"{pf.column}:{pf.transform}" for pf in settings.partition) or "unpartitioned"
        )
        evolution = (
            ", ".join(
                f"{r.from_transform}->{r.to_transform}@{r.older_than_days}d"
                for r in settings.partition_evolution.rules
            )
            if settings.partition_evolution.enabled
            else "disabled"
        )
        print(f"  {identifier}: [{parts}] ordering={settings.ordering.mode} evolution={evolution}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
