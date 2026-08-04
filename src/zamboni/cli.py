"""Command-line entry point.

The verbs, ordered by how much they change:

Never touch a table -- these take no ``--yes``:

* ``doctor``          -- reports the installed PyIceberg's capabilities.
* ``engines``         -- reports what each engine supports, and what it refuses.
* ``describe``        -- profiles a table: layout, blockers, warnings.
* ``plan``            -- shows what compaction would rewrite, and what it skips.
* ``validate-config`` -- loads a ``table-config.json`` and reports what it means.
* ``from-catalog``    -- derives a starting config from a Singer catalog. Opens no
  catalog connection and reads no table; it does write a local file, defaulting to
  ``table-config.json``.

Mutating -- **one rule, no exceptions: without ``--yes``, nothing is committed.**
Every one of these previews instead, and says on stdout that it did. That notice
is unconditional, including on a run that found nothing to do: three of these
verbs used to print it only when they had work, which made the rule visible on
some runs and not others.

* ``compact``                 -- rewrites data files into target-sized ones.
* ``expire``                  -- applies the retention policy, deletes what it orphans.
* ``remove-orphans``          -- deletes unreferenced files past the age guard.
* ``remove-dangling-deletes`` -- drops delete files no data file can match.
* ``rewrite-manifests``       -- regroups manifests without touching data.
* ``apply-properties``        -- sets the Iceberg table properties from the config.

That rule is why ``compact`` takes both ``--yes`` and ``--dry-run``: the second is
redundant, kept because scripts pass it to say what they mean.

Each mutating verb takes ``--engine`` (default ``local``). The rule above holds on
every engine: where one cannot preview an operation -- Trino previews nothing --
a run without ``--yes`` is *refused*, rather than executed or dressed up as a dry
run it did not perform. See :mod:`zamboni.maintainers`.

Connection details come from environment variables or flags so the same
invocation works from a shell, a cron entry, or a container.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import replace

from . import maintainers, version_banner
from .capabilities import detect
from .catalog_import import config_from_catalog, load_catalog
from .compactor import CompactionBlocked, TableCompactor
from .config import CompactionConfig, MemoryMode
from .maintainers import (
    EngineConfigProblem,
    MaintenanceRequest,
    Operation,
    PreviewUnavailable,
    UnsupportedOperation,
)
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

    if args.command == "engines":
        for name in maintainers.available():
            print(maintainers.get(name).capabilities().describe())
            print()
        return 0

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

        if args.command in ("describe", "plan"):
            # Still built directly: both are read-only profiling of the local
            # table state, not operations an engine performs.
            compactor = _compactor_for(session, args)

        if args.command == "describe":
            print(compactor.describe().summary())
            return 0

        if args.command == "plan":
            print(compactor.plan().describe())
            return 0

        if args.command == "compact":
            # A bare invocation never mutates, matching the other five verbs.
            # `compact` used to exit 2 here instead, which meant the one command
            # people reach for first behaved unlike everything else -- and the
            # runbook had to explain the difference rather than state a rule.
            dry_run = args.dry_run or not args.yes
            maintainer, request = _prepare(session, args, Operation.COMPACT)
            result = maintainer.execute(
                Operation.COMPACT, args.table, request=request, dry_run=dry_run
            )
            print(result.describe())
            if dry_run:
                print("\n  dry run -- re-run with --yes to rewrite and commit.")
            return 0
    except CompactionBlocked as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except UnsupportedOperation as exc:
        # A refusal, not a failure -- same class as a blocked table, so the
        # same exit code and the same operator response: read the reason.
        print(str(exc), file=sys.stderr)
        return 3
    except (PreviewUnavailable, EngineConfigProblem) as exc:
        # Fixable by changing the invocation or the config, so a usage error.
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        session.close()

    parser.error(f"unknown command {args.command}")
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="zamboni", description=__doc__.splitlines()[0])
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--version", action="version", version=version_banner())
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="report the installed PyIceberg's capabilities")
    sub.add_parser("engines", help="report what each engine supports, and what it refuses to do")

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
            _add_engine_arg(p)
            p.add_argument(
                "--yes",
                action="store_true",
                help="actually rewrite and commit. Without it this is a dry run.",
            )
            p.add_argument(
                "--dry-run",
                action="store_true",
                help="preview without committing. Omitting --yes does the same; this says it.",
            )

    ex = sub.add_parser("expire", help="apply the retention policy and delete the files it orphans")
    ex.add_argument("table")
    ex.add_argument("--table-config", help="path to table-config.json")
    _add_engine_arg(ex)
    ex.add_argument(
        "--yes",
        action="store_true",
        help="actually expire and delete. Without it this is a dry run.",
    )
    ex.add_argument("--max-snapshot-age-days", type=int)
    ex.add_argument("--min-snapshots-to-keep", type=int)
    _add_catalog_args(ex)

    ro = sub.add_parser("remove-orphans", help="delete unreferenced files under the table location")
    ro.add_argument("table")
    ro.add_argument("--table-config", help="path to table-config.json")
    _add_engine_arg(ro)
    ro.add_argument(
        "--yes",
        action="store_true",
        help="actually delete. Without it this is a dry run.",
    )
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
    _add_engine_arg(dd)
    dd.add_argument(
        "--yes",
        action="store_true",
        help="actually commit the removal. Without it this is a dry run.",
    )
    _add_catalog_args(dd)

    rm = sub.add_parser(
        "rewrite-manifests", help="regroup manifest entries by partition so predicates prune"
    )
    rm.add_argument("table")
    rm.add_argument("--table-config", help="path to table-config.json")
    _add_engine_arg(rm)
    rm.add_argument(
        "--yes",
        action="store_true",
        help="actually commit the rewrite. Without it this is a dry run.",
    )
    rm.add_argument("--min-input-manifests", type=int)
    _add_catalog_args(rm)

    ap = sub.add_parser(
        "apply-properties",
        help="set the metadata-retention table properties declared in table-config.json",
    )
    ap.add_argument("table")
    ap.add_argument("--table-config", help="path to table-config.json")
    _add_engine_arg(ap)
    ap.add_argument(
        "--yes",
        action="store_true",
        help="actually set them. Without it this is a dry run.",
    )
    _add_catalog_args(ap)

    return parser


def _add_engine_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--engine",
        default="local",
        choices=maintainers.available(),
        help="which engine performs the operation. `zamboni engines` reports what "
        "each one supports, and what it refuses.",
    )
    # Connection details for a non-local engine. Environment fallbacks match the
    # ZAMBONI_* pattern the catalog flags already use, so a cron entry needs no
    # flags at all.
    p.add_argument("--trino-host", default=os.environ.get("ZAMBONI_TRINO_HOST"))
    p.add_argument("--trino-port", default=os.environ.get("ZAMBONI_TRINO_PORT"))
    p.add_argument("--trino-user", default=os.environ.get("ZAMBONI_TRINO_USER"))
    p.add_argument(
        "--trino-version",
        default=os.environ.get("ZAMBONI_TRINO_VERSION"),
        help="server version, e.g. 479. Gates arguments that only newer Trino "
        "accepts; unknown means the tool assumes the older behaviour.",
    )
    p.add_argument(
        "--trino-catalog",
        default=os.environ.get("ZAMBONI_TRINO_CATALOG"),
        help="the Trino catalog holding the Iceberg tables. Default `iceberg`.",
    )


def _add_catalog_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("catalog")
    g.add_argument("--uri", default=os.environ.get("ZAMBONI_URI"), help="REST catalog endpoint")
    g.add_argument("--warehouse", default=os.environ.get("ZAMBONI_WAREHOUSE"))
    g.add_argument("--credential", default=os.environ.get("ZAMBONI_CREDENTIAL"))
    g.add_argument("--token", default=os.environ.get("ZAMBONI_TOKEN"))
    g.add_argument("--oauth2-server-uri", default=os.environ.get("ZAMBONI_OAUTH2_SERVER_URI"))
    g.add_argument("--scope", default=os.environ.get("ZAMBONI_SCOPE"))
    g.add_argument(
        "--local-warehouse",
        default=os.environ.get("ZAMBONI_LOCAL_WAREHOUSE"),
        help="path to a filesystem warehouse with a SQL catalog, instead of --uri",
    )

    s = p.add_argument_group("s3 / minio")
    s.add_argument("--s3-endpoint", default=os.environ.get("ZAMBONI_S3_ENDPOINT"))
    s.add_argument("--s3-access-key-id", default=os.environ.get("ZAMBONI_S3_ACCESS_KEY_ID"))
    s.add_argument("--s3-secret-access-key", default=os.environ.get("ZAMBONI_S3_SECRET_ACCESS_KEY"))
    s.add_argument("--s3-region", default=os.environ.get("ZAMBONI_S3_REGION", "us-east-1"))


def _add_config_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("compaction")
    g.add_argument("--target-file-size-bytes", type=int)
    g.add_argument("--min-input-files", type=int, default=2)
    g.add_argument("--rewrite-all", action="store_true")
    g.add_argument(
        "--partial-progress",
        action="store_true",
        help="commit each rewrite group as it completes instead of the whole run at "
        "once. Mirrors Iceberg's partial-progress.enabled: more commits, but a "
        "failure leaves earlier groups compacted rather than redoing everything.",
    )
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
            "need --uri and --warehouse (or ZAMBONI_URI / ZAMBONI_WAREHOUSE), "
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
        partial_progress=args.partial_progress,
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
        partial_progress=args.partial_progress,
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


def _maintainer_for(session: CatalogSession, args: argparse.Namespace):
    """The engine named by ``--engine``, defaulting to the local one."""
    options = {
        key: value
        for key, value in (
            ("host", getattr(args, "trino_host", None)),
            ("port", getattr(args, "trino_port", None)),
            ("user", getattr(args, "trino_user", None)),
            ("catalog", getattr(args, "trino_catalog", None)),
            ("version", getattr(args, "trino_version", None)),
        )
        if value
    }
    return maintainers.get(getattr(args, "engine", "local"))(session, options)


def _request_for(args: argparse.Namespace) -> MaintenanceRequest:
    """Engine-neutral inputs: the declarative settings plus the CLI overrides.

    Deliberately not a built compactor or a resolved RetentionPolicy -- those
    are the local engine's vocabulary, and handing them to a maintainer would
    make every other engine translate *out of* ours instead of *from* the
    config.
    """
    table_config = TableConfig.load(args.table_config) if args.table_config else None
    # Mirrors what _compactor_for did: without a config file the flags *are* the
    # layout, so the full flag-derived config is used; with one, the file owns
    # layout and only the operational half comes from the flags. Only compact
    # needs either -- the reclaim verbs take no compaction arguments at all.
    compaction = None
    if args.command == "compact":
        compaction = _operational_config(args) if table_config else _config_from(args)
    return MaintenanceRequest(
        retention=_retention_for(args),
        compaction=compaction,
        table_config=table_config,
        max_snapshot_age_days=getattr(args, "max_snapshot_age_days", None),
        min_snapshots_to_keep=getattr(args, "min_snapshots_to_keep", None),
        older_than_days=getattr(args, "older_than_days", None),
        min_input_manifests=getattr(args, "min_input_manifests", None),
    )


def _prepare(session: CatalogSession, args: argparse.Namespace, operation: Operation):
    """Everything that must hold before an engine is allowed to run.

    In this order deliberately: an unsupported operation is refused before its
    config is validated, and both happen before consent is considered, so the
    message an operator gets names the most fundamental problem rather than the
    first one a code path happens to reach.
    """
    maintainer = _maintainer_for(session, args)
    request = _request_for(args)

    maintainer.check_supported(operation)
    if problems := maintainer.validate(operation, request):
        raise EngineConfigProblem(
            f"this configuration cannot run {operation.value} on {maintainer.name}:\n  - "
            + "\n  - ".join(problems)
        )
    maintainer.check_consent(operation, yes=args.yes)
    return maintainer, request


def _retention_for(args: argparse.Namespace):
    """Retention settings from table-config.json, or the built-in defaults."""
    from .tableconfig import Retention

    if not args.table_config:
        return Retention()
    return TableConfig.load(args.table_config).for_table(args.table).retention


def _expire(session: CatalogSession, args: argparse.Namespace) -> int:
    from .expire import ExpiryAborted

    if not _retention_for(args).expire_snapshots.enabled:
        print(f"{args.table}: expire_snapshots is disabled in the table config")
        return 0

    maintainer, request = _prepare(session, args, Operation.EXPIRE)
    try:
        result = maintainer.execute(
            Operation.EXPIRE, args.table, request=request, dry_run=not args.yes
        )
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
    from .orphans import OrphanCleanupAborted

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

    maintainer, request = _prepare(session, args, Operation.REMOVE_ORPHANS)
    try:
        result = maintainer.execute(
            Operation.REMOVE_ORPHANS, args.table, request=request, dry_run=not args.yes
        )
    except ValueError as exc:
        # A bad guard is a usage error, not a crash. argparse accepts negative
        # ints happily, so this is the only place it can be caught.
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except OrphanCleanupAborted as exc:
        print(f"aborted, nothing deleted: {exc}", file=sys.stderr)
        return 4

    print(result.describe())
    if not args.yes:
        print("\n  dry run -- re-run with --yes to delete.")
    return 0


def _remove_dangling_deletes(session: CatalogSession, args: argparse.Namespace) -> int:
    from .deletes import DanglingDeleteError

    if not _retention_for(args).remove_dangling_deletes.enabled:
        print(f"{args.table}: remove_dangling_deletes is disabled in the table config")
        return 0

    maintainer, request = _prepare(session, args, Operation.REMOVE_DANGLING_DELETES)
    try:
        result = maintainer.execute(
            Operation.REMOVE_DANGLING_DELETES, args.table, request=request, dry_run=not args.yes
        )
    except DanglingDeleteError as exc:
        print(f"aborted, nothing removed: {exc}", file=sys.stderr)
        return 4

    print(result.describe())
    if not args.yes:
        print("\n  dry run -- re-run with --yes to commit the removal.")
    return 0


def _rewrite_manifests(session: CatalogSession, args: argparse.Namespace) -> int:
    from .manifests import ManifestRewriteError

    if not _retention_for(args).rewrite_manifests.enabled:
        print(f"{args.table}: rewrite_manifests is disabled in the table config")
        return 0

    maintainer, request = _prepare(session, args, Operation.REWRITE_MANIFESTS)
    try:
        result = maintainer.execute(
            Operation.REWRITE_MANIFESTS, args.table, request=request, dry_run=not args.yes
        )
    except ManifestRewriteError as exc:
        print(f"aborted, nothing rewritten: {exc}", file=sys.stderr)
        return 4

    print(result.describe())
    if not args.yes:
        print("\n  dry run -- re-run with --yes to commit the rewrite.")
    return 0


def _apply_properties(session: CatalogSession, args: argparse.Namespace) -> int:
    from .properties import unreferenced_metadata_files

    settings = _retention_for(args).metadata
    maintainer, request = _prepare(session, args, Operation.APPLY_PROPERTIES)
    result = maintainer.execute(
        Operation.APPLY_PROPERTIES, args.table, request=request, dry_run=not args.yes
    )
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

    if not args.yes:
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
