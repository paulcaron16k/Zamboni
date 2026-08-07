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
from pathlib import Path

from . import maintainers, settings, version_banner
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

USAGE = """\
getting started
  zamboni doctor                          is this PyIceberg build usable?
  zamboni describe  acme.events           profile a table, change nothing
  zamboni maintenance acme.events         preview every operation on one table

daily operation -- see docs/devops.md
  zamboni maintenance --warehouse acme --status --yes

  With ./zamboni.yml and ./.env present, that is the whole cron line:

    17 2 * * *  cd /srv/zamboni && zamboni maintenance --warehouse acme --status \\
                  --yes >> /var/log/zamboni/acme.log 2>&1

  `maintenance` runs the six operations in the order docs/runbook.md
  establishes, over every configured table, and exits with the worst code any
  of them produced.

one rule, no exceptions
  Without --yes nothing is committed. Every mutating verb previews instead and
  says so. Where an engine cannot preview -- Trino previews nothing -- the run
  is refused rather than executed.

exit codes
  0 success   2 usage   3 refused (blocked table or unsupported operation)
  4 a safety check aborted the run; nothing was deleted. Investigate.

further reading
  docs/devops.md    cron, zamboni.yml, .env, multi-tenant warehouses
  docs/runbook.md   the six-verb order, cadence, sizing the orphan guard
  zamboni engines   what each engine supports, and what it refuses
"""


def _apply_profile(args: argparse.Namespace, profile) -> None:
    """Fill in what neither a flag nor the environment supplied.

    Only ever fills gaps. A flag beats the profile because a one-off run has to
    be able to override a committed file without editing it, and the flag
    defaults already carry the ZAMBONI_* values -- so anything still None here
    was specified nowhere else.
    """
    # uri and warehouse: fill only when unset. Their flag defaults already carry
    # ZAMBONI_URI / ZAMBONI_WAREHOUSE, so None here means nobody said.
    for attribute in ("uri", "warehouse"):
        if not getattr(args, attribute, None) and (value := getattr(profile, attribute)):
            setattr(args, attribute, value)

    # engine is different: its flag default is the literal "local", not None, so
    # "unset" and "explicitly local" are indistinguishable on the namespace. A
    # profile may therefore only *raise* it off the default -- which means
    # `--engine local` cannot override a profile that says trino. Acceptable:
    # the profile is the deployment's choice, and the flag can still name any
    # other engine. Recorded here because the asymmetry is not obvious.
    if getattr(args, "engine", None) == "local" and profile.engine != "local":
        args.engine = profile.engine

    # Per-warehouse table configuration, the multi-tenant layout in
    # docs/devops.md section 5: $ZAMBONI_ROOT/configs/{warehouse}/table-config.json
    if getattr(args, "table_config", None) is None and getattr(args, "warehouse", None):
        candidate = profile.table_config_for(args.warehouse)
        if candidate.is_file():
            args.table_config = str(candidate)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # Before anything reads an environment variable or a flag default: the
    # dotenv file has to be in os.environ first, and the profile supplies what
    # neither a flag nor the environment did.
    try:
        args.zamboni_profile, env_file = settings.resolve(
            profile_path=args.profile, env_path=args.env
        )
    except settings.ProfileError as exc:
        parser.error(str(exc))
        return 2
    _apply_profile(args, args.zamboni_profile)
    if args.verbose:
        logging.getLogger(__name__).debug(
            "profile: %s, env: %s", args.zamboni_profile.source, env_file
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
    if args.command == "table-config" and args.table_config_command == "validate":
        return _validate_config(args)
    if args.command == "table-config" and args.table_config_command == "summary":
        return _table_config_summary(args)

    try:
        session = _session_from(args)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    try:
        # The reclaim verbs take no compaction arguments, so they dispatch
        # before a compactor is built from flags they do not have.
        if args.command == "maintenance":
            return _maintenance(session, args)

        if args.command == "warehouses":
            return _warehouses(session, args)

        if args.command == "table-config":
            return _table_config_generate(session, args)

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
    parser = argparse.ArgumentParser(
        prog="zamboni",
        description=__doc__.splitlines()[0],
        epilog=USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--profile",
        help="non-secret configuration. Default: ./zamboni.yml, then "
        "$ZAMBONI_ROOT/zamboni.yml. See docs/devops.md.",
    )
    parser.add_argument(
        "--env",
        help="dotenv file holding credentials. Default: ./.env. Cron gives a job "
        "almost no environment, and a crontab is a poor place for a secret.",
    )
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

    # `table-config` groups the three things an operator does to that file.
    # `validate-config` stays as it is: it shipped in 0.1.0 and removing a verb
    # is breaking under docs/releasing.md, which for this tool is not a cost
    # worth paying to tidy a name. It is now an alias of `table-config validate`
    # rather than a second implementation.
    tc = sub.add_parser("table-config", help="create, check and explain a table-config.json")
    tc_sub = tc.add_subparsers(dest="table_config_command", required=True)

    tcg = tc_sub.add_parser(
        "generate",
        help="write a starter table-config.json describing the catalog as it is today",
    )
    tcg.add_argument("-o", "--output", default="table-config.json")
    tcg.add_argument("--namespace", help="only this namespace; default is every table")
    tcg.add_argument("--force", action="store_true", help="overwrite an existing output file")
    _add_catalog_args(tcg)

    tcv = tc_sub.add_parser(
        "validate", help="check that a table-config.json parses and means something"
    )
    tcv.add_argument("config", help="path to table-config.json")

    tcs = tc_sub.add_parser(
        "summary",
        help="what this config would do, per table, including the defaults you did not write",
    )
    tcs.add_argument("config", help="path to table-config.json")
    tcs.add_argument("--table", help="only this table")

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

    mt = sub.add_parser(
        "maintenance",
        help="run every operation, in the runbook order, over every configured table",
        description=(
            "The DevOps entry point: one command a cron line can call. Runs the six "
            "operations in the order docs/runbook.md establishes, over every table in "
            "the configuration, and exits with the worst code any of them produced. "
            "See docs/devops.md for the crontab line and the multi-tenant layout."
        ),
    )
    mt.add_argument("table", nargs="?", help="one table; default is every configured table")
    mt.add_argument("--table-config", help="path to table-config.json")
    mt.add_argument(
        "--status",
        action="store_true",
        help="report file counts and bytes before and after, so a nightly log answers "
        "'did it help' without a second tool",
    )
    mt.add_argument(
        "--yes",
        action="store_true",
        help="actually commit every operation. Without it this is a dry run.",
    )
    mt.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    mt.add_argument("--max-snapshot-age-days", type=int)
    mt.add_argument("--min-snapshots-to-keep", type=int)
    mt.add_argument("--older-than-days", type=int)
    mt.add_argument("--min-input-manifests", type=int)
    _add_engine_arg(mt)
    _add_catalog_args(mt)
    _add_config_args(mt)

    wh = sub.add_parser(
        "warehouses",
        help="list the warehouses this catalog knows about, one per line",
        description=(
            "Plain output on purpose: its job is to be input to something else -- "
            "generating a crontab, a CronJob per tenant, an Airflow DAG. Zamboni does "
            "not schedule anything; see docs/devops.md section 5."
        ),
    )
    _add_catalog_args(wh)

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
    # Spark had no flags at all until ZMBNI-913 -- the maintainer read `remote`,
    # `master` and `catalog` from its options, and nothing on the CLI ever put
    # them there. `--engine spark` was reachable and unconfigurable.
    p.add_argument(
        "--spark-remote",
        default=os.environ.get("ZAMBONI_SPARK_REMOTE"),
        help="Spark Connect endpoint, e.g. sc://localhost:15002. Needs no JVM "
        "here; the server owns the Iceberg extensions and the S3 credentials "
        "`remove-orphans` lists with.",
    )
    p.add_argument(
        "--spark-master",
        default=os.environ.get("ZAMBONI_SPARK_MASTER"),
        help="Spark master for a local driver, e.g. local[*]. Mutually "
        "exclusive with --spark-remote.",
    )
    p.add_argument(
        "--spark-catalog",
        default=os.environ.get("ZAMBONI_SPARK_CATALOG"),
        help="the Spark catalog holding the Iceberg tables. Default `iceberg`.",
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
    engine = getattr(args, "engine", "local")
    # Per engine, not merged: `catalog` means a different thing to each, and one
    # flat dict made --trino-catalog configure Spark. It did, before ZMBNI-913.
    by_engine = {
        "trino": (
            ("host", "trino_host"),
            ("port", "trino_port"),
            ("user", "trino_user"),
            ("catalog", "trino_catalog"),
            ("version", "trino_version"),
        ),
        "spark": (
            ("remote", "spark_remote"),
            ("master", "spark_master"),
            ("catalog", "spark_catalog"),
        ),
    }
    options = {
        key: value
        for key, attribute in by_engine.get(engine, ())
        if (value := getattr(args, attribute, None))
    }
    return maintainers.get(engine)(session, options)


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
    if args.command in ("compact", "maintenance"):
        if table_config is None:
            compaction = _config_from(args)
        else:
            # Translate the file's declared *layout* -- ordering, sizing -- on top
            # of the operational flags. Passing only `_operational_config` here
            # meant `ordering.mode: zorder` never left the config file for any
            # non-local engine, because `config_from_table_settings` is otherwise
            # reached solely through `TableCompactor.from_table_config`. Spark is
            # the one other engine that can Z-order, so the capability was real
            # in the maintainer and unreachable from the CLI.
            from .config import config_from_table_settings

            table = getattr(args, "table", None)
            settings = (
                table_config.for_table(table)
                if table and table in table_config.tables
                else DEFAULT_SETTINGS
            )
            compaction = config_from_table_settings(settings, _operational_config(args))
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


def _maintenance(session: CatalogSession, args: argparse.Namespace) -> int:
    """Every operation, in the runbook order, over every configured table.

    One exit code: the **worst** any operation produced, so a partial failure is
    never reported as success. That matters more here than anywhere else in the
    CLI, because this is the entry point a cron line calls and nobody reads.
    """
    from .maintainers import Operation

    profile = args.zamboni_profile
    tables = _tables_to_maintain(args, profile)
    if not tables:
        print(
            "no tables to maintain. Name one on the command line, list them in "
            f"{profile.source or 'zamboni.yml'}, or point --table-config at a config "
            "that declares some."
        )
        return 2

    operations = [Operation(name) for name in profile.operations]
    before = _status_snapshot(session, tables) if args.status else None

    worst = 0
    failures: list[str] = []
    for table in tables:
        print(f"\n{table}")
        print("  " + "-" * 68)
        done: set = set()
        for operation in operations:
            if skipped_by := _already_fulfilled(session, args, operation, done):
                # `fulfilled_by` is not decoration. On Spark, dangling-delete
                # removal *is* an option of rewrite_data_files, so running both
                # would compact the table twice -- the second time to no effect.
                print(f"  {operation.value}: already done by {skipped_by.value}")
                continue
            code = _run_one(session, args, operation, table)
            done.add(operation)
            worst = max(worst, code)
            if code:
                failures.append(f"{table} {operation.value} (exit {code})")
                if code == 4:
                    # A safety check aborted. Everything after it on this table
                    # reads the same state, so continuing would be doing more
                    # work on a warehouse we have just said we do not trust.
                    print(f"  stopping this table: {operation.value} aborted")
                    break

    if before is not None:
        _print_status_delta(session, tables, before)

    if failures:
        print("\nfailed:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
    return worst


def _already_fulfilled(session: CatalogSession, args: argparse.Namespace, operation, done: set):
    """The operation this one rides on, if that already ran in this sequence.

    Declared per engine via ``OperationSupport.fulfilled_by``. Only skips when
    the fulfilling operation actually ran: a profile listing
    `remove-dangling-deletes` without `compact` still gets its work done.
    """
    # No try/except. An earlier version swallowed everything here with the
    # comment "capability lookup must not fail a run", which was not true: the
    # very next step calls the same `capabilities()` through `check_supported`,
    # unguarded, so a genuine failure surfaced moments later anyway -- with a
    # worse stack trace, and after silently skipping the skip. Worse, returning
    # None on error means "not fulfilled", so a lookup failure would cause the
    # double-run this function exists to prevent: a silent wrong answer where a
    # refusal belongs.
    support = _maintainer_for(session, args).capabilities().of(operation)
    if support.fulfilled_by and support.fulfilled_by in done:
        return support.fulfilled_by
    return None


def _tables_to_maintain(args: argparse.Namespace, profile) -> list[str]:
    """Explicit argument, then the profile, then everything in table-config."""
    if getattr(args, "table", None):
        return [args.table]
    if profile.tables:
        return list(profile.tables)
    if args.table_config:
        return sorted(TableConfig.load(args.table_config).tables)
    return []


def _run_one(session: CatalogSession, args: argparse.Namespace, operation, table: str) -> int:
    """One operation, reusing the single-verb handler so behaviour cannot drift.

    Dispatching to the same functions the individual verbs use is the point: a
    second implementation of "expire" that maintenance calls would be a second
    thing to keep correct.
    """
    handlers = {
        "compact": _compact_one,
        "expire": _expire,
        "remove-orphans": _remove_orphans,
        "remove-dangling-deletes": _remove_dangling_deletes,
        "rewrite-manifests": _rewrite_manifests,
        "apply-properties": _apply_properties,
    }
    scoped = argparse.Namespace(**vars(args))
    scoped.table = table
    try:
        return handlers[operation.value](session, scoped)
    except CompactionBlocked as exc:
        print(f"  {operation.value}: {exc}", file=sys.stderr)
        return 3
    except UnsupportedOperation as exc:
        # Declared, not a failure: Trino cannot remove dangling deletes, and a
        # fleet run should skip that rather than fail every night over it.
        print(f"  {operation.value}: skipped -- {exc}")
        return 0
    except (PreviewUnavailable, EngineConfigProblem) as exc:
        print(f"  {operation.value}: {exc}", file=sys.stderr)
        return 2


def _compact_one(session: CatalogSession, args: argparse.Namespace) -> int:
    from .maintainers import Operation

    dry_run = args.dry_run or not args.yes
    maintainer, request = _prepare(session, args, Operation.COMPACT)
    result = maintainer.execute(Operation.COMPACT, args.table, request=request, dry_run=dry_run)
    print(result.describe())
    if dry_run:
        print("\n  dry run -- re-run with --yes to rewrite and commit.")
    return 0


def _status_snapshot(session: CatalogSession, tables: list[str]) -> dict[str, tuple]:
    from .profile import profile_table

    snapshot = {}
    for table in tables:
        try:
            p = profile_table(session.table(table))
            snapshot[table] = (len(p.live_files), p.total_bytes, p.total_records)
        except Exception as exc:
            # Status is reporting, not the job. A table it cannot read must not
            # turn a successful maintenance run into a failure.
            logging.getLogger(__name__).debug("status unavailable for %s: %s", table, exc)
    return snapshot


def _print_status_delta(
    session: CatalogSession, tables: list[str], before: dict[str, tuple]
) -> None:
    from .units import human_bytes

    after = _status_snapshot(session, tables)
    print("\nstatus")
    print("  " + "-" * 68)
    print(f"  {'table':<34} {'files':>14} {'bytes':>18}")
    for table in tables:
        was, now = before.get(table), after.get(table)
        if not was or not now:
            continue
        print(
            f"  {table:<34} {was[0]:>6} -> {now[0]:<6} "
            f"{human_bytes(was[1]):>8} -> {human_bytes(now[1]):<8}"
        )
        if was[2] != now[2]:
            # Row counts must not move. Saying so loudly beats a silent diff.
            print(
                f"    ROW COUNT CHANGED: {was[2]} -> {now[2]}. Maintenance must never "
                "do this; investigate before running again.",
                file=sys.stderr,
            )


def _warehouses(session: CatalogSession, args: argparse.Namespace) -> int:
    """List the catalog's warehouses, one per line.

    Deliberately plain: no header, no counts, no formatting. This is the input
    to a crontab generator or a CronJob template, and anything decorative here
    becomes something the caller has to strip.
    """
    names = session.warehouses()
    if not names:
        print(
            "no warehouses reported. This catalog may not expose a management API; "
            "list them from your provisioning system instead.",
            file=sys.stderr,
        )
        return 0
    for name in names:
        print(name)
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


def _table_config_generate(session: CatalogSession, args: argparse.Namespace) -> int:
    """A starter config that describes the catalog **as it is today**.

    Deliberately descriptive rather than aspirational. Every table gets the
    default settings plus its *current* partition spec written out, so the first
    run of `zamboni maintenance` against the generated file changes nothing but
    file sizes -- an operator can diff their intent against reality instead of
    discovering it during a run that deletes things.

    `from-catalog` is the other direction: a Singer catalog describes tables
    that may not exist yet. This one reads a live Iceberg catalog.
    """
    from pyiceberg.exceptions import NoSuchTableError

    from .orphans import _all_table_identifiers
    from .tableconfig import DEFAULT_SETTINGS, PartitionField, TableConfig

    output = Path(args.output)
    if output.exists() and not args.force:
        print(f"{output} exists; pass --force to overwrite", file=sys.stderr)
        return 2

    identifiers = _all_table_identifiers(session.catalog)
    if args.namespace:
        wanted = tuple(args.namespace.split("."))
        identifiers = [i for i in identifiers if i[: len(wanted)] == wanted]

    tables, skipped = {}, []
    for identifier in sorted(identifiers):
        name = ".".join(identifier)
        try:
            tbl = session.catalog.load_table(identifier)
        except NoSuchTableError:  # dropped between listing and loading
            skipped.append((name, "no longer exists"))
            continue
        # V1 is refused by every operation, so writing it into a config would
        # generate a file whose first run errors on a table nobody can maintain.
        if tbl.metadata.format_version < 2:
            skipped.append((name, "format version 1, which Zamboni refuses"))
            continue
        partition = tuple(
            PartitionField(
                column=tbl.schema().find_column_name(f.source_id) or "?", transform=str(f.transform)
            )
            for f in tbl.spec().fields
        )
        tables[name] = replace(DEFAULT_SETTINGS, partition=partition)

    config = TableConfig(tables=tables, defaults=DEFAULT_SETTINGS)
    config.validate()
    config.dump(str(output))

    print(f"wrote {output}: {len(tables)} table(s)")
    for name, reason in skipped:
        print(f"  skipped {name}: {reason}")
    print("\nThis describes the catalog as it is now. Edit it to say what you want,")
    print("then: zamboni table-config summary " + str(output))
    return 0


#: What an unset expiry knob resolves to, named rather than printed as `None`.
#: Kept beside the summary because it is documentation; the resolution itself
#: lives in expire.py.
_UNSET_AGE = "unset -> history.expire.max-snapshot-age-ms, else 5"
_UNSET_KEEP = "unset -> history.expire.min-snapshots-to-keep, else 1"


def _table_config_summary(args: argparse.Namespace) -> int:
    """What the file *does*, including the defaults nobody wrote down.

    `validate` answers "does this parse". This answers the question an operator
    actually has before a run that deletes files: which operations are on, what
    windows they use, and where a value came from when it is not in the file.
    """
    from .tableconfig import DEFAULT_SETTINGS, TableConfig

    config = TableConfig.load(args.config)
    names = sorted(config.tables)
    if args.table:
        if args.table not in config.tables:
            print(
                f"{args.table} is not in {args.config}; it would get the defaults "
                f"({len(config.tables)} table(s) are named)",
                file=sys.stderr,
            )
            return 2
        names = [args.table]

    print(f"{args.config}: version {config.version}, {len(config.tables)} table(s)")
    print("Values not written in the file are marked (default).\n")

    def mark(value, fallback, *, unset: str = "") -> str:
        """Render a setting, saying where it came from.

        `None` is not "no value": it means the knob was left out, and expiry
        then falls back to the Iceberg table property and only then to the spec
        default. A bare `None` told an operator nothing and read like a bug --
        naming the fallback is the whole point of a summary.
        """
        if value is None:
            return unset or "unset"
        return f"{value}" if value != fallback else f"{value} (default)"

    for name in names:
        s = config.for_table(name)
        d = DEFAULT_SETTINGS
        print(f"{name}")
        parts = (
            "partition [" + ", ".join(f"{pf.column}:{pf.transform}" for pf in s.partition) + "]"
            if s.partition
            else "unpartitioned"
        )
        print(f"  layout     {parts}, ordering {mark(s.ordering.mode, d.ordering.mode)}")
        if s.ordering.mode == "zorder" and s.ordering.zorder:
            print(
                f"             zorder columns {', '.join(s.ordering.zorder.columns)}"
                "  -- local and spark only; trino cannot do this"
            )

        r = s.retention
        expire = "on" if r.expire_snapshots.enabled else "OFF"
        de = d.retention.expire_snapshots
        print(
            f"  expire     {expire}, "
            "keep "
            + mark(
                r.expire_snapshots.max_snapshot_age_days,
                de.max_snapshot_age_days,
                unset=_UNSET_AGE,
            )
            + " day(s), minimum "
            + mark(
                r.expire_snapshots.min_snapshots_to_keep,
                de.min_snapshots_to_keep,
                unset=_UNSET_KEEP,
            )
            + " snapshot(s)"
        )
        orphans = "on" if r.remove_orphan_files.enabled else "OFF"
        do = d.retention.remove_orphan_files
        print(
            f"  orphans    {orphans}, files older than "
            f"{mark(r.remove_orphan_files.older_than_days, do.older_than_days)} day(s) "
            "-- this deletes storage; the guard is what stands between it and a live write"
        )
        dangling = "on" if r.remove_dangling_deletes.enabled else "OFF"
        print(f"  dangling   {dangling}")
        manifests = "on" if r.rewrite_manifests.enabled else "OFF"
        print(f"  manifests  {manifests}")
        if r.metadata.previous_versions_max is None and r.metadata.delete_after_commit is None:
            print("  metadata   unset; apply-properties has nothing to set and will refuse")
        else:
            print(
                f"  metadata   keep {r.metadata.previous_versions_max} previous version(s), "
                f"delete after commit {r.metadata.delete_after_commit}"
            )
        ev = s.partition_evolution
        if ev.enabled and ev.rules:
            rules = ", ".join(
                f"{r_.from_transform}->{r_.to_transform} after {r_.older_than_days}d"
                for r_ in ev.rules
            )
            print(f"  evolution  {rules}  -- local engine only")
        else:
            print("  evolution  disabled")
        print()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
