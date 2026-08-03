#!/usr/bin/env -S uv run --script
"""Run every maintenance operation against a live Lakekeeper + MinIO stack.

Everything else in this package is verified against a SQL catalog over a local
directory. That covers the logic but not the two things only a real deployment
answers: whether the REST catalog accepts the snapshots we build, and whether
the S3 credentials permit a bucket **listing** -- which orphan removal needs and
no other operation does.

Scoped deliberately: it creates its own namespace, works only inside it, and
drops it at the end. It never touches a table it did not create.

    uv run scripts/verify-live.py --warehouse acme_db
    uv run scripts/verify-live.py --warehouse acme_db --keep   # leave it behind
"""

from __future__ import annotations

import argparse
import socket
import sys
import traceback
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

from icemaint import CatalogSession, CompactionConfig, S3Settings, TableCompactor
from icemaint.deletes import DanglingDeleteCleaner, find_dangling
from icemaint.expire import RetentionPolicy, SnapshotExpirer
from icemaint.manifests import ManifestRewriter
from icemaint.orphans import OrphanCleaner, list_storage, storage_roots
from icemaint.profile import profile_table
from icemaint.properties import apply_metadata_properties
from icemaint.reachable import reachable_files
from icemaint.tableconfig import MetadataSettings


def map_host(alias: str, target: str) -> None:
    """Resolve `alias` to `target` for this process only.

    Lakekeeper returns the S3 endpoint from the *warehouse storage profile* in
    each load-table response, and those per-table properties win over anything
    the client configures. A warehouse set up for in-cluster access therefore
    advertises `http://minio:9000` even to a client outside the cluster, and
    `--s3-endpoint` cannot override it.

    Rather than edit /etc/hosts or reconfigure the warehouse, the name is mapped
    here, in this process. Nothing outside the script is touched.
    """
    real = socket.getaddrinfo

    def patched(host, port, *args, **kwargs):
        return real(target if host == alias else host, port, *args, **kwargs)

    socket.getaddrinfo = patched


NAMESPACE = "icemaint_verify"
SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "category", StringType(), required=False),
    NestedField(3, "payload", StringType(), required=False),
)
ARROW = pa.schema(
    [
        pa.field("id", pa.int32(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
        pa.field("payload", pa.string(), nullable=True),
    ]
)
SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
)


@dataclass
class Check:
    name: str
    ok: bool
    detail: str

    def render(self) -> str:
        return f"  {'PASS' if self.ok else 'FAIL'}  {self.name:<34} {self.detail}"


def batch(start: int, count: int, category: str) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(start, start + count), type=pa.int32()),
            "category": pa.array([category] * count, type=pa.string()),
            "payload": pa.array([f"p{i}" for i in range(start, start + count)], type=pa.string()),
        },
        schema=ARROW,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--uri", default="http://localhost:8181/catalog")
    parser.add_argument("--warehouse", help="Lakekeeper warehouse name (REST mode)")
    parser.add_argument("--s3-endpoint", default="http://localhost:9000")
    parser.add_argument("--s3-access-key-id", default="minio_admin")
    parser.add_argument("--s3-secret-access-key", default="minio_password")
    parser.add_argument("--s3-region", default="us-east-1")
    parser.add_argument(
        "--bypass-remote-signing",
        action="store_true",
        help="send s3.remote-signing-enabled=false and use the credentials above directly. "
        "Needed on a warehouse whose storage profile has remote signing with "
        "push-s3-delete-disabled, where the signer refuses HEAD and DELETE.",
    )
    parser.add_argument(
        "--direct-s3",
        metavar="s3://bucket/prefix",
        help="skip the REST catalog: use a local SQL catalog over this S3 prefix with the "
        "credentials below. Exercises real object storage -- including LIST and DELETE -- "
        "on a deployment whose REST catalog remote-signs and refuses them.",
    )
    parser.add_argument("--keep", action="store_true", help="do not drop the test namespace")
    parser.add_argument(
        "--map-host",
        metavar="ALIAS=TARGET",
        action="append",
        default=[],
        help="resolve ALIAS to TARGET in this process, e.g. minio=172.19.0.2. Needed when "
        "the warehouse advertises an in-cluster S3 endpoint.",
    )
    args = parser.parse_args()
    if not args.direct_s3 and not args.warehouse:
        parser.error("--warehouse is required unless --direct-s3 is given")

    for mapping in args.map_host:
        alias, _, target = mapping.partition("=")
        if not target:
            parser.error(f"--map-host expects ALIAS=TARGET, got {mapping!r}")
        map_host(alias, target)
        print(f"  resolving {alias} -> {target} for this process only")

    session = _direct_session(args) if args.direct_s3 else _rest_session(args)

    identifier = f"{NAMESPACE}.events"
    checks: list[Check] = []
    return _verify(session, identifier, checks, args)


def _rest_session(args) -> CatalogSession:
    return CatalogSession.for_lakekeeper(
        uri=args.uri,
        warehouse=args.warehouse,
        s3=S3Settings(
            endpoint=args.s3_endpoint,
            access_key_id=args.s3_access_key_id,
            secret_access_key=args.s3_secret_access_key,
            region=args.s3_region,
        ),
        extra={"s3.remote-signing-enabled": "false"} if args.bypass_remote_signing else None,
    )


def _direct_session(args) -> CatalogSession:
    """A SQL catalog over an S3 prefix, talking to MinIO with real credentials.

    The REST catalog is bypassed entirely. That loses coverage of the commit
    protocol -- which the REST run already covers -- and gains the operations
    the remote signer refuses: ListObjectsV2, HeadObject, DeleteObject.
    """
    import tempfile

    from pyiceberg.catalog.sql import SqlCatalog

    # Deliberately not a context manager: the catalog outlives this function and
    # the file is a throwaway in the system temp dir.
    db_path = tempfile.mkstemp(suffix=".db")[1]
    catalog = SqlCatalog(
        "direct",
        **{
            "uri": f"sqlite:///{db_path}",
            "warehouse": args.direct_s3,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": args.s3_endpoint,
            "s3.access-key-id": args.s3_access_key_id,
            "s3.secret-access-key": args.s3_secret_access_key,
            "s3.region": args.s3_region,
        },
    )
    from icemaint.session import _new_duckdb

    return CatalogSession(catalog=catalog, con=_new_duckdb(4), threads=4)


def _verify(session: CatalogSession, identifier: str, checks: list[Check], args) -> int:

    def check(name: str, fn):
        try:
            detail = fn()
            checks.append(Check(name, True, detail or ""))
            return True
        except Exception as exc:
            checks.append(Check(name, False, f"{type(exc).__name__}: {exc}"))
            traceback.print_exc(file=sys.stderr)
            return False

    try:
        _cleanup(session, identifier, quiet=True)
        session.catalog.create_namespace_if_not_exists(NAMESPACE)

        # -- setup ------------------------------------------------------
        def create():
            tbl = session.catalog.create_table(
                identifier, schema=SCHEMA, partition_spec=SPEC, properties={"format-version": "2"}
            )
            for i in range(4):
                tbl.append(pa.concat_tables([batch(i * 10, 10, "a"), batch(500 + i * 10, 10, "b")]))
            return f"4 commits, {session.table(identifier).scan().count()} rows"

        if not check("create + append via REST", create):
            return _report(checks)

        rows_before = session.table(identifier).scan().count()

        # -- the credential question ------------------------------------
        def listing():
            tbl = session.table(identifier)
            roots = storage_roots(tbl)
            found = list_storage(tbl, roots)
            if not found:
                raise RuntimeError(f"listing returned nothing under {roots}")
            return f"{len(found)} object(s) under {roots[0]}"

        can_list = check("s3 LIST (orphan removal only)", listing)

        def reachable():
            tbl = session.table(identifier)
            found = reachable_files(tbl)
            return f"{len(found)} referenced\n{found.describe()}"

        check("reachable set", reachable)

        check(
            "profile",
            lambda: f"{len(profile_table(session.table(identifier)).live_files)} live files",
        )

        # -- the write path ---------------------------------------------
        def compact():
            result = TableCompactor(session, identifier, CompactionConfig()).execute()
            return result.describe().splitlines()[0]

        check("compact (REST replace snapshot)", compact)

        check(
            "rows preserved by compaction",
            lambda: _expect(session.table(identifier).scan().count(), rows_before),
        )

        check(
            "dangling deletes",
            lambda: DanglingDeleteCleaner().run(session.table(identifier)).describe(),
        )

        check(
            "rewrite manifests",
            lambda: ManifestRewriter().run(session.table(identifier)).describe().splitlines()[0],
        )

        check(
            "apply metadata properties",
            lambda: (
                apply_metadata_properties(
                    session.table(identifier), MetadataSettings(previous_versions_max=2)
                )
                .describe()
                .splitlines()[0]
            ),
        )

        # -- reclaiming --------------------------------------------------
        def expire():
            policy = RetentionPolicy(max_snapshot_age_ms=0, min_snapshots_to_keep=1)
            return SnapshotExpirer(policy).run(session.table(identifier)).describe()

        check("expire snapshots + delete files", expire)

        if can_list:
            check(
                "remove orphans",
                lambda: OrphanCleaner(older_than_days=0).run(session.table(identifier)).describe(),
            )
        else:
            checks.append(
                Check("remove orphans", False, "skipped: the LIST above failed, so it cannot run")
            )

        check(
            "rows preserved by reclaiming",
            lambda: _expect(session.table(identifier).scan().count(), rows_before),
        )
        check(
            "still readable after everything",
            lambda: f"{len(find_dangling(session.table(identifier)).removable)} dangling left",
        )

    finally:
        if not args.keep:
            _cleanup(session, identifier, quiet=False)
        session.close()

    return _report(checks)


def _expect(actual, expected):
    if actual != expected:
        raise AssertionError(f"expected {expected}, got {actual}")
    return f"{actual} rows"


def _cleanup(session: CatalogSession, identifier: str, *, quiet: bool) -> None:
    for drop, arg in (
        (session.catalog.drop_table, identifier),
        (session.catalog.drop_namespace, NAMESPACE),
    ):
        try:
            drop(arg)
        except (NoSuchTableError, NoSuchNamespaceError):
            pass
        except Exception as exc:
            if not quiet:
                print(f"cleanup: could not drop {arg}: {exc}", file=sys.stderr)


def _report(checks: list[Check]) -> int:
    print("\n  Live Lakekeeper + MinIO verification")
    print("  " + "-" * 72)
    for c in checks:
        print(c.render())
    failed = [c for c in checks if not c.ok]
    print("  " + "-" * 72)
    print(f"  {len(checks) - len(failed)}/{len(checks)} passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
