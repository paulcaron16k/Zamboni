#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = "==3.13.*"
# dependencies = ["requests>=2.32", "python-dotenv>=1.0"]
# ///
"""Bootstrap the dev-stack Lakekeeper and create the Zamboni warehouse.

Idempotent: run it as often as you like. `docker compose up` alone leaves
Lakekeeper un-bootstrapped and with no warehouse, so this is the second half of
bringing the stack up.

    uv run dev-stack/bootstrap.py
    uv run dev-stack/bootstrap.py --show      # print config without changing anything

The warehouse is created with **STS credential vending**, deliberately. Lakekeeper
can instead remote-sign each request, and a remote-signing warehouse refuses
ListObjectsV2, HeadObject and multi-object DELETE -- which means compaction fails
and no storage can ever be reclaimed. That is not hypothetical; it is what the
first live verification run hit. See docs/live-verification.md.

MinIO needs no trust setup for STS. Lakekeeper's own docs: "Unlike for AWS, we do
not need any special trust-setup for vended credentials / STS with most S3
compatible solutions like Minio... Make sure to select `flavor` to have the value
`s3-compat`."
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests
from dotenv import dotenv_values

HERE = Path(__file__).resolve().parent
TIMEOUT = 30


def load_env() -> dict[str, str]:
    env_file = HERE / ".env"
    if not env_file.exists():
        sys.exit(
            f"missing {env_file}\n"
            f"  cp {HERE.name}/.env.sample {HERE.name}/.env   # then re-run"
        )
    values = {k: v for k, v in dotenv_values(env_file).items() if v is not None}
    missing = [
        k
        for k in (
            "LAKEKEEPER_PORT",
            "MINIO_PORT",
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "S3_BUCKET",
            "S3_REGION",
            "S3_GATEWAY",
            "WAREHOUSE_NAME",
        )
        if not values.get(k)
    ]
    if missing:
        sys.exit(f"{env_file} is missing: {', '.join(missing)}")
    return values


def management(env: dict[str, str], path: str) -> str:
    return f"http://localhost:{env['LAKEKEEPER_PORT']}/management/v1{path}"


def server_info(env: dict[str, str]) -> dict:
    try:
        r = requests.get(management(env, "/info"), timeout=TIMEOUT)
    except requests.exceptions.ConnectionError:
        sys.exit(
            f"cannot reach Lakekeeper on port {env['LAKEKEEPER_PORT']}.\n"
            "  cd dev-stack && docker compose up -d"
        )
    r.raise_for_status()
    return r.json()


def bootstrap(env: dict[str, str]) -> None:
    info = server_info(env)
    if info.get("bootstrapped"):
        print("  lakekeeper   already bootstrapped")
        return
    r = requests.post(
        management(env, "/bootstrap"),
        json={"accept-terms-of-use": True},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    print("  lakekeeper   bootstrapped")


def storage_config(env: dict[str, str]) -> dict:
    return {
        "storage-profile": {
            "type": "s3",
            "bucket": env["S3_BUCKET"],
            "region": env["S3_REGION"],
            "key-prefix": env["WAREHOUSE_NAME"],
            # The compose network's gateway, not `http://minio:9000`. An
            # in-cluster hostname is advertised to every client, including ones
            # outside the cluster, and per-table storage properties win over
            # anything a client configures -- so `minio` would be unresolvable
            # on your laptop and no flag could fix it. The gateway address
            # routes to the host from inside the network and to itself from the
            # host, so one endpoint serves both.
            "endpoint": f"http://{env['S3_GATEWAY']}:{env['MINIO_PORT']}",
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": env.get("S3_STS_ENABLED", "true").lower() == "true",
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": env["MINIO_ROOT_USER"],
            "aws-secret-access-key": env["MINIO_ROOT_PASSWORD"],
        },
    }


def existing_warehouse(env: dict[str, str]) -> dict | None:
    r = requests.get(management(env, "/warehouse"), timeout=TIMEOUT)
    r.raise_for_status()
    for warehouse in r.json().get("warehouses", []):
        if warehouse.get("name") == env["WAREHOUSE_NAME"]:
            return warehouse
    return None


def create_warehouse(env: dict[str, str]) -> dict:
    if found := existing_warehouse(env):
        profile = found.get("storage-profile", {})
        print(f"  warehouse    {env['WAREHOUSE_NAME']} already exists ({found['warehouse-id']})")
        if not profile.get("sts-enabled"):
            print(
                "    WARNING: this warehouse does not have sts-enabled. Storage cannot be\n"
                "    reclaimed through it -- the signer refuses LIST, HEAD and DELETE.\n"
                "    Delete it and re-run, or set a different WAREHOUSE_NAME in .env."
            )
        return found

    r = requests.post(
        management(env, "/warehouse"),
        json={"warehouse-name": env["WAREHOUSE_NAME"], **storage_config(env)},
        timeout=TIMEOUT,
    )
    if not r.ok:
        sys.exit(f"warehouse creation failed ({r.status_code}): {r.text}")
    print(f"  warehouse    {env['WAREHOUSE_NAME']} created ({r.json()['warehouse-id']})")
    return existing_warehouse(env) or {}


def describe(env: dict[str, str]) -> None:
    lk, s3 = env["LAKEKEEPER_PORT"], env["MINIO_PORT"]
    print("\n  Endpoints")
    print("  " + "-" * 68)
    print(f"    catalog      http://localhost:{lk}/catalog")
    print(f"    UI           http://localhost:{lk}/ui/     (served by Lakekeeper itself)")
    print(f"    MinIO S3     http://localhost:{s3}")
    print(f"    MinIO console http://localhost:{env['MINIO_CONSOLE_PORT']}")
    print(f"    Postgres     localhost:{env['POSTGRES_PORT']}")
    print("\n  Use it")
    print("  " + "-" * 68)
    print(f"    export ZAMBONI_URI=http://localhost:{lk}/catalog")
    print(f"    export ZAMBONI_WAREHOUSE={env['WAREHOUSE_NAME']}")
    print("    zamboni describe <namespace>.<table>")
    print(f"\n    ./bin/demo --catalog lakekeeper next-day\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--show", action="store_true", help="print config, change nothing")
    args = parser.parse_args()
    env = load_env()

    if args.show:
        print(json.dumps(storage_config(env), indent=2))
        describe(env)
        return 0

    info = server_info(env)
    print(f"\n  Lakekeeper {info.get('version')}  queues={info.get('queues')}")
    print("  " + "-" * 68)
    bootstrap(env)
    warehouse = create_warehouse(env)
    profile = warehouse.get("storage-profile", {})
    print(
        f"  storage      s3://{profile.get('bucket')}/{profile.get('key-prefix')}"
        f"  sts={profile.get('sts-enabled')}  signing={profile.get('remote-signing-enabled')}"
    )
    describe(env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
