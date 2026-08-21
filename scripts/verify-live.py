#!/usr/bin/env -S uv run --quiet --script
# SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = "==3.13.*"
# dependencies = []
# ///
"""Run the dev-stack test suite against a deployment this repo did not create.

`tests/test_dev_stack.py` reads `dev-stack/.env`, so on its own it can only check
the stack in this repository. That answers "does our stack work". It cannot
answer "does *that* warehouse work", which is the question you have when
onboarding a real deployment -- and the more interesting one, because a warehouse
can be perfectly healthy for reads and unable to reclaim a single byte.

    uv run scripts/verify-live.py --port 8181 --warehouse acme_db \\
        --s3-host 172.19.0.2 --s3-port 9000

This was 348 lines that called every operation itself. It is now a launcher,
because that version duplicated the tests: two implementations of the same
checks, free to drift apart. Not hypothetical here --
`unreferenced_metadata_files` once reintroduced a bug that had been fixed an hour
earlier in the other copy of its logic.

What you get is pytest's own report. A failure *is* the diagnosis: a warehouse
without STS fails `test_the_warehouse_vends_credentials_rather_than_signing` by
name, which is more use than a bespoke PASS/FAIL table saying the same thing.

Exit code is pytest's. Anything after `--` is passed through, so `-- -x` stops at
the first failure and `-- -k listing` runs one check.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
SUITE = "tests/test_dev_stack.py"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="Anything after -- goes to pytest, e.g. -- -x -k deletion",
    )
    parser.add_argument("--port", required=True, help="Lakekeeper port, e.g. 8181")
    parser.add_argument("--warehouse", required=True, help="warehouse name, e.g. acme_db")
    parser.add_argument(
        "--s3-host",
        help="host the warehouse's S3 endpoint resolves to. Needed when it advertises an "
        "in-cluster name: per-table storage properties beat client config, so such an "
        "endpoint cannot be overridden -- only reached.",
    )
    parser.add_argument("--s3-port", help="port of that endpoint")
    parser.add_argument("--s3-console-port", help="MinIO console port, if it has one")
    parser.add_argument(
        "--allow-skips",
        action="store_true",
        help="treat an unreachable target as a skip rather than a failure. Off by default: "
        "a suite of skips looks exactly like a suite of passes.",
    )
    parser.add_argument("pytest_args", nargs="*", help=argparse.SUPPRESS)
    args = parser.parse_args()

    env = dict(os.environ)
    env["ZAMBONI_VERIFY_PORT"] = args.port
    env["ZAMBONI_VERIFY_WAREHOUSE"] = args.warehouse
    for value, var in (
        (args.s3_host, "ZAMBONI_VERIFY_S3_HOST"),
        (args.s3_port, "ZAMBONI_VERIFY_S3_PORT"),
        (args.s3_console_port, "ZAMBONI_VERIFY_S3_CONSOLE_PORT"),
    ):
        if value:
            env[var] = value
    if not args.allow_skips:
        env["ZAMBONI_REQUIRE_DEV_STACK"] = "1"

    print(f"  verifying warehouse {args.warehouse!r} on port {args.port}")
    print(f"  suite: {SUITE}\n")
    return subprocess.call(
        ["uv", "run", "pytest", SUITE, "-v", *args.pytest_args], cwd=PROJECT, env=env
    )


if __name__ == "__main__":
    raise SystemExit(main())
