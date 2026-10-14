# SPDX-License-Identifier: Apache-2.0
"""Measure how many of Lakekeeper's own events reach NATS, in a burst or an outage.

The numbers in docs/event-driven-maintenance.md §5 about Lakekeeper's event
delivery come from this script, run against the dev stack (whose Lakekeeper
publishes on `lakekeeper.events`, dev-stack/docker-compose.yaml). From the
project root, with the stack up and bootstrapped:

    uv run scripts/lakekeeper-event-loss.py --commits 4000 --threads 16
    uv run scripts/lakekeeper-event-loss.py --commits 4000 --threads 16 --outage

Each commit is a property update -- one `updateTable` event, no data files -- on
one of `--threads` tables, committed concurrently. Two counts are taken, because
they answer different questions:

- **stored**: what the NATS server received, counted by a JetStream stream
  bound to the subject for the run. This is Lakekeeper's own loss.
- **received**: what one core-NATS subscriber saw. With `--outage` that
  subscriber is disconnected too, so it can also miss what the server did get --
  core NATS delivers only to whoever is connected at the time.

`--outage` stops the stack's nats service before the commits and starts it after
them, so Lakekeeper buffers and then flushes or drops. A drop is logged by
Lakekeeper as "Failed to emit event"; the script counts those lines too.

Leaves nothing behind: the namespace, its tables and the stream are removed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import nats
import pyarrow as pa

from zamboni import CatalogSession

DEV_STACK = Path(__file__).resolve().parent.parent / "dev-stack"
SUBJECT = "lakekeeper.events"
STREAM = "LAKEKEEPER_EVENT_LOSS"


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", *args], cwd=DEV_STACK, check=True, capture_output=True, text=True
    ).stdout


def ours(payload: bytes, namespace: str) -> bool:
    doc = json.loads(payload)
    return bool(doc.get("namespace") == namespace and doc.get("type") == "updateTable")


async def measure(args: argparse.Namespace) -> dict[str, Any]:
    session = CatalogSession.for_lakekeeper(uri=args.uri, warehouse=args.warehouse)
    namespace = f"zloss_{uuid.uuid4().hex[:8]}"
    session.catalog.create_namespace(namespace)
    names = [f"{namespace}.t{i}" for i in range(args.threads)]
    for name in names:
        session.catalog.create_table(name, schema=pa.schema([pa.field("id", pa.int32())]))

    def commit(name: str, count: int) -> None:
        table = session.catalog.load_table(name)
        for k in range(count):
            update = table.transaction().set_properties({"zamboni.loss": str(k)})
            table = update.commit_transaction()

    client = await nats.connect(args.nats, max_reconnect_attempts=-1)
    js = client.jetstream()
    received: set[str] = set()

    async def heard(msg: Any) -> None:
        if ours(msg.data, namespace):
            received.add(json.loads(msg.data)["id"])

    try:
        await js.add_stream(name=STREAM, subjects=[SUBJECT])
        await client.subscribe(SUBJECT, cb=heard)
        await client.flush()
        since = time.time()
        if args.outage:
            compose("stop", "nats")
        per_table = args.commits // args.threads
        started = time.monotonic()
        await asyncio.gather(*(asyncio.to_thread(commit, n, per_table) for n in names))
        took = time.monotonic() - started
        if args.outage:
            compose("start", "nats")
        expected = per_table * args.threads
        for _ in range(args.settle):
            await asyncio.sleep(1)
            if len(received) >= expected:
                break
        stored = 0
        # Acked as read: a pull consumer stops delivering at max_ack_pending
        # (1000 by default) unacknowledged messages, which undercounts.
        reader = await js.pull_subscribe(SUBJECT, stream=STREAM)
        while True:
            try:
                batch = await reader.fetch(500, timeout=2)
            except nats.errors.TimeoutError:
                break
            for msg in batch:
                stored += ours(msg.data, namespace)
                await msg.ack()
        dropped = compose("logs", "lakekeeper", "--since", f"{int(time.time() - since) + 5}s")
        return {
            "commits": expected,
            "threads": args.threads,
            "outage": args.outage,
            "seconds": round(took, 1),
            "commits_per_second": round(expected / took),
            "stored": stored,
            "received": len(received),
            "logged_drops": dropped.count("Failed to emit event"),
        }
    finally:
        await js.delete_stream(STREAM)
        await client.close()
        for name in names:
            session.catalog.drop_table(name)
        session.catalog.drop_namespace(namespace)
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--commits", type=int, default=400)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--outage", action="store_true", help="stop nats during the commits")
    parser.add_argument("--settle", type=int, default=60, help="seconds to wait for delivery")
    parser.add_argument("--uri", default="http://localhost:8182/catalog")
    parser.add_argument("--warehouse", default="zamboni")
    parser.add_argument("--nats", default="nats://127.0.0.1:4232")
    print(json.dumps(asyncio.run(measure(parser.parse_args()))))


if __name__ == "__main__":
    main()
