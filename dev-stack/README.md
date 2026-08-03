# Zamboni dev stack

Lakekeeper, Postgres and MinIO, configured so that Zamboni's **storage
reclamation actually works** — which is not the default and is the whole reason
this stack exists.

```bash
cp dev-stack/.env.sample dev-stack/.env      # first time only
cd dev-stack && docker compose up -d
uv run bootstrap.py                          # bootstrap + create the warehouse

export ZAMBONI_URI=http://localhost:8182/catalog
export ZAMBONI_WAREHOUSE=zamboni
```

Then, from the project root:

```bash
./bin/demo --catalog lakekeeper clear
./bin/demo --catalog lakekeeper next-day
./bin/demo --catalog lakekeeper maintenance --reclaim-now
uv run pytest tests/test_dev_stack.py        # 12 tests; skipped if the stack is down
```

| | |
|---|---|
| Catalog | http://localhost:8182/catalog |
| UI | http://localhost:8182/ui/ |
| MinIO S3 | http://localhost:9010 |
| MinIO console | http://localhost:9011 |
| Postgres | localhost:5433 |

Ports are shifted off the defaults so this coexists with any other Lakekeeper or
MinIO on the machine. `docker compose down -v` removes the volumes too.

There is **no separate UI container** — Lakekeeper serves its own UI at `/ui/`.

---

## The two settings that matter

Everything else here is ordinary compose. These two are the ones that took a
failed verification run to find.

### `sts-enabled: true`

Lakekeeper can give a client access to storage two ways, and only one of them
lets Zamboni reclaim anything:

| | STS credential vending | Remote signing |
|---|---|---|
| Client receives | temporary credentials + session token | nothing; each request is signed by Lakekeeper |
| FileIO selected | `PyArrowFileIO` | `FsspecFileIO` + `S3V4RestSigner` |
| `GET` / `PUT` | works | works |
| `ListObjectsV2` | works | **refused** (400 from the signer) |
| `HeadObject` | works | **refused** (403) |
| multi-object `DELETE` | works | **refused** |
| Consequence | everything works | compaction fails; `expire` commits but frees nothing; `remove-orphans` cannot run |

A remote-signing warehouse looks perfectly healthy. Reads work, writes work,
tables are queryable. It fails only when you try to free a byte — which is why
`tests/test_dev_stack.py` asserts the setting directly rather than inferring it
from a successful read.

Note that `remote-signing-enabled` may *also* be true on this warehouse. **STS
takes precedence**; it is the presence of STS that matters, not the absence of
signing. `bootstrap.py` warns if it finds an existing warehouse without it.

MinIO needs no trust setup for this — per Lakekeeper's docs, an access key that
can read and write the bucket is enough, with `flavor: s3-compat`.

### The endpoint is the compose gateway, not `minio`

Lakekeeper returns the storage endpoint in every *load-table* response, and those
per-table properties beat anything a client configures. So a warehouse created
with `endpoint: http://minio:9000` advertises an in-cluster hostname to clients
outside the cluster, and **no client-side flag can override it** — `--s3-endpoint`
is ignored.

This stack advertises `http://172.31.0.1:9010`, the pinned compose network
gateway. That address routes to the host from inside the network (so Lakekeeper
can validate the warehouse) and to itself from the host (so your laptop can read
the tables). One endpoint, both sides, no `/etc/hosts` entry and no name mapping.

That is why `docker-compose.yaml` pins the subnet instead of letting Docker pick:
the gateway address has to be knowable in advance to go in `.env`. If
`172.31.0.0/24` collides on your machine, change it in both places.

---

## Files

| | |
|---|---|
| `docker-compose.yaml` | The stack. Project name `zamboni-dev`, own network, pinned subnet |
| `.env.sample` | Committed template. Every value is dev-only |
| `.env` | Yours; **gitignored**. `cp .env.sample .env` |
| `bootstrap.py` | Bootstraps Lakekeeper and creates the warehouse. Idempotent; `--show` prints config without changing anything |

`tests/test_dev_stack.py::test_env_sample_and_env_declare_the_same_keys` fails if
the two `.env` files drift apart, so the template stays usable.

---

## Troubleshooting

**`cannot reach Lakekeeper on port 8182`** — the stack is not up, or the port is
taken. `docker compose ps` should show `db`, `lakekeeper` and `minio` healthy;
`migrate` and `createbucket` exit 0 and stay exited, which is correct.

**`warehouse creation failed (400)`** — usually the endpoint. Lakekeeper
validates a new warehouse by writing to it, so if it cannot reach
`S3_GATEWAY:MINIO_PORT` from inside the network, creation fails. Check the
gateway matches the pinned subnet: `docker network inspect
zamboni-dev_zamboni --format '{{(index .IPAM.Config 0).Gateway}}'`.

**Reclamation reports files it cannot delete** — you are on a warehouse without
STS. `uv run bootstrap.py` warns about this. Delete the warehouse in the UI and
re-run, or pick a new `WAREHOUSE_NAME`.

**Tests all skip** — expected when the stack is down. They skip on a missing
`.env`, an unreachable Lakekeeper, an un-bootstrapped one, or a missing
warehouse, and each skip says which.
