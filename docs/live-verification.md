# Live verification: Lakekeeper + MinIO

Everything else is tested against a SQL catalog over a local directory. That
covers the logic and misses the two things only a real deployment answers:
whether the REST catalog accepts the snapshots we build, and whether the storage
credentials permit a bucket **listing** and a **delete** — which the reclaim
operations need and no other operation does.

Reproduce with [`scripts/verify-live.py`](../scripts/verify-live.py). It creates
its own namespace, works only inside it, and drops it at the end.

```bash
# Against the REST catalog
uv run scripts/verify-live.py --warehouse acme_db --map-host minio=172.19.0.2

# Against the object store directly (SQL catalog, real credentials)
uv run scripts/verify-live.py --direct-s3 s3://warehouse/zamboni-verify \
    --s3-endpoint http://172.19.0.2:9000
```

Verified against Lakekeeper **0.13.1**, MinIO, PyIceberg 0.11.1.

---

## Result

| | REST (remote signing) | Direct S3 |
|---|---|---|
| create + append | pass | pass |
| profile / reachable set | pass | pass |
| compact | **fail** — 403 on HeadObject | pass |
| remove dangling deletes | pass | pass |
| rewrite manifests | pass | pass |
| apply metadata properties | pass | pass |
| expire snapshots | commits, **deletes nothing** | pass — 26 files deleted |
| remove orphans | **cannot run** — LIST refused | pass — 7 files deleted |
| rows preserved throughout | pass | pass |

**13/13 against direct S3. 10/13 through the REST catalog**, with all three
failures traced to one cause below.

---

## What the REST catalog blocks, and why

The warehouse under test has this storage profile:

```
sts-enabled:              false
remote-signing-enabled:   true
push-s3-delete-disabled:  true
```

That is **remote signing**, not credential vending. The client never holds S3
credentials; it asks Lakekeeper to sign each request. Lakekeeper signs object
`GET` and `PUT`, and refuses the rest:

- `ListObjectsV2` → `SignError: Failed to sign request 400`. Orphan removal
  cannot run at all.
- Multi-object `POST /bucket?delete` (what s3fs issues for a delete) → the same.
  Expiry commits the snapshot removal and then reports every file deletion as
  failed, which is what `failed_deletes` is for.
- `HeadObject` → `403 Forbidden` from MinIO, which is why compaction fails: its
  reader stats the file before opening it.

None of this is a defect in `zamboni`, and none of it is a defect in
Lakekeeper — `push-s3-delete-disabled: true` says plainly that clients are not
meant to delete objects; that is the `tabular_purge` queue's job. It does mean:

> **Storage reclamation needs a warehouse that vends credentials
> (`sts-enabled: true`) or direct S3 credentials.** On a remote-signing
> warehouse, `zamboni` can compact metadata and expire snapshots but cannot
> free a byte.

`--bypass-remote-signing` sends `s3.remote-signing-enabled=false`, but
Lakekeeper returns the storage settings in each *load-table* response and those
per-table properties win over anything the client configures. The flag is kept
because it is the right thing to try; it does not work here.

Two smaller deployment notes:

- The warehouse advertises the **in-cluster** endpoint (`http://minio:9000`) to
  every client, including ones outside the cluster, and `--s3-endpoint` cannot
  override it for the same reason. `--map-host minio=172.19.0.2` resolves the
  name inside the verification process only.
- Lakekeeper 0.13.1 OSS ships the queues `soft_deletion`, `tabular_purge` and
  `task_log_cleanup`. There is still no compaction, expiry or orphan queue.

---

## Four bugs this found

All four were invisible to the whole local suite, and all four sat in the same
place: the boundary between how metadata records a location and how a listing
returns one. Two were found by running against the stack; two more by reviewing
the fixes afterwards, once it was clear that was where the risk lived.

### 1. `canonical()` is not idempotent for object keys

`reachable.canonical` maps a metadata location onto the form a listing returns:
`s3://bucket/key` → `bucket/key`. `list_storage` then applied it *again* to the
listing output. A scheme-less path is resolved as a local one, so
`bucket/key` became `/current/working/dir/bucket/key`, and **every live file
looked unreferenced**.

Locally the function is idempotent — listings there are already absolute paths —
so nothing caught it.

It did not delete anything: the completeness invariant ("every referenced file
must exist in the listing") saw all seven referenced files missing and aborted.
That invariant was written on the argument that a partial listing makes the
complement meaningless. This is exactly that case, and it is the reason orphan
removal is safe to enable by default.

### 2. Deletion needs the location, not the key

Having fixed the keys, orphan removal found the right seven files and deleted
none of them: `FileIO.delete("bucket/key")` treats the key as a local path.
`StorageFile` now carries both forms — `path` to compare with, `location` to
delete with — the same distinction `expire.files_of_snapshots` already made.

The first run after the fix reported `deleted 0 file(s)` beside
`7 file(s) could not be deleted`, which is what made it obvious. Counting failed
deletes separately rather than assuming success is what turned a silent no-op
into a visible one.

### 3. Orphan removal only understood one FileIO

`list_storage` reached for `PyArrowFileIO._initialize_fs`. A Lakekeeper warehouse
with remote signing sets `py-io-impl` to `FsspecFileIO` in its per-table config,
and those properties win over the client's — so the function raised
`AttributeError` against precisely the deployment it was written for. It now
takes either, and aborts explicitly on a FileIO exposing neither.

`properties.unreferenced_metadata_files` had grown its own copy of the same
listing code and so reintroduced the same assumption. It now calls
`list_storage`, which also gets it `write.metadata.path` for free.

### 4. The fsspec listing asked for the bucket twice

Found by review, not by the run — and it would have survived the run either way.
`parse_location` returns `("s3", "warehouse", "warehouse/acme/tbl")`: the path
*already* carries the bucket. The new fsspec branch prefixed the netloc again
and asked for `warehouse/warehouse/acme/tbl`, which lists nothing.

That is the dangerous failure mode for this operation, because the unreferenced
set is the *complement* of the listing: an empty listing does not look like an
error, it looks like a table with no orphans. The live run hid it behind the
signer refusing the request outright — the prefix in that error message
(`prefix=warehouse%2Facme_db%2F…`) is the evidence, and it took reading the URL
rather than the exception to see it.

Verified after the fix, against MinIO with `py-io-impl` forced to `FsspecFileIO`:
21 objects listed, 3 stranded `metadata.json` identified and deleted, rows
intact.
