# Reclaiming storage: what each catalog posture allows

Only two of Zamboni's six operations **delete files** — `expire` and
`remove-orphans` — and those are the ones a catalog's storage policy can quietly
take away. Three of the other four move metadata and nothing else; `compact`
writes new data files and deletes none, beyond cleaning up its own output when a
rewrite is abandoned before committing. This page says what you get under each posture, what to
configure to get all of it, and what to ask a bucket administrator for when the
catalog is not going to give it to you.

One correction first, because the short version of this story is usually told
wrong: **the catalog is never optional.** Orphan removal subtracts the set of
files a table references from a listing of what is in storage, and the reference
set comes from table metadata — whose *current* pointer is the one fact only the
catalog owns. Guessing the newest `metadata.json` from a directory listing is
exactly the unsafe shortcut this tool refuses everywhere else. The accurate claim
is narrower and still worth having:

> The catalog is needed for the metadata pointer, which even a restrictive
> catalog serves. The object store is reached with our own credentials — and that
> is the part a restrictive catalog was blocking.

PyIceberg is still how metadata is read. Nothing here replaces it.

---

## 1. On a restrictive catalog: what can be done, and what is done

A **remote-signing** warehouse hands the client no credentials and signs each
request instead, and Lakekeeper signs object `GET` and `PUT` while refusing the
rest. Measured against Lakekeeper 0.13.1 and MinIO
([live-verification.md](live-verification.md)), with a warehouse whose profile is
`sts-enabled: false`, `remote-signing-enabled: true`, `push-s3-delete-disabled: true`:

| Operation | Through a signing catalog | Why |
|---|---|---|
| `compact` | **fails** | its reader stats the file before opening it; `HeadObject` → `403 Forbidden` |
| `expire` | **commits, frees nothing** | the snapshot removal succeeds; every file deletion then fails. `failed_deletes` is what reports it |
| `remove-orphans` | **cannot run** | `ListObjectsV2` → `SignError: Failed to sign request 400` |
| `remove-dangling-deletes` | works | metadata only |
| `rewrite-manifests` | works | metadata only |
| `apply-properties` | works | metadata only |

**Three of the six, then** — and the three that fail are the three that touch
data files. That is a narrower statement than "maintenance works", and it is the
honest one: a maintenance run against such a warehouse reports success on half
its work while the table keeps growing.

**It will look healthy the whole time.** Reads work, writes work, tables are
queryable, `zamboni describe` is correct. Nothing is visibly wrong until you
check whether a byte was ever freed — which is why `tests/test_dev_stack.py`
asserts the warehouse setting directly rather than inferring health from a
successful read.

Worth saying plainly: none of this is a defect, in Zamboni or in Lakekeeper. A
profile carrying `push-s3-delete-disabled: true` is stating that clients are not
meant to delete objects — in Lakekeeper's design that is the `tabular_purge`
queue's job. Remote signing is the better control for the job it exists for:
hand a BI tool a catalog token and it never sees a storage credential, and access
is revoked by declining to sign rather than by rotating a key. It is the wrong
control for maintenance because the verbs it declines are the ones reclaiming
storage is made of.

---

## 2. Configure it this way, and Zamboni does the rest

### Lakekeeper

Set this on the **warehouse's storage profile**, at creation:

```jsonc
{
  "storage-profile": {
    "type": "s3",
    "sts-enabled": true,          // required; no default
    "flavor": "s3-compat",        // or "aws" (the default)
    // "remote-signing-enabled" may be left alone: it defaults to true,
    // and Lakekeeper tries vended credentials first, falling back to signing.
    // It is the presence of STS that decides, not the absence of signing.
    "sts-role-arn": "arn:aws:iam::...:role/...",  // required when flavor is "aws"
    "bucket": "...",
    "region": "...",
    "key-prefix": "..."
  }
}
```

A `storage-credential` block accompanies the profile in the same request — for
`s3-compat`, `{"type": "s3", "credential-type": "access-key", ...}`. Those are the
credentials Lakekeeper exchanges for the short-lived ones it vends;
`dev-stack/bootstrap.py` posts both together and is a working example.

- `sts-enabled` is **required and has no default**. A profile that does not
  mention it is rejected; a profile that sets it `false` is posture 1 above.
- `remote-signing-enabled` is optional and **defaults to `true`**, so a warehouse
  created without mentioning signing has signing on. That is fine — see the
  comment above.
- `sts-role-arn` (or `assume-role-arn`) is required when `flavor` is `aws`. For
  `flavor: s3-compat` — MinIO and friends — no trust setup is needed at all; an
  access key that can read and write the bucket is enough.
- `push-s3-delete-disabled` (default `true`) controls whether `s3.delete-enabled=false`
  is pushed to clients, which is a separate way for deletion to be switched off.

With `sts-enabled: true`, **all six operations work through the catalog**:
13/13 in live verification, `remove-orphans` included, with no host-name mapping.
See [dev-stack/README.md](../dev-stack/README.md) for all four combinations of
the two flags and what each means.

### Polaris

**There is no signing setting to get wrong: Polaris has no remote-signing mode.**
It vends credentials only — it calls AWS STS `AssumeRole` with, in its own words,
"an inline session policy scoped to the specific table locations and operations
(read, list, write) the caller is authorized to perform", and returns
`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token` and
`s3.session-token-expires-at-ms`. That is the same shape Lakekeeper's STS path
produces, which is why PyIceberg selects `PyArrowFileIO` for both.

Create the catalog with S3 storage:

```bash
polaris catalogs create \
  --storage-type s3 \
  --default-base-location s3://bucket/prefix \
  --allowed-location s3://bucket/prefix \
  --role-arn arn:aws:iam::...:role/... \
  --external-id ... \
  my_catalog
```

What decides whether reclaim works here is **the principal's privileges**, not a
storage flag, because the session policy is scoped to them. The grantable
privileges are `TABLE_CREATE` (catalog level), `TABLE_LIST` (namespace level),
and `TABLE_DROP`, `TABLE_READ_DATA`, `TABLE_WRITE_DATA` (table level). A
principal holding only `TABLE_READ_DATA` receives a credential scoped to reading,
so a reclaim pass will be refused by the object store rather than by the catalog.

**Not verified here:** Polaris documents the three operation categories (read,
list, write) but publishes no mapping from those privileges to individual S3
actions such as `s3:DeleteObject`. If you need certainty that deletion is
included, test it — `zamboni remove-orphans <table>` previews without `--yes` and
a run with it reports `failed_deletes` rather than claiming success.

### Is that feature-complete against Spark?

**No.** With storage access configured, Zamboni matches Spark on how it finds
orphans — both list the store as a principal holding real credentials, both
collapse scheme aliases before comparing (Spark's `equal_schemes`, default
`map('s3a,s3n','s3')`; Zamboni's `reachable.canonical()`), and both abort on path
ambiguity rather than guess (Spark's `prefix_mismatch_mode=ERROR`; Zamboni's
completeness invariant).

What Spark has and Zamboni does not is a way to **skip the listing entirely**:
`file_list_view` takes the candidate list from a dataset — a cloud storage
inventory report, typically — and `prefix_listing` changes how the listing is
done. That is not an access capability, it is a scale and cost one: a full
recursive listing of a large warehouse is slow and billable however good your
credentials are. Tracked as ZMBNI-94, unbuilt.

In the other direction Zamboni previews all six operations; Spark's `dry_run`
exists on `remove_orphan_files` alone and Trino has none. "Feature-complete" is
therefore false in both directions, and the gap that matters for a large
warehouse is the listing one.

---

## 3. Orphan removal by an object-store admin role

The remaining posture is: the catalog will not vend or sign storage access, and
is not going to be reconfigured. Give Zamboni the object store's own credentials
and it reaches storage directly — the warehouse system owns that store, and the
catalog is a service in front of it, not its owner.

Which settings, for the store the warehouse is actually in — the table's own
location decides, and credentials for the wrong one are **refused up front**
rather than failing at the first read:

```bash
# .env, mode 600. These may also go under `storage:` in zamboni.yml --
# which then holds credentials, and is held to the same mode rule.

# S3, and every S3-compatible store: MinIO, Silo, Garage, Ceph RGW
ZAMBONI_S3_ACCESS_KEY_ID=...
ZAMBONI_S3_SECRET_ACCESS_KEY=...

# Google Cloud Storage. A path to a service-account key file, or the literal
# `google_default` to use ambient credentials -- which is what GKE Workload
# Identity provides, and needs no secret in the environment at all.
ZAMBONI_GCS_TOKEN=/var/run/secrets/gcp/key.json
ZAMBONI_GCS_PROJECT_ID=...            # optional

# Azure Blob / ADLS Gen2. An account name plus exactly one credential.
ZAMBONI_AZURE_ACCOUNT_NAME=...
ZAMBONI_AZURE_ACCOUNT_KEY=...         # or ZAMBONI_AZURE_SAS_TOKEN, or
                                      # ZAMBONI_AZURE_CLIENT_ID + _CLIENT_SECRET + _TENANT_ID
```

Configure one provider, not two: a session holds one set of credentials, and a
fleet spanning two stores needs two runs.

`ZAMBONI_CREDENTIAL_USE` decides when they are preferred over the catalog's:
`always` (the default), `reclaim-only` for `expire` and `remove-orphans` alone —
which leaves reads and compaction in the catalog's audit trail — or `never`. With
none configured, a reclaim run **refuses up front** rather than listing what it
can and deleting what it managed to sign. See
[Storage credentials](user_guide.md#storage-credentials-who-talks-to-the-object-store).

### The permissions to ask for

#### S3, and S3-compatible stores

Derived from the calls the reclaim path actually makes, not from a template:
the listing is one recursive `ListObjectsV2` per storage root
(`orphans.list_storage`), the reachable set is read out of table metadata,
manifest lists and manifests (`reachable.reachable_files`), and deletion is
per-object (`tbl.io.delete(...)`, one path at a time).

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ZamboniListTablePrefix",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::WAREHOUSE-BUCKET",
      "Condition": {"StringLike": {"s3:prefix": ["WAREHOUSE-PREFIX/*"]}}
    },
    {
      "Sid": "ZamboniReadAndReclaim",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::WAREHOUSE-BUCKET/WAREHOUSE-PREFIX/*"
    }
  ]
}
```

- `s3:ListBucket` is the bucket-level action behind `ListObjectsV2`; the prefix
  condition keeps the grant inside the warehouse. It is scoped to the bucket ARN,
  not the object ARN — a common way to get this wrong and see an empty listing
  rather than an error.
- `s3:GetObject` covers `HeadObject` too, which is what an object store checks
  for a stat. Metadata, manifest lists and manifests are all read through it.
- `s3:DeleteObject` is the only destructive grant, and it is the whole point.
  Multi-object delete (`POST /bucket?delete`) is governed by the same action;
  Zamboni deletes one object at a time.
- **`s3:PutObject` is deliberately absent.** Reclaim never writes. Add it only if
  the same credential is also to run `compact`, which writes the rewritten files
  — and which uses `s3:DeleteObject` too, to clean up its own output when a
  rewrite is abandoned before committing.

#### Google Cloud Storage

The same three capabilities, as one predefined role on the bucket:

```bash
gcloud storage buckets add-iam-policy-binding gs://WAREHOUSE-BUCKET \
  --member=serviceAccount:zamboni@PROJECT.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin
```

`roles/storage.objectAdmin` covers list, read and delete on objects, and
deliberately not `storage.buckets.*` — nothing here creates or deletes a bucket.
`roles/storage.objectViewer` is the read-only half and is **not** enough: it
cannot delete, so a reclaim run would list correctly and then fail on every
removal.

On GKE, prefer **Workload Identity** over a key file: bind that service account
to the pod's Kubernetes service account and set `ZAMBONI_GCS_TOKEN=google_default`.
No secret reaches the environment, and the credential refreshes itself. A
downloaded key file works too — give `ZAMBONI_GCS_TOKEN` its path — and is the
reason GCS is routed through `gcsfs` rather than pyarrow, which accepts only a
bearer token with an expiry.

#### Azure Blob / ADLS Gen2

**Storage Blob Data Contributor** on the container, which carries read, write,
delete and list. Its read-only counterpart, *Storage Blob Data Reader*, has the
same shortfall as `objectViewer` above.

```bash
az role assignment create \
  --assignee <principal-id> \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>/blobServices/default/containers/<container>"
```

An account key or a SAS token grants the same access without a role assignment;
prefer the service principal where the deployment allows it, because it is the
one that can be scoped and revoked per identity.

#### All three

Whoever administers the bucket is entitled to ask what this is for, and the
honest answer is that it is a deliberate grant, not a way around a boundary:
it is the answer to *"my catalog is configured for readers and I also need to
reclaim storage"*, not to *"the catalog said no"*. The safety invariants that
govern what may be deleted are unchanged by whose credentials are in use —
see [design.md §6.6](design.md).

### Checking it worked

```bash
zamboni remove-orphans <table>            # previews; without --yes nothing is deleted
zamboni remove-orphans <table> --yes      # reports what it deleted, and what it could not
uv run bootstrap.py --show                # dev stack only: prints sts=... signing=...
```

A run that cannot list refuses up front and names the setting. A run that lists
but cannot delete prints `N file(s) could not be deleted` — both `expire` and
`remove-orphans` end with that line rather than claiming success. Either of those
is the signal that the posture is not what you thought.
