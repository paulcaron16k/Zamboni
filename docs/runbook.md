# Operator runbook

How to run Zamboni against a real warehouse: in what order, how often, and which
number to change when something looks wrong.

Design rationale is in [design.md](design.md); this is the operational half.
Where a number here is measured rather than chosen, it says so.

---

## 0. Before the first run

**Check the warehouse can reclaim storage at all.** This is the one precondition
worth verifying before anything else, because a warehouse that cannot reclaim
looks completely healthy: reads work, writes work, compaction appears to succeed.

```bash
uv run scripts/verify-live.py --port 8181 --warehouse your_warehouse \
    --s3-host <host> --s3-port <port>
```

If `test_the_warehouse_vends_credentials_rather_than_signing` fails, stop. A
remote-signing Lakekeeper warehouse refuses `ListObjectsV2`, `HeadObject` and
multi-object `DELETE`, so `expire` will commit and free nothing while
`remove-orphans` cannot run at all. Measured, not theoretical — see
[live-verification.md](live-verification.md). The fix is a warehouse with
`sts-enabled: true`, or direct S3 credentials.

**Then dry-run everything.** Every mutating verb except `compact` previews without `--yes`:

```bash
zamboni describe  your.table          # read-only: layout, blockers, warnings
zamboni plan      your.table          # what compaction would rewrite, and what it skips
zamboni expire    your.table          # what retention would expire and delete
zamboni remove-orphans your.table     # what is unreferenced, and what the guard holds back
```

> **One inconsistency to know.** `compact` *refuses* to run without either
> `--yes` or `--dry-run` and exits 2. The other five mutating verbs treat a
> missing `--yes` as a dry run. So `zamboni compact your.table` errors, while
> `zamboni expire your.table` previews. Tracked as ZMBNI-911.

---

## 1. The order, and why each position matters

Run all six as one sequence. The order is not arbitrary — three of the five gaps
between them are load-bearing.

```bash
zamboni compact                 your.table --table-config c.json --yes
zamboni apply-properties        your.table --table-config c.json --yes
zamboni remove-dangling-deletes your.table --table-config c.json --yes
zamboni rewrite-manifests       your.table --table-config c.json --yes
zamboni expire                  your.table --table-config c.json --yes
zamboni remove-orphans          your.table --table-config c.json --yes
```

| Step | Why here |
|---|---|
| `compact` | Rewrites data files. Frees **no storage** — the files it supersedes are still referenced by the snapshot it compacted out of, which is what keeps time travel working |
| `apply-properties` | Anywhere before `remove-orphans`. Trimming the metadata log is what makes stale `metadata.json` unreferenced for the last step to find, so only that ordering constraint is real |
| `remove-dangling-deletes` | After compaction, because compaction is what *creates* dangling deletes: it applies them and writes files with a higher sequence number |
| `rewrite-manifests` | After the delete removal, so the manifests it emptied are gone before we decide how to regroup what remains |
| `expire` | Drops the old snapshots, which is the only thing that unreferences the superseded files. Deletes exactly the difference |
| `remove-orphans` | Last, sweeping what nothing references: abandoned writes, and metadata versions dropped from the log |

`./bin/demo maintenance` runs exactly this sequence — `src/himsdemo/cli.py` is the
worked example, and the two are kept in the same order deliberately.

**The step most often left out is `expire`.** Without it, compaction and manifest
rewriting make queries faster and storage *larger*.

---

## 2. Cadence

There is no schedule to recommend, because the useful one falls out of your own
retention settings. The arithmetic:

**Compaction** is driven by ingest. One commit produces at least one data file,
one manifest, one manifest list and one `metadata.json`, and nothing is updated
in place. So after *N* commits you have ≥ *N* of each. In the demo, one commit
per hour of activity gave 58 data files in five days — at which point metadata
was **340 KiB describing 218 KiB of data**, and a query for one day opened one
manifest per data file it read. Compact often enough that the count stays in the
tens, not the thousands: `commits_per_day × days_between_runs` is the number to
keep an eye on.

**Expiry and orphan removal are gated by their own windows**, so running them
more often than the window is cheap but reclaims nothing:

| | Default | Reclaims nothing until |
|---|---|---|
| `expire_snapshots.max_snapshot_age_days` | 5 (Iceberg's) | a snapshot is 5 days old |
| `remove_orphan_files.older_than_days` | 3 (Iceberg's) | an unreferenced file is 3 days old |

So a nightly run of the whole sequence is a reasonable default: compaction does
work every night, and the two reclaim steps do nothing for the first few nights
and then steady out. They will report that plainly rather than looking broken:

```
5 file(s) (7.0KiB) left in place: younger than the 3-day age guard
```

**Run it on one schedule, not six.** The steps are ordered for a reason and
splitting them across cron entries reintroduces the gap the order closes.

---

## 3. Sizing the orphan guard

The single most consequential number here, because it is the one that can destroy
data if set wrong.

**The rule:** `older_than_days` must exceed the longest single write the warehouse
performs. A file that has been written but not yet committed is indistinguishable
from an orphan — there is no flag on it, nothing in the catalog refers to it, and
the age guard is the only thing standing between it and deletion.

**The trap:** the longest write is usually *your own compaction*, not your ingest.
A tap writing hourly micro-batches commits in seconds. A compaction rewriting a
large partition can run for a long time and writes every output file before it
commits anything — which is exactly the window the guard protects.

**Measure it rather than guess.** Time your largest compaction:

```bash
time zamboni compact your.biggest.table --table-config c.json --yes
```

Then set `older_than_days` to comfortably more than that, and never less than
Iceberg's default of 3. If your longest compaction is 4 hours, 3 days is already
17× headroom; if it is two days, 3 is too tight.

**What the guard does not protect.** Long-running *readers*. A query holding a
file list while the file is deleted underneath it fails, and file mtime is the
wrong clock for that — the file may be days old and still in use. Snapshot
retention is the mechanism there: keep `max_snapshot_age_days` longer than your
longest query.

**`--reclaim-now` / a zero guard.** Only on a warehouse nothing else is writing.
It prints a warning for that reason. The demo uses it so storage visibly falls;
production should not.

---

## 4. What to watch

`zamboni describe` and `./bin/demo status` report these. Rules of thumb, with the
reasoning so you can adapt them:

| Signal | Concerning when | Why |
|---|---|---|
| data files per partition | growing run to run | compaction is not keeping up with ingest, or `min_input_files` is set too high |
| average file size | far below `target_file_size_bytes` | same, and each small file costs a separate object open |
| metadata bytes vs data bytes | metadata approaching or exceeding data | planning cost now dominates. The demo hit this in five days |
| manifests per data file | approaching 1:1 | the metadata tier has stopped being an index and become a second copy of the file list — `rewrite-manifests` is the answer |
| unreferenced files | growing across runs | either the guard is holding them (expected, and reported) or deletion is failing. Check `N file(s) could not be deleted` |
| `! dangling-delete-files` | present after maintenance | `remove-dangling-deletes` is disabled or was skipped |

---

## 5. Exit codes

| Code | Means | Do |
|---|---|---|
| 0 | Success | — |
| 2 | Usage error: bad flag, missing `--uri`/`--warehouse`, negative guard, or `compact` without `--yes`/`--dry-run` | Fix the invocation |
| 3 | The table is **blocked**: format version 1 or 3, equality deletes, or an unsafe PyIceberg build | Read the reason. These are refusals, not failures — see [design.md §6](design.md#6-constraints) |
| 4 | A **safety check aborted** the run. Nothing was deleted | Investigate before retrying. This is the interesting one |

**Exit 4 deserves attention.** It means a reclaim operation found its own view of
the world untrustworthy and stopped: a referenced file missing from a storage
listing, an empty reference category on a table that has snapshots, or a file
marked for deletion still referenced after the commit. During live verification
this fired on a real keying bug and prevented the deletion of every live file. If
you see it, the message names the condition; do not work around it with a
narrower scope.

---

## 6. Recovery

**A compaction failed partway.** Nothing was committed — a run commits once, at
the end. Output files that were written are now orphans and the next
`remove-orphans` will sweep them once they pass the guard. Re-run the compaction.

**With `--partial-progress`, a run failed partway.** Groups that committed stay
committed. Re-running picks up the rest; the finished ones are already at target
size and will be skipped by `min_input_files`.

**Expiry deleted files and something now cannot read.** Check whether the reader
was pinned to an expired snapshot. Expiry only deletes files that *no retained
snapshot references*, computed as a set difference, so a live table cannot lose a
file it needs — but a consumer holding an old snapshot id can lose the one it
asked for. That is what `min_snapshots_to_keep` and `max_snapshot_age_days` are
for.

**A named tag or branch disappeared.** `max_ref_age_days` was configured and the
ref was past it. This is off by default; if it was on unintentionally, remove it
from `table-config.json`. There is deliberately no CLI flag for it, so it can only
have come from the config or a table property.

**Storage did not fall after maintenance.** Expected, unless `expire` ran and its
window has opened. Compare the `data files` and `on disk` lines: `on disk` staying
high with `N superseded` means the files are retained for time travel, exactly as
designed.
