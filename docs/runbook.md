# Operator runbook: when a maintenance cycle fails

A nightly run exited non-zero, or did nothing, or did something you did not
expect. This is how to find out what happened.

For setting maintenance up in the first place, see
[user_guide.md](user_guide.md). For running each step by hand and the reasoning
behind the sequence, see [runbook-dev.md](runbook-dev.md). Design rationale is
in [design.md](design.md).

---

## 1. Start with the exit code

The exit code tells you which of four quite different things happened, and it
is the fastest way to avoid investigating the wrong one.


| Code | Means | Do |
|---|---|---|
| 0 | Success | — |
| 2 | Usage error: bad flag, missing `--uri`/`--warehouse`, negative guard | Fix the invocation |
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

---

## 2. Get the logs, and get a stack trace

**Capture stderr.** That is the whole requirement, and a cron line can do it:

```cron
17 3 * * * cd /srv/zamboni && zamboni maintenance --yes --verbose >> /var/log/zamboni/cron.log 2>&1
```

`2>&1` is the load-bearing part. Redirecting only stdout captures the summary
and loses the traceback, which is the one thing you cannot reconstruct
afterwards.

[devops.md §1](devops.md) argues against a shell wrapper, and it is right about
the thing it is arguing against: a wrapper must never loop the six verbs or the
tables, because that order is load-bearing and `maintenance` already owns it.
There are two narrow reasons to have one anyway, and neither involves
reimplementing anything:

- **A dated log file.** `%` is special in a crontab and has to be escaped, so
  `date +%F` in a cron line is a quoting trap. In a script it is ordinary.
- **`set -euo pipefail`** if you do anything before the `zamboni` call — start a
  Trino, wait for a Spark server — so a failed prerequisite stops the run
  instead of proceeding into a maintenance that cannot work.

If neither applies, the cron line above is complete and a wrapper adds nothing.
When one does:

```bash
#!/usr/bin/env bash
# /usr/local/bin/zamboni-nightly
set -euo pipefail

LOG=/var/log/zamboni/$(date +%F).log
cd /srv/zamboni

# --verbose logs every operation and its result. `2>&1` matters: the stack
# trace of an unexpected failure goes to stderr, and it is the one thing you
# cannot reconstruct after the fact.
exec zamboni maintenance --table-config table-config.json --yes --verbose \
     >> "$LOG" 2>&1
```

What each part buys you:

| | Why |
|---|---|
| `set -euo pipefail` | a failed prerequisite stops the run instead of continuing into a maintenance that cannot work |
| `>> "$LOG" 2>&1` | **stderr is where the traceback is.** Redirecting only stdout captures the summary and loses the cause |
| `--verbose` | one line per table per operation, so you can see how far it got |
| dated log file | a failure at 03:00 on Tuesday is still readable on Friday, and `date +%F` is awkward inside a crontab |
| `exec` | the wrapper does not sit between cron and the exit code |

Note what is *not* in it: no loop over verbs, no loop over tables, no `source
.env`, no flag assembly. Those belong to `maintenance`, `--env` and `--profile`
respectively, and a wrapper that takes them on is the one devops.md warns
about.

Without `--verbose`, Zamboni prints per-table results but not the operation
boundaries, so a failure looks like it happened "somewhere in the run". With it,
the last line before the traceback names the table and the operation.

**Getting a traceback at all.** Expected conditions — a blocked table, a failed
safety check, a bad flag — exit with codes 2/3/4 and a message rather than a
traceback, because they are answers rather than crashes. An unexpected failure
prints a full Python traceback to stderr. If you have a message but want the
frames, re-run the single operation that failed with `-v`:

```bash
zamboni -v remove-orphans acme.events --table-config table-config.json
```

Note there is no `--yes` there. Reproducing a failure in preview mode is safe
and is almost always enough, because the operations do their listing, planning
and checking before they commit anything.

---

## 3. Ask the table what state it is in

`zamboni describe` is read-only and is the first thing to run against a table
that behaved oddly:

```console
$ zamboni describe acme.events
```

It reports the live data-file count and bytes, a size histogram, the partition
specs in use, the sort order id, delete-file counts, and any blockers or
warnings. Blockers are what produce exit 3; they are named, not inferred.

For a *per-partition* view — which is what you want when compaction is not
keeping up, or when you are sizing memory — use `zamboni plan`. It prints one
line per group it would build and one per partition it is skipping, with the
reason:

```console
$ zamboni plan acme.events --table-config table-config.json
acme.events: 3 group(s), 41 file(s), target 134217728 bytes, snapshot 1169625616853321267
  skipped spec=0 partition=(2026-08-07): 1 candidate file(s) < min_input_files=2
```

The `maintenance --status` flag reports file counts and bytes before and after
the whole cycle, which is how you answer "did anything actually happen":

```bash
zamboni maintenance --table-config table-config.json --status --yes
```

### A health check for monitoring

`maintenance` in preview mode changes nothing, touches every configured table,
and exits non-zero if the catalog is unreachable or a table is blocked. That
makes it a usable liveness check:

```bash
# exits 0 if every configured table is reachable and unblocked
zamboni maintenance --table-config table-config.json >/dev/null
```

Pair it with `zamboni doctor`, which reports whether the installed PyIceberg is
usable at all — a probe rather than a version comparison, so it stays true
across upgrades.


`zamboni describe` and `./bin/zamboni-demo status` report these. Rules of thumb, with the
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

---

## 4. Common failures, and what they mean

**`remove-orphans` alone fails, everything else passes.** On Spark, this is the
S3A credential path — see Mode 4 in [user_guide.md](user_guide.md). On the local
engine, it is usually a catalog that remote-signs rather than vending
credentials, which refuses the bucket listing orphan removal needs.

**Everything succeeds and storage does not fall.** Expected until `expire` has
a window to work in. Compaction frees nothing by itself: the files it supersedes
are still referenced by the snapshot it compacted out of, which is what keeps
time travel working. Check the `on disk` line against `data files`; `N
superseded` means the files are being retained deliberately.

**Unreferenced file counts grow every run.** Either the age guard is holding
them, which is reported explicitly (`5 file(s) left in place: younger than the
3-day age guard`), or deletion is failing — look for `N file(s) could not be
deleted`, which is a permissions answer, not a Zamboni one.

**A run took far longer than usual, or the host ran out of memory.** Compaction
holds roughly twice a partition's on-disk size in RAM, and an unpartitioned
table is one partition. `zamboni plan` shows the groups it would build. See the
memory section of [user_guide.md](user_guide.md).

**Exit 3 on a table that used to work.** Something started writing equality
deletes to it — usually a Flink or merge-on-read pipeline. PyIceberg cannot read
them, so the table is refused rather than compacted into something that
resurrects deleted rows.

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

**Reporting any of the above.** Quote `zamboni --version`, which reports all three
versions that matter:

```console
$ zamboni --version
zamboni 0.1.0 (pyiceberg 0.11.1, python 3.13.14)
```

The PyIceberg version is not decoration. Which operations this tool will attempt
is decided by probing the installed build rather than comparing versions
(`zamboni doctor` prints the full result), so the same Zamboni refuses equality
deletes against one PyIceberg and reads them against another. What a version
change is allowed to alter — including the defaults that decide what gets deleted
— is in [releasing.md](releasing.md).
