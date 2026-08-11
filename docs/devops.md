# Running Zamboni in production

The [runbook](runbook.md) explains *what* each operation does and why the order
matters. This is the other half: what you actually put in a crontab, where
configuration lives, and how it works when you have one warehouse per customer.

**The short version.** One command, one line per warehouse:

```cron
17 2 * * *  cd /srv/zamboni && zamboni maintenance --warehouse acme >> /var/log/zamboni/acme.log 2>&1
```

Everything below is why that line is the whole interface.

---

## 1. No shell wrapper

The obvious shape for this is a `run-maintenance.sh` that sources an env file,
loops the six verbs in order, and checks exit codes. **Don't write it**, and the
`maintenance` command exists so you don't have to.

The reason is not tidiness. **The six-verb order is load-bearing** — runbook.md
§1 spends a table explaining why each position matters, and three of the five
gaps between them are real constraints rather than preference. A wrapper puts
that order in a file that is copied between sites, edited under pressure, and
never tested. The tool already knows the order, is versioned with it, and has
tests that fail if it changes.

What a wrapper usually adds, and where it actually belongs:

| Wrapper does | Instead |
|---|---|
| `source .env` | `--env`, defaulting to `./.env` (§3) |
| Assembles twenty flags | `--profile`, defaulting to `./zamboni.yml` (§2) |
| Loops the six verbs | `maintenance` |
| Loops tables | `maintenance` does every table in the config |
| Redirects a log | `>>` in the cron line |
| Reports before/after | `--status` |
| Prevents overlapping runs | `flock`, which takes a command directly |

That last one is the only genuine gap, and it is still not a wrapper:

```cron
17 2 * * *  cd /srv/zamboni && /usr/bin/flock -n /var/lock/zamboni-acme.lock \
              zamboni maintenance --warehouse acme >> /var/log/zamboni/acme.log 2>&1
```

**Overlapping runs are worth preventing.** Orphan removal deletes files it finds
unreferenced; a compaction running concurrently in another process has written
output files it has not yet committed. The age guard is what protects those, and
it is sized for *ingest*, not for a second copy of maintenance. `flock -n` makes
a late-running job skip rather than pile up.

---

## 2. `zamboni.yml` — the non-secret configuration

Found automatically at `./zamboni.yml`, or given with `--profile`. Everything
that is not a credential:

```yaml
# Catalog connection. The URI and warehouse a client needs; no credentials.
uri: https://lakekeeper.internal/catalog
warehouse: acme

# Which engine performs the work. `zamboni engines` reports what each supports.
engine: local

# Where per-warehouse table configuration lives. See §5.
root: /srv/zamboni

# Which operations run, in the runbook order. Omit the key to run all six.
# Listing them out is how you disable one without editing a cron line.
operations:
  - compact
  - apply-properties
  - remove-dangling-deletes
  - rewrite-manifests
  - expire
  - remove-orphans

# Optional: only these tables. Default is every table in table-config.json.
# tables:
#   - acme.events
```

**Resolution order**, highest first: a command-line flag, then a `ZAMBONI_*`
environment variable, then `./zamboni.yml`, then `$ZAMBONI_ROOT/zamboni.yml`,
then the built-in default. A flag always wins, so a one-off run can override the
profile without editing it.

---

## 3. `.env` — the secrets, separately

Found at `--env`, then `./.env`, then `$ZAMBONI_ROOT/.env` -- the same order
as the profile. Copy
[env.sample](../env.sample), which lists every variable Zamboni reads.

**Why a file rather than the crontab.** Cron gives a job almost no environment,
so credentials have to come from somewhere. Putting them in the crontab itself
puts them in `crontab -l`, in every backup of `/var/spool/cron`, and in the
process table of anything that inspects the command line. A `0600` file read by
the process is the smaller exposure.

**Why not both.** Real environment variables still win over the file, so a
container or a systemd unit that injects secrets properly needs no `.env` at all
— the file is a convenience for cron, not the mechanism.

---

## 4. `--status`

Prints warehouse state before and after the run:

```
$ zamboni maintenance --warehouse acme --status
acme, before:
  3 tables, 1,284 data files, 4.1GiB data, 812MiB metadata
...
acme, after:
  3 tables, 47 data files, 4.0GiB data, 61MiB metadata
  reclaimed 219MiB
```

This is what makes a nightly log answer "did it help" without a second tool. The
numbers to watch, and what they mean when they go the wrong way, are in
[runbook.md §3](runbook.md).

---

## 5. Multi-tenant: one warehouse per customer

The layout Zamboni expects, rooted at `ZAMBONI_ROOT` (default `~/.zamboni`,
usually `/srv/zamboni` under a service account):

```
$ZAMBONI_ROOT/
  zamboni.yml                      # fleet-wide defaults
  .env                             # fleet-wide credentials
  configs/
    acme/table-config.json         # per-customer table layout
    globex/table-config.json
    initech/table-config.json
```

`--warehouse acme` -- or `--db acme`, the same flag -- reads
`$ZAMBONI_ROOT/configs/acme/table-config.json`. Nothing else changes between
customers, which is the point: the per-customer surface is one file in a
predictable place, so provisioning a new customer is writing that file and
adding a cron line.

**That file names its warehouse too, and is checked against this one.** The
directory selects; the `warehouse` key in the file confirms. It exists because
this layout invites exactly one mistake -- copy `acme`'s config into `globex`'s
directory, forget to edit a line, maintain the wrong tenant's tables all night --
and one line of assertion turns that into an error before anything is touched.

### One invocation per warehouse, not one loop

**This is the recommendation, and the reason is blast radius.** A single process
sweeping every customer has one exit code, one log, and one failure mode that
stops the rest. Per-warehouse invocation gives you:

- **Isolation.** A table in `acme` that aborts on a safety check (exit 4) must
  not stop `globex` being maintained. With separate invocations that is free;
  inside one loop it is a policy you have to get right.
- **A per-customer exit code**, which is what alerting keys on. "Last night's
  maintenance failed" is not actionable; "acme failed, 40 others succeeded" is.
- **Staggering.** Five hundred customers at 02:00 is five hundred concurrent
  compactions against one catalog and one object store. Separate lines can be
  spread across the window; a loop is serial or it is a thundering herd.
- **Retries and timeouts that already exist.** Your scheduler has them. A loop
  inside Zamboni would be reimplementing a job runner badly.

There is deliberately **no `--all-warehouses`**. An earlier draft of this
document described one in detail, including what its `--help` said; no such flag
was ever written. The claim is removed rather than the flag added, because every
argument above is an argument against it: a loop inside Zamboni would have one
exit code, one log, no staggering, and would be reimplementing the retry and
timeout logic your scheduler already has.

For a small fleet where per-warehouse cron lines feel like overkill, generate
them -- `zamboni warehouses` exists for exactly that, and is the subject of the
next section.

### Discovery generates the schedule; it is not the scheduler

`zamboni warehouses` lists what the catalog knows about:

```console
$ zamboni warehouses
acme
globex
initech
```

That is deliberately a plain list, because its job is to be input to something
else — generating a crontab, a Kubernetes CronJob per tenant, or an Airflow DAG:

```bash
zamboni warehouses | awk '{printf "%d 2 * * *  cd /srv/zamboni && zamboni maintenance --warehouse %s >> /var/log/zamboni/%s.log 2>&1\n", NR%%60, $1, $1}'
```

**Zamboni does not schedule anything**, and this boundary is deliberate. A tool
that discovers, schedules, retries and alerts is a job runner; you already have
one, and it is better at those four things than a maintenance tool will ever be.
What Zamboni owns is doing the work correctly and reporting what happened.

### When one customer fails

Exit codes are unchanged from [runbook.md §1](runbook.md) — 0 success, 2 usage,
3 refused, 4 a safety check aborted — and `maintenance` returns the *worst* code
any operation produced, so a partial failure is never reported as success.

Exit 4 on one customer is the interesting case and it does not mean "retry".
Something about that warehouse looked untrustworthy enough to stop before
deleting: a referenced file missing from a listing, or another table sharing a
location. Read the message, fix the cause, and re-run that one warehouse. The
other 499 are unaffected, which is the argument for per-warehouse invocation in
one sentence.
