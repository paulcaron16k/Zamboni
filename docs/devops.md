# Running Zamboni in production

The [runbook](runbook.md) explains *what* each operation does and why the order
matters. This is the other half: what you actually put in a crontab, where
configuration lives, and how it works when you have one warehouse per customer.

**The short version.** One command, one line per warehouse:

```cron
47 1 * * *  sleep $(shuf -i 0-3600 -n 1); cd /srv/zamboni && flock -n /var/lock/zamboni-acme.lock zamboni maintenance --warehouse acme >> /var/log/zamboni/acme.log 2>&1
```

Everything below is why that line is the whole interface. The `sleep` spreads
the fleet's start times and the `flock` stops a run starting while the last one
is still going — [§1](#spreading-the-load) explains both, and why neither is
optional at fleet scale.

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
47 1 * * *  sleep $(shuf -i 0-3600 -n 1); cd /srv/zamboni && /usr/bin/flock -n /var/lock/zamboni-acme.lock zamboni maintenance --warehouse acme >> /var/log/zamboni/acme.log 2>&1
```

One line, and it has to be: crontab(5) — "There is no way to split a single
command line onto multiple lines, like the shell's trailing `\`". An earlier
version of this example ended its first line with one.

**Overlapping runs are worth preventing.** Orphan removal deletes files it finds
unreferenced; a compaction running concurrently in another process has written
output files it has not yet committed. The age guard is what protects those, and
it is sized for *ingest*, not for a second copy of maintenance. `flock -n` makes
a late-running job skip rather than pile up.

**That lock is the whole in-progress guard for cron.** It is per host and per
warehouse: it stops tonight's run starting while last night's is still going on
the same machine, and nothing more. Two hosts with the same crontab, or a cron
line and a `zamboni serve` against the same warehouse, are not excluded from
each other — run maintenance for a warehouse from exactly one place.

### Spreading the load

**A fleet scheduled from one template starts in one minute.** Five hundred
warehouses at `17 2 * * *` are five hundred processes listing and rewriting the
same object store at 02:17, alongside everything else in the estate scheduled
on the hour. Nothing in a cron deployment queues them — each line is its own
process — so the spike is the fleet's whole width.

`zamboni serve` spreads its firings by default (±5% of the interval, capped at
±30 minutes — [event-driven-maintenance.md §4](event-driven-maintenance.md#4-configuration)).
The crontab equivalent is to **start the line 30 minutes early and sleep a
random 0–60 minutes**:

```cron
# acme, nominally 02:17: starts between 01:47 and 02:47, a fresh time each night.
47 1 * * *  sleep $(shuf -i 0-3600 -n 1); cd /srv/zamboni && flock -n /var/lock/zamboni-acme.lock zamboni maintenance --warehouse acme >> /var/log/zamboni/acme.log 2>&1
47 1 * * *  sleep $(shuf -i 0-3600 -n 1); cd /srv/zamboni && flock -n /var/lock/zamboni-globex.lock zamboni maintenance --warehouse globex >> /var/log/zamboni/globex.log 2>&1
```

Three details in that line are load-bearing:

| | Why |
|---|---|
| `shuf -i 0-3600 -n 1`, not `$RANDOM % 3600` | crontab(5): "Percent-signs (%) in the command, unless escaped with backslash (\\), will be changed into newline characters", so `%` cuts the command short. And the line runs under `/bin/sh`, which need not have `$RANDOM` at all |
| `sleep` **before** `flock` | the sleep does not hold the lock, so a run that is genuinely still going from last night is what `flock -n` sees — not tonight's own sleeping copy |
| one line | see above: no trailing `\` |

`shuf` is GNU coreutils. Cron runs in the **daemon's** time zone, not UTC
(crontab(5): "It currently does not support per-user timezones"), so the same
line on two hosts in different zones runs at different instants — the service
evaluates schedules in UTC precisely to avoid that.

**Under systemd, use a timer instead** — `RandomizedDelaySec=` is the same idea
built in, drawn afresh "before each iteration" (systemd.timer(5)):

```ini
# /etc/systemd/system/zamboni-acme.timer
[Timer]
OnCalendar=*-*-* 01:47:00 UTC
RandomizedDelaySec=1h
# The default AccuracySec=1min coalesces timers and partly undoes the spread;
# systemd.timer(5) says to set it to 1us "to optimally stretch timer events".
AccuracySec=1us
# Catch up once on a run missed while the host was down, as `serve` does.
Persistent=true

[Install]
WantedBy=timers.target
```

with the matching `zamboni-acme.service` running the same `flock -n … zamboni
maintenance --warehouse acme` as the cron line, minus the `sleep`. Leave
`FixedRandomDelay=` at its default, false: true reuses one offset for every
firing, so two warehouses that collide collide every night.

**Turning the spread off is a choice to make knowingly.** Fixed minutes per
warehouse (`07 2`, `23 2`, `41 2`, …) are fine for a handful of warehouses that
someone maintains by hand. They are not fine for a fleet that a provisioner
templates, which is the case the default is for.

---

## 2. `zamboni.yml` — the non-secret configuration

Found automatically at `./zamboni.yml`, or given with `--profile`. Everything
that is not a credential:

```yaml
# zamboni.yml
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

## 6. Collecting what the runs report

Every run reports its own economics on stdout — what it maintained, what it
skipped, and what share of the scheduled work had no input:

```
18 operation(s) on 3 table(s): 9 maintained, 9 skipped, 0 failed -- 50% of the work had no input
```

That line is for whoever is watching. `--json` is for whoever is not:

```bash
zamboni maintenance --warehouse acme --yes --json /var/log/zamboni/acme.jsonl
```

One JSON object per run, **appended**, carrying the counters, the per-operation
outcomes, the exit code, the start and end times in UTC, and the three versions
that produced it. JSON Lines rather than a JSON array because a cron line has to
append without reading what is already there — a series that needs a closing
bracket is corrupt every time a run is killed.

`-` writes the object to stdout instead, for a container that ships stdout and
has nowhere to put a file.

**A telemetry fault is not a maintenance fault.** A bad path, a full disk or an
unserialisable result is reported on stderr and the exit code stays the
maintenance exit code. A run that did its work and exits non-zero because a log
directory was missing teaches an operator to distrust the exit code, which is
the one thing here that has to stay trustworthy.

**One file per warehouse, or one file for all of them?** Either, on a local
filesystem: concurrent appends from separate `zamboni` processes do not
interleave, measured at 32 processes appending 500 KB each. That guarantee is
the local filesystem's, not Zamboni's — **on NFS, use one file per warehouse**,
because append is not atomic there and a lock would be no more dependable.

The per-warehouse layout in §5 gives you one file per customer for free, which
is also what makes `zamboni runs` able to break the fleet down by warehouse.

### Iceberg-shaped metrics, if you have somewhere to put them

`--metrics` is separate from `--json` and answers a different question: `--json`
records what the *run* did, `--metrics` reports what each *commit* did, in
Iceberg's own vocabulary.

```bash
zamboni maintenance --warehouse acme --yes --metrics catalog
```

- **`catalog`** POSTs a `CommitReport` to the catalog's metrics endpoint. This
  is what Java does by default, so a Lakekeeper already collecting commit
  metrics from Spark jobs starts collecting Zamboni's in the same shape. A
  catalog that does not implement it answers 404/405/501 and the reporter stops
  asking for the rest of the run.
- **`log`** writes one JSON line per report to the `zamboni.metrics` logger —
  the same zero-infrastructure route as `--json`.
- **`otel`** exports over OTLP/HTTP when `iceberg-zamboni[otel]` is installed.
  Without the extra it is **silent by design**, and the run says so on stderr
  rather than leaving you wondering.

Repeat the flag for several. It costs one extra metadata load per *committing*
operation, which is why it is off by default.

**Configuring `otel`** is OpenTelemetry's own environment variables, read by its
SDK — set them in the process environment (the cron line, the unit, the pod
spec), **not in `.env`**, which supplies only `ZAMBONI_*`:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
OTEL_SERVICE_NAME=zamboni-acme \
  zamboni maintenance --warehouse acme --yes --metrics otel
```

`OTEL_EXPORTER_OTLP_HEADERS` and `OTEL_RESOURCE_ATTRIBUTES` work as the SDK
documents them; the service is named `zamboni` unless you name it. The metrics
are sent when the run ends — a run is shorter than the SDK's 60 s export
interval — so **an unreachable collector delays the exit**, by up to
`OTEL_EXPORTER_OTLP_TIMEOUT` (default 10 s; measured 7.8 s). It never changes
the exit code. Embedded in an application that has configured its own SDK,
Zamboni uses that one and configures nothing.

**None of this replaces §6.** The monthly review runs on `zamboni runs`, which
needs no collector at all. Metrics are for a deployment that already has
somewhere to send them.

### Reading the series back

```bash
zamboni runs /var/log/zamboni
```

```
8 run(s), 2026-09-26T14:18:44Z to 2026-09-26T14:18:59Z
  120 operation(s) on 5 table(s): 75 maintained, 45 skipped, 0 failed -- 38% of the work had no input
  runs with failures       0 (worst exit 0)
  longest run              0.9s

  acme                        4 run(s)    38% skipped    0 failed run(s)
  globex                      4 run(s)    38% skipped    0 failed run(s)
```

A directory argument reads the `.jsonl` files in it, so the command is the same
whether you kept one file or five hundred. `--json` emits the aggregate for a
dashboard instead of prose.

It **exits 0 whatever it finds**, including a week of nothing but failures. This
is a report: a verb that exits non-zero for successfully telling you bad news
gets wrapped in `|| true` and then ignored. The one exception is finding no run
logs at all, which is exit 2 — a path matching nothing is a mistyped path far
more often than it is a fleet that did not run.

A run log is written by a cron line on a machine nobody is watching, so it will
eventually contain a half-written record from the night the box rebooted. Those
are **counted and reported**, not raised on:

```
  unreadable records       1
```

A number beside that line is something you can judge. A traceback instead of
last week's figures is not.

### What the numbers are for

| Figure | Question it answers | What to do about it |
|---|---|---|
| `% of the work had no input` | are we scheduling maintenance that has nothing to do? | high and stable is *fine* — the skip is cheap. It is the input to the [ZMBNI-106](https://github.com/paulcaron16k/Zamboni/issues/106) decision on event-driven triggering, not an alert |
| `runs with failures`, `worst exit` | is maintenance actually completing? | **this is the alert.** Exit 4 means a safety check stopped before deleting: read the message, fix the cause, re-run that warehouse ([§5](#when-one-customer-fails)) |
| `longest run` | will the window hold as the fleet grows? | trending toward the gap between cron firings is the signal to split the schedule, before runs start overlapping |
| `unreadable records` | is the collection itself healthy? | more than the occasional reboot means something is truncating the file — check rotation |
| `built by N version(s)` | did a figure move because the workload changed, or because the build did? | shown only when the series spans builds. Which operations Zamboni even attempts depends on the installed PyIceberg, so compare like with like before drawing a conclusion |

A per-warehouse breakdown appears whenever the series covers more than one,
sorted by name so two weeks' output can be diffed. "The fleet is at 50%" is not
actionable; "globex is at 5% and everything else is at 60%" is.

## 7. The monthly review

Collecting the figures is §6. This section is the part that makes them matter:
somebody looks, on a cadence, and writes down what they decided.

**Owner: Paul. Cadence: monthly. Next review: 2026-10-26.**

Nothing enforces that date — no test fails when it passes. That was a deliberate
choice (ZMBNI-134) and it is the weak link in this section, so it is written
here rather than left implicit: if the review is not held, the loop is a
document and not a loop.

### The procedure, in full

```bash
zamboni runs /var/log/zamboni
```

Then, for each of these, a row in the log below:

1. **Skip share, per warehouse.** Not an alert. A high, stable figure means the
   watermark check is doing its job. What matters is whether it is *stable* —
   a warehouse that moved from 60% to 5% took on a new writer, and a warehouse
   that moved the other way may have lost one.
2. **Failed runs and the worst exit code.** This *is* the alert, and it should
   normally be zero. Exit 4 means a safety check stopped before deleting
   anything; [§5](#when-one-customer-fails) is the response.
3. **Longest run against the cron gap.** If the longest sweep is approaching the
   interval between firings, split the schedule *before* runs start overlapping
   rather than after.
4. **Unreadable records.** More than the occasional reboot means something is
   truncating the file.
5. **Whether the series spans Zamboni or PyIceberg versions.** If it does,
   compare like with like before concluding that anything moved.

### What each figure is *not* for

A skip share is an economics figure, not a health figure. It is tempting to
alert on it because it is the most eye-catching number in the report, and that
would generate a page every night for a fleet that is working perfectly. The
health figures are items 2 and 3.

### Review log

The first row is a **local baseline**, not production: eight runs over two
synthetic warehouses, recorded so the first production figure has something to
be surprising against rather than landing with no context. It is labelled as
such, because a baseline quietly mistaken for production evidence is worse than
no baseline.

| Date | Window reviewed | Runs | Skip share | Failed runs | Longest run | Decided |
|---|---|---|---|---|---|---|
| 2026-09-26 | local, synthetic | 8 | 38% | 0 | 0.9s | Baseline only — **not production**. Recorded at the close of ZMBNI-133 so the first real reading has a comparison. No decision taken |
| _next: 2026-10-26_ | | | | | | first review with production data; feeds the [ZMBNI-106 gate](https://github.com/paulcaron16k/Zamboni/issues/106) |

### The one-off decision this feeds

Separately from the standing health question, this data was to decide whether
to build phases 4 and 5 of event-driven maintenance. Both were built ahead of it
by their owner's decision, recorded on #106: phase 4, the scheduler and `zamboni
serve` (§8 below), on 2026-09-30, and phase 5, the NATS consumer (§8, "Events from
NATS"), to make `zamboni serve` feature-complete for 0.6.0. Phase 6, partition
targeting, shipped on its own merits. The review still answers whether events are
worth *running* — the decision record, with the evidence that would say so, is
[event-driven-maintenance.md §8](event-driven-maintenance.md).

The short version, because it is easy to read the wrong conclusion out of a
large number: **phases 1–2 already capture the whole "no input at all" saving
without any event plumbing.** A large skip share on its own is therefore not an
argument for building more. Phases 4–6 are a *latency* argument — they would
reach the same saving sooner — and they need evidence that latency matters
before they are worth it.

---

## 8. `zamboni serve` — a fleet with no crontab

The third way to run Zamboni, beside cron (§1) and the library: one long-running
process reads a **fleet file** — warehouses, a schedule each, their table config
— and maintains them on its own schedule. Each table runs exactly what
`zamboni maintenance <table>` would, in a bounded pool of worker processes, so
everything above about the profile, `.env`, exit codes and `--json` still holds.
The design is [event-driven-maintenance.md §2–§7](event-driven-maintenance.md).

```bash
zamboni serve --fleet /etc/zamboni/fleet/fleet.yaml --yes --json /var/log/zamboni/runs.jsonl
zamboni service-status                    # the state file, as JSON
zamboni service-status --probe liveness   # exit 0 or 1, for an exec probe
zamboni config-reload                     # reload the fleet file now (SIGHUP)
zamboni runs /var/log/zamboni             # the same report cron runs give
```

Without `--yes` every table previews, forever — the same rule as everywhere
else, and the service logs a warning at start so a forgotten flag is visible.
`--json` writes one record per table in the format `maintenance --json` writes,
so §6's collection and §7's review work unchanged.

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zamboni
spec:
  replicas: 1                    # no claim protocol: two pods maintain one table twice
  strategy:
    type: Recreate               # RollingUpdate briefly runs two pods
  selector:
    matchLabels: {app: zamboni}
  template:
    metadata:
      labels: {app: zamboni}
    spec:
      terminationGracePeriodSeconds: 3600   # longer than the longest table; see below
      containers:
        - name: zamboni
          image: registry.internal/zamboni:0.6.0
          args:
            - --profile=/etc/zamboni/profile/zamboni.yml
            - serve
            - --fleet=/etc/zamboni/fleet/fleet.yaml
            - --yes
            - --json=/var/log/zamboni/runs.jsonl
            - --state-file=/run/zamboni/state.json
            - --pid-file=/run/zamboni/serve.pid
          envFrom:
            - secretRef: {name: zamboni-credentials}   # ZAMBONI_CREDENTIAL, ZAMBONI_S3_SECRET_ACCESS_KEY, ...
          resources:
            limits: {cpu: "4", memory: 8Gi}            # the pool sizes itself from these
          startupProbe:
            exec: {command: [zamboni, service-status, --state-file=/run/zamboni/state.json, --probe=startup]}
            periodSeconds: 5
            failureThreshold: 60
            timeoutSeconds: 10
          readinessProbe:
            exec: {command: [zamboni, service-status, --state-file=/run/zamboni/state.json, --probe=readiness]}
            periodSeconds: 30
            timeoutSeconds: 10
          livenessProbe:
            exec: {command: [zamboni, service-status, --state-file=/run/zamboni/state.json, --probe=liveness]}
            periodSeconds: 60
            failureThreshold: 3
            timeoutSeconds: 10
          volumeMounts:
            - {name: fleet, mountPath: /etc/zamboni/fleet}       # a directory, never subPath
            - {name: profile, mountPath: /etc/zamboni/profile}
            - {name: run, mountPath: /run/zamboni}
            - {name: spill, mountPath: /tmp}
            - {name: logs, mountPath: /var/log/zamboni}
      volumes:
        - {name: fleet, configMap: {name: zamboni-fleet}}
        - {name: profile, configMap: {name: zamboni-profile}}
        - {name: run, emptyDir: {}}
        - {name: spill, emptyDir: {}}
        - {name: logs, persistentVolumeClaim: {claimName: zamboni-logs}}
```

Each line that is not boilerplate is there for a reason that has already cost
someone, or would:

| Setting | Why |
|---|---|
| `replicas: 1`, `strategy: Recreate` | There is no claim protocol. Two pods would maintain the same table twice, and a `RollingUpdate` runs two pods for the length of the rollout. The service's own lock (beside the pid file) stops a second `serve` on the same host and pid file only — it cannot see another pod |
| `timeoutSeconds: 10` on every probe | **Margin, not need.** `zamboni service-status` answers in 0.18 s (three runs, 2026-10-08), well inside the 1 s default — it imports none of PyIceberg, DuckDB or Arrow, and a test fails if that changes (ZMBNI-154). Before 0.6.0 it took 1.4–1.5 s and the default failed every probe, restarting the pod forever without saying why. Keep the margin anyway: an exec probe on a CPU-throttled node, or one whose first run warms the filesystem cache, can take several times as long |
| `terminationGracePeriodSeconds` | SIGTERM stops intake and lets tables in flight finish; Kubernetes sends SIGKILL when the period ends. Set it above the **longest table**: run `zamboni runs` over the service's `--json` log and read *longest run* — each record is one table — then add margin. The default, 30 s, is shorter than a large compaction |
| liveness checks nothing external | It asserts only that the scheduler loop ticked in the last 180 s. A catalog or object-store outage is not fixed by a restart, and a probe that checked one would turn that outage into a restart loop |
| the fleet file as a **directory** mount | A ConfigMap mounted with `subPath` is not updated when the ConfigMap changes, so the service would never see a new tenant. Mounted as a directory it is projected by symlink swap, which the reload watcher follows |
| `/run/zamboni` and `/tmp` as `emptyDir` | The state file, the pid file and per-table scratch files go in the first; DuckDB spills to the second. A read-only root filesystem needs both writable |
| `resources.limits` | The pool reads the cgroup's CPU quota and memory limit, not the node's, and gives each worker's DuckDB its share ([event-driven-maintenance.md §3](event-driven-maintenance.md)). Without limits it sizes itself from the node |

**Credentials: from the environment, not a projected file.** `envFrom` a Secret
puts `ZAMBONI_*` variables in the environment, where they win over any file and
need no mode. The alternative — `zamboni.yml` holding credentials, projected
from a Secret — is supported, but Kubernetes projects Secret volumes at mode
`0644` and Zamboni refuses a credential file that is group- or
other-readable (§3). So that route needs **`defaultMode: 0600`**:

```yaml
volumes:
  - name: profile
    secret:
      secretName: zamboni-profile
      defaultMode: 0600        # Zamboni rejects 0644 on a file holding secrets
```

and a 0600 file is readable only by its owner, which Kubernetes decides rather
than you. Check `ls -l` inside the pod before relying on it; the environment
route has no such question.

**A hard kill is safe, and not free.** A compaction commits one snapshot at the
end, so one killed before that commits nothing, and the files it wrote are
orphans for the next `remove-orphans`. With `--partial-progress` the groups
already committed stay committed. But a service killed mid-compaction on every
deploy never finishes a large table — which is why the grace period matters.

### Outside Kubernetes

Under systemd, `KillMode=control-group` — the default — sends SIGTERM to every
process in the unit, workers included. That is handled: workers ignore SIGTERM
and SIGINT and finish under the parent's direction, so a `systemctl stop` drains
exactly as a pod does. Set `TimeoutStopSec=` above the longest table for the
same reason as the grace period. `zamboni config-reload` (SIGHUP) reloads the
fleet file at once instead of at the next 30 s poll; it reads the pid file and
refuses to signal a process that is not zamboni, because a stale pid and
SIGHUP's default action would otherwise kill an unrelated one.

**Writing the fleet file.** Write a temporary file and rename it over the old
one. The service only adopts a change it has seen unchanged on two consecutive
polls, so a file caught half-written is not adopted — but a rename is never
half-written at all.

### Events from NATS

On its own the service maintains every table on its schedule. With a `nats:`
block it also hears when a table was **written** — a `write.completed` event
([write-completed.md](write-completed.md)) — and brings that table's
write-driven maintenance forward from the next schedule to minutes after the
writes stop. It needs `iceberg-zamboni[nats]`; without the extra, a `nats:` block
is exit 2 at start, naming it.

```yaml
# zamboni.yml
nats:
  servers: [nats://nats-0.nats:4222, nats://nats-1.nats:4222]
  subject: prod.iceberg.write.completed     # one subject per environment
  jetstream: auto                          # auto | true | false
  username: zamboni
  # password: from ZAMBONI_NATS_PASSWORD, in the Secret
```

Every key has a `ZAMBONI_NATS_*` variable that overrides it alone (env.sample
lists them), so the servers and subject can sit in a committed ConfigMap and only
the password or token come from the Secret. A password or token written into the
file makes it a credential file, with §3's mode rule.

**What an event does — and does not.** A table's events are coalesced until it
has been quiet for 5 minutes, or 30 minutes after the first, whichever is sooner;
then the table is offered with reason `event`, which runs only `compact`,
`remove-dangling-deletes` and `rewrite-manifests` — never `expire` or
`remove-orphans`, which stay on the schedule. The due-check still decides whether
there is work. An event naming a table the fleet file does not list is counted
and dropped. An event for a table already being maintained runs it once more
when it finishes, so a write during a run is not lost.

**Core NATS or JetStream.** `auto` asks the account for JetStream and uses it if
it answers; `true` refuses to start consuming without it.

| | Core | JetStream |
|---|---|---|
| Delivery | at-most-once: an event published while the service is down is gone | at-least-once: a durable consumer resumes where it stopped |
| Duplicates | possible from the publisher; the service deduplicates on table and snapshot | also deduplicated by the server on `Nats-Msg-Id` |
| On the broker | a subscription | stream `zamboni_<subject>` (dots become `_`) if no stream already captures the subject, kept 7 days; durable consumer `zamboni_<subject>` |

Neither is a correctness requirement: an event lost on core NATS costs that table
the wait for its schedule. On JetStream, `max_deliveries` (default 5) bounds
redelivery after an internal error, and is applied **when the durable consumer is
created** — to change it later, delete the consumer (`nats consumer rm`) and let
the service recreate it. Name `stream` when your platform team owns streams and
Zamboni should not create one.

**When NATS goes away**, nothing stops. The service retries the broker forever,
the schedule keeps maintaining the fleet, liveness holds, and **readiness fails**
until the source reconnects — within a second of either change. `zamboni
service-status` shows `nats_connected` and, under `events`, the counts per
source: `received`, `offered`, `coalesced`, `duplicates`, `unmapped`,
`rejected`, `errors`. That a running service survives the broker being stopped
and started under it is a test (`test_killing_nats_under_zamboni_serve_costs_readiness_and_nothing_else`),
run in CI against the dev stack's NATS.

**The same counts in OpenTelemetry**, for a deployment that collects it:
`zamboni serve --metrics otel`, configured exactly as for `maintenance` (§6 —
the `OTEL_*` variables in the pod spec, not `.env`). It reports the consumer,
not the table runs:

| Metric | Unit | Per |
|---|---|---|
| `messaging.client.consumed.messages` | `{message}` | message delivered — a redelivery counts again |
| `messaging.process.duration` | `s` | message, from receipt to acknowledgement |
| `zamboni.events.offered` | `{table}` | table offered once its quiet period passed |

Each carries `messaging.system=nats` and the subject as
`messaging.destination.name`; on JetStream, the durable consumer as
`messaging.consumer.group.name`. The two messaging metrics also carry
`zamboni.event.outcome` — `pending` and `coalesced` (accepted), `duplicate`,
`unmapped` (a table the fleet file does not list), `rejected` (malformed) and
`error` (ours) — so "no events" and "events, all for tables we do not manage"
look different on a dashboard. `error.type` marks a processing failure:
`zamboni.events.EventError` for a malformed message, the exception's class for
an internal one. The `messaging.*` names are OpenTelemetry's and still marked
Development upstream, so a later convention may rename them.

Two `zamboni serve` deployments must not share a subject. On JetStream they
would share the durable consumer, and each event would reach only one of them;
on core NATS both would hear every event, for tables only one of them manages.

### Delete pressure

A merge-on-read table gets slower to read with every delete or update until
compaction folds the deletes back in. On a nightly schedule that can be a day of
slow reads. So the service also checks each warehouse every 15 minutes and
offers any table whose deletes have piled up. That table then runs `compact` and
`remove-dangling-deletes`, and the due-check still decides within them. It is on
by default; the thresholds sit in the fleet file, beside the schedule:

```yaml
# fleet.yaml
pressure:                   # the fleet's; omit for the defaults below
  delete_files: 10          # live delete files
  delete_ratio: 0.10        # delete files per data file
  deleted_rows_ratio: 0.15  # position-deleted rows per row
  check_every_minutes: 15
warehouses:
  - name: acme
    schedule: "0 2 * * *"
    pressure: {delete_files: 40}   # this key only; the rest from above
    table_config: acme/table-config.json
  - name: globex
    schedule: "30 3 * * *"
    pressure: false                # never checked
    table_config: globex/table-config.json
```

Any one threshold met is enough; `null` switches one off. **The defaults are
starting values someone chose, not measurements.** Raise them for a table whose
readers do not mind, or lower them for one queried all day. A table with
equality deletes is never offered, because compaction refuses it.

A check is `zamboni health --table-config … --json` in a worker, one
metadata load per table (about 25 ms on the dev stack). It queues for a worker
like a table does, so on a pool busy with long compactions checks wait. Run the
same command yourself to see what the service sees:

```bash
zamboni health --table-config acme/table-config.json --json /tmp/acme-health.jsonl
```

`zamboni service-status` shows each warehouse's last check under `pressure`:
`checked_at`, `tables`, `over`, `offered` and `errors`. Each table offered is
also logged with the threshold it met. A table that cannot be read is an error
in that check, not a stop: the others are still measured, and the schedule
still maintains it.
