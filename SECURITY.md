# Security policy

Zamboni deletes files. That is not a side effect of what it does — snapshot
expiry, orphan-file removal and dangling-delete removal exist to reclaim
storage, and each of them decides that some object is no longer referenced and
then removes it.

So the failure mode of a defect here is somebody's data, and this policy treats
that as the primary category rather than an afterthought.

## What to report, in priority order

**1. Anything that could delete data that is still referenced.** This is the
category that matters most, and it does not need to be exploitable by an
attacker to be worth reporting. A logic error that makes a live file look
unreferenced is as serious as a crafted input that causes the same thing —
arguably more so, because it fires without anyone trying.

Concretely, tell us if you find a way to make Zamboni:

- delete a file a retained snapshot still references;
- compute a reachable-file set that omits a category (data, delete files,
  manifests, manifest lists, `metadata.json`, statistics, partition statistics);
- pass the age guard for a file that is still being written;
- remove orphans belonging to a *different* table that shares a location prefix;
- commit a snapshot that drops rows, or resurrects rows a delete file removed.

**2. Anything that bypasses a safety check.** The reclaim operations abort
rather than delete when their view of the world looks untrustworthy — a
referenced file missing from a storage listing, an empty reference category on a
table that has snapshots. A way to make one of those checks pass when it should
not is a security issue even if you cannot yet turn it into deletion.

**3. Credential handling.** Zamboni reads catalog tokens and, in some
configurations, S3 keys. Report anything that logs them, writes them to disk,
sends them somewhere unexpected, or widens the credentials a run needs.

**4. The usual.** Code execution, path traversal, dependency vulnerabilities we
are exposed to.

## What is not a vulnerability

- **An operation refusing to run.** Exit codes 3 and 4 are refusals: a blocked
  table, or a safety check that aborted. They are the design working.
- **Deleting files you configured it to delete.** An `older_than_days` set below
  your longest write will delete files that were still being written. That is
  documented in [docs/runbook-dev.md](docs/runbook-dev.md) as the most
  consequential number in the configuration, and it is a configuration error
  rather than a defect. Report it anyway if you think the default or the
  documentation is what led you there — that *is* worth fixing.
- **Behaviour of Trino or Spark.** With `--engine trino` or `--engine spark` the
  work happens server-side. Report engine defects to those projects; report to
  us if Zamboni asks them to do the wrong thing.

## How to report

**Please do not open a public issue for anything in category 1 or 2.**

Use GitHub's private vulnerability reporting on this repository
(*Security* → *Report a vulnerability*), which is the preferred channel because
it keeps the report and the fix in the same place.

If that is unavailable to you, email **paul@experienceflow.com** with `zamboni
security` in the subject.

Include, if you can: the version (`zamboni --version` reports Zamboni, PyIceberg
and Python — all three matter, because which operations are attempted is decided
by probing the installed PyIceberg), the catalog and storage in use, and the
smallest reproduction you have. A reproduction against a local SQL catalog is
worth more than a description of a production incident, and
`zamboni.testing` exists to help build one.

## What to expect

This project is currently maintained by one person, so response is best-effort
rather than contractual. Honestly stated:

| | |
|---|---|
| Acknowledgement | within a week |
| Assessment of category 1 or 2 | within two weeks |
| Fix or a plan | depends entirely on what it is |

If you have had no acknowledgement after a week, assume the message went astray
and try the other channel.

## Which versions get fixes

`0.x`, and only the latest. There is one released version at the time of
writing and no backport branch; a data-loss fix ships as a patch release, and
[docs/releasing.md](docs/releasing.md) commits to that even when the fix breaks
a working pipeline — such changes are marked `SAFETY` in the changelog and say
what changed and why.

Note that "latest" includes the PyIceberg you have installed. PyIceberg 0.11.x is
fully supported; Zamboni caps at `<0.12` while the 0.12 release candidates are
tested, currently for
[apache/iceberg-python#3758](https://github.com/apache/iceberg-python/issues/3758).
Removing the cap locally is not supported.

## Disclosure

Report privately, and we will agree a disclosure timing with you. The default is
that a fix ships first and the changelog entry names the problem plainly
afterwards — this project's changelog already carries a `SAFETY` category for
exactly this, and entries there say what was wrong rather than "hardening
improvements".

Credit is offered by default and withheld on request.
