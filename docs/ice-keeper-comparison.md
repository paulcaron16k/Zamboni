# Zamboni vs ice-keeper

A comparison against a maintenance service that is already deployed. This is
[roadmap.md RM-6](roadmap.md); the engine comparison it sits beside is
[engine-comparison.md](engine-comparison.md).

**Method.** Read from a checkout of [CybercentreCanada/ice-keeper](https://github.com/CybercentreCanada/ice-keeper)
(MIT) on 2026-08-03, at commit
`cd31a4d`. Statements about its behaviour come from its source, and the file is
named where it matters.

**It found a defect in Zamboni.** §4 — ice-keeper refuses to run orphan removal on
a table whose location is shared with another table, and Zamboni does not. That
is a data-loss path in shipped `v0.1.0`, raised as ZMBNI-507 and **since fixed** —
reproduced end to end first, where it destroyed the colocated table outright.

---

## 1. What it is

`ice-keeper` is a service, not a command: 8 399 lines of Python, scheduled nightly
in Airflow, maintaining a fleet of tables read from a maintenance-schedule table.
It runs PyIceberg 0.10.x for metadata work and PySpark over py4j for the heavy
operations, and in one case calls Iceberg's **Java** API directly through the
Spark JVM.

Its actions are expiry, fast expiry, orphan removal, rewrite/optimization,
partition widening, data lifecycle, and config auditing.

**The difference in kind:** Zamboni is a tool you invoke against one table.
ice-keeper is a scheduler, a work queue, a journal, and a set of diagnostics that
decide *which* tables and partitions need work at all. Most of what follows
follows from that.

---

## 2. Architecture, and what transfers

Its extension point is `ActionStrategy` (`ice_keeper/task/action/action.py`):

```python
@abstractmethod
def prepare_statement_to_execute(self) -> str: ...
@abstractmethod
def execute_statement(self, sub_executor, sql_stm) -> dict[str, Any]: ...
```

**Every action produces a SQL string**, rendered from a Jinja template, which
Spark executes. Coherent, because both of its backends are SQL engines.

**This is the closest prior art to ZMBNI-12, and it is a useful negative result.**
The seam cannot be ours: Zamboni manipulates Iceberg metadata through PyIceberg
and has no SQL to emit. That the most similar project in the space chose a seam
unavailable to us is worth knowing *before* designing the interface rather than
after — it is the reason ZMBNI-12's seam is operation-level (engine-comparison.md
§5).

What does transfer is smaller and still worth having:

- **`check_should_execute_action()` as a first-class step.** Each strategy decides
  whether there is anything to do before anything is prepared. Zamboni's verbs
  each answer this in their own way; a common pre-check is where the "nothing to
  do" path belongs.
- **Journaling around the action, not inside it.** `ActionTask.execute` writes a
  journal entry with start/end/status/duration and catches `ActionWarning`
  separately from `Exception`, so "ran, but you should look" is a distinct
  outcome from "failed". Zamboni has exit code 4 for one case of this and nothing
  for the rest.

---

## 3. Capability comparison

### Theirs, that Zamboni does not have

| Capability | What it does | Verdict |
|---|---|---|
| **Storage inventory as the file list** | `StorageInventoryReport` reads a cloud-provider inventory report *as a table* and builds a `file_list_view`, which it passes to Spark's `remove_orphan_files` in place of a directory listing | **Adopt the idea** — see §5 |
| **Maintenance schedule** | A table of which tables to maintain with what settings; `MaintenanceScheduleEntry` carries per-table retention and thresholds, refreshed per run | Adapt, if Zamboni becomes a service |
| **Journal** | Per-table, per-action history: SQL executed, status, timings, counts | Adapt |
| **Partition health diagnosis** | `PartitionDiagnosis` picks partitions to optimize, grouping sub-partitions by cumulative size into buckets (`optimization_grouping_size_bytes`) or at fixed depth | Adopt selectively — Zamboni's `min_input_files` is a cruder version of the same question |
| **Skip recently-unmodified tables** | `_is_table_recently_modified()` checks `metadata_log_entries` for a change within 5 days and skips the table if not | **Adopt** — cheap, and Zamboni currently lists storage for a table nothing has written to |
| **Location-uniqueness check** | Refuses orphan removal when the table's location is shared | **Adopt — this is a defect fix**, §4 |
| **Widening preconditions** | `WideningRule` carries `required_fixed_columns` (must be non-NULL before widening), a `filter_expr`, and `partition_depth_required` | Consider — Zamboni's evolution has no data-integrity precondition |
| **Data lifecycle** | Deletes data older than `lifecycle_max_days` by an ingestion-time column | **Decline** — deleting business data is a different product from maintaining a table's physical layout |
| **Notification** | Email on outcome | Decline — belongs to whatever schedules it |
| **Config auditing** | Checks table configuration against expectations | Consider, alongside `validate-config` |

### Ours, that ice-keeper does not have

| Capability | Why it matters |
|---|---|
| **Preview on every mutating verb** | ice-keeper is a scheduled service; it runs or it does not. Zamboni's rule that nothing commits without `--yes` has no counterpart |
| **Structural capability probes** | `capabilities.py` refuses a PyIceberg build that would corrupt a table. ice-keeper pins `>=0.10.0,<0.11.0` and trusts the pin |
| **Format-version handling** | V1 refused, V3 metadata-only with row lineage as the stated reason. No equivalent found |
| **`max-ref-age-ms`** | Zamboni implements the spec's retention step 2; ice-keeper's expiry does not |
| **Declarative config with rejection** | `table-config.json` rejects unknown keys and refuses contradictions at load; ice-keeper's settings live in a schedule table |
| **No JVM** | ice-keeper needs Spark for its main operations. Zamboni runs against PyIceberg and DuckDB alone, which is the reason it exists |
| **Multi-key Z-order without Spark** | Morton encoding as DuckDB SQL. ice-keeper has `zorder_udf.py` but reaches Spark to use it |

---

## 4. The defect this found

`orphans.py` in ice-keeper:

```python
MaintenanceSchedule(Scope()).check_table_location_is_unique(
    self.mnt_props.full_name, self.mnt_props.table_location)
```

Raised **before** the orphan-removal procedure runs: if another known table
shares this table's location, refuse.

Zamboni's equivalent is `storage_roots()`, whose docstring says files belonging to
a sibling table "must be out of reach **by construction**". The construction is
scoping the sweep to this table's own location — which prevents a warehouse-wide
sweep, and does **not** prevent the case where another table's files are *inside*
that location. Then those files are listed, found unreferenced by this table, and
deleted.

Reachable configurations: a table created with an explicit `location` under a
shared prefix; a second table whose `write.data.path` points inside the first;
two tables pointed at one location after a `register_table` mistake.

The claim "by construction" is the specific thing that is wrong — the code is
protecting against a different failure than the comment says it is. Raised as **ZMBNI-507** and fixed: `colocated_tables()` enumerates the catalog and
refuses when any other table's location overlaps ours, before the listing runs.
It is now the fourth checked invariant in design.md §6.6.

Reproduced before fixing, with four ordinary catalog calls and no explicit
`location=` argument: create `db.orders`, append twice, rename it to
`db.orders_v2`, create `db.orders` again. Maintaining the new table deleted all
nine files of the renamed one, including its current metadata, and it could no
longer be read.

---

## 5. Storage inventory, and the subtlety in it

The capability worth taking. Zamboni's orphan removal needs a bucket listing, and
against a remote-signing Lakekeeper warehouse it cannot get one — `ListObjectsV2`
is refused, so the operation cannot run at all (live-verification.md). An
inventory report sidesteps the listing entirely.

The subtlety is in `_init_older_than()`:

```python
age_of_inventory_report = 1 + abs((current_date - inventory_date).days)
n_days = age_of_inventory_report + retention_days_orphan_files
older_than = current_date - timedelta(days=n_days)
```

**The age guard is widened by the age of the report itself.** A cloud inventory is
generated periodically, so it is stale by construction: a file created after it was
written does not appear in it, and cannot be distinguished from one that was never
there. Adding the report's age to the retention window is what keeps the guard's
meaning intact — "older than the longest write" has to be measured from the file
list's clock, not today's.

It also falls back to a full directory listing when the inventory returns nothing,
rather than treating an empty inventory as "no files exist" — which would have
made every file an orphan. That is the same failure Zamboni's completeness
invariant exists to catch, handled by a different mechanism.

Any Zamboni adoption of this must bring both behaviours. The listing bypass alone,
without the widened guard, is a data-loss feature.

---

## 6. Recommendations

**Adopt (defect and cheap wins):**

1. **ZMBNI-507** — location-uniqueness check before orphan removal. Not optional;
   this is a data-loss path in shipped code.
2. **Skip tables not recently modified** — a `metadata_log_entries` check before
   listing storage. Cheap, and the saving is the expensive operation.

**Adopt the idea, on evidence:**

3. **Inventory-backed file listing**, with the widened age guard and the
   fallback-to-listing behaviour. This is the only known answer to orphan removal
   on a remote-signing warehouse, which is currently a hard stop. Spark exposes the
   same idea as `file_list_view` (engine-comparison.md §3 row 9), so it is a
   general mechanism rather than an ice-keeper invention.

**Consider, not now:**

4. Partition-health diagnosis as a better selector than `min_input_files`.
5. Widening preconditions — a non-NULL requirement before partition evolution.
6. Journal and schedule — but only as part of deciding open question 4 below.

**Decline:**

7. The SQL-statement seam (§2), data lifecycle deletion, and notification.

**And the question this does not answer:** roadmap.md's open question 4 asks
whether the schedule/journal layer is in scope at all. This comparison sharpens it
rather than settling it — ice-keeper is a *service* and Zamboni is a *tool*, and
most of the capability gap in §3 is that difference rather than a set of missing
features. That is a product decision, and it should be made deliberately rather
than by accreting one adopted capability at a time.
