# Zamboni — Delivery Plan

**What was built, what proves it, and what is deliberately left out.**

Design rationale lives in [design.md](design.md); this document is the delivery record.
Day-to-day operation is [runbook.md](runbook.md). Outstanding work is tracked in [tasks.md](tasks.md).

| | |
|---|---|
| Status | All planned operations implemented. 327 tests passing. Verified against a live Lakekeeper 0.13.1 + MinIO — [live-verification.md](live-verification.md) |
| In scope | Data-file compaction, layout ordering, partition evolution, dangling-delete removal, manifest rewriting, snapshot expiry, orphan-file removal, metadata retention |
| Out of scope | Rewriting a *partially* dangling delete manifest; splitting one partition across manifests; format-version 3 row rewriting |
| Demo | [../data/healthims](../data/healthims) — five days of simulated hospital discharge ingest |

---

## 1. Phasing

Each phase landed with its tests before the next began. Nothing destructive was written
before the referenced-file set it depends on was proven complete.

| Phase | Delivered |
|---|---|
| 1 | Compaction: profile → plan → rewrite → `replace` commit; bounded memory |
| 2 | Ordering: declared sort, multi-key Z-order, honest `sort_order_id` |
| 3 | Partition evolution: days → months without moving where new data lands |
| 4 | Declarative config: `table-config.json`, Meltano `x-iceberg` import |
| 5 | Reclamation: `reachable` set, snapshot expiry, orphan-file removal |
| 6 | Dangling-delete removal, manifest rewriting, metadata retention, V3 blocker |
| 7 | Live verification against Lakekeeper + MinIO |
| 8 | Dev stack (Lakekeeper + Postgres + MinIO, STS-vending), its tests, and the demo running on it |
| 9 | CI: lint, the suite on 3.11 and 3.13, the PEP 723 executables, and the dev stack end to end |

---

## 2. Delivered outcomes

- **Small-file compaction** for copy-on-write and merge-on-read (V2 position delete) tables.
- **Query-shaped layout**: multi-key Z-order — built here, because nothing in the stack has
  it — declared sort, and `sort_order_id` stamped only when the output genuinely satisfies
  the table's declared order.
- **Days-to-months partition evolution**, on by default at 90 days, without changing where
  new data lands.
- **Manifest rewriting**: regroup entries by partition so predicates prune at manifest
  level, preserving sequence numbers exactly.
- **Dangling-delete removal**: compaction leaves every position delete file referenced
  forever, because `_OverwriteFiles._deleted_entries` filters to `DataFileContent.DATA`.
  Applicability is delegated to PyIceberg's own `DeleteFileIndex`, so a file called dangling
  is one no scan would have applied.
- **Storage reclamation**: snapshot expiry against the spec's retention algorithm, and
  orphan-file removal fenced by list-before-reachable ordering, an age guard, and
  abort-on-doubt invariants.
- **Metadata retention**: the two `write.metadata.*` properties, applied deliberately, with
  the measured consequence of each documented.
- **Bounded memory**: peak ≈ one output file, independent of table size.

---

## 3. Functional requirements

Each requirement names the test that demonstrates it. `tests/test_docs.py` fails if a cited
test stops existing, so this table cannot rot silently.

### FR-1 — Compaction

| ID | Requirement | Verified by |
|---|---|---|
| FR-1.1 | Rewriting preserves every row | `test_unpartitioned_compaction_preserves_data` |
| FR-1.2 | Commit as an Iceberg `replace` snapshot | `test_commit_uses_replace_operation` |
| FR-1.3 | Group and commit per `(spec_id, partition)`; never mix partitions in one output file | `test_partitioned_compaction_is_partition_scoped` |
| FR-1.4 | Bucket-transform partitions are rewritten correctly | `test_bucket_partitioned_table_compacts` |
| FR-1.5 | Partitions below the size or count threshold are left alone | `test_min_input_files_skips_small_partitions`, `test_files_at_target_size_are_left_alone` |
| FR-1.6 | Target size resolves config → table property → default | `test_target_size_resolution_order`, `test_explicit_config_beats_table_properties` |
| FR-1.7 | An empty table is a no-op, not an error | `test_empty_table_is_a_no_op` |
| FR-1.8 | A multi-partition run commits once; a failure leaves the table untouched | `test_a_multi_partition_rewrite_commits_once_by_default`, `test_an_atomic_run_that_fails_leaves_the_table_untouched`, `test_a_failed_atomic_run_leaves_no_referenced_file_missing` |
| FR-1.9 | `partial_progress` commits per group and keeps what already landed | `test_partial_progress_commits_each_group`, `test_partial_progress_keeps_what_already_committed` |
| FR-1.10 | One atomic commit records every evolved group, and honours `snapshot_operation` | `test_an_atomic_commit_records_every_evolved_group`, `test_evolution_honours_the_overwrite_escape_hatch` |

### FR-2 — Merge-on-read

| ID | Requirement | Verified by |
|---|---|---|
| FR-2.1 | Live rows are preserved, not physical rows — a delete file makes them differ | `test_compaction_preserves_live_rows_not_physical_rows`, `test_fixture_really_is_merge_on_read` |
| FR-2.2 | Deleted rows never reappear | `test_deleted_rows_do_not_come_back`, `test_copy_on_write_delete_then_compact_preserves_survivors` |
| FR-2.3 | Delete files left referenced after compaction are reported | `test_delete_files_are_reported_as_dangling` |
| FR-2.4 | A block policy can refuse merge-on-read tables outright | `test_block_policy_refuses_a_mor_table` |

### FR-3 — Ordering

| ID | Requirement | Verified by |
|---|---|---|
| FR-3.1 | Z-order matches a reference Morton encoding | `test_matches_the_reference_morton_encoding`, `test_three_keys_interleave` |
| FR-3.2 | Z-order clusters multiple keys better than a single-key sort | `test_clusters_better_than_a_single_key_sort` |
| FR-3.3 | Ordering never loses or duplicates a row | `test_preserves_every_row` |
| FR-3.4 | Non-numeric and single-valued keys are handled | `test_handles_non_numeric_keys`, `test_single_valued_column_does_not_break_the_scale` |
| FR-3.5 | An over-budget or single-column z-order is rejected at load | `test_rejects_an_over_budget_key`, `test_rejects_a_single_column`, `test_zorder_bit_budget_is_enforced` |
| FR-3.6 | `sort_order_id` is stamped only when output satisfies the declared order | `test_sort_expression_orders_output_but_never_stamps_sort_order`, `test_sort_by_table_order_applies_and_stamps_the_declared_order`, `test_no_sort_expression_leaves_sort_order_unset` |
| FR-3.7 | Sorting by table order on an unsorted table is refused | `test_sort_by_table_order_refuses_an_unsorted_table`, `test_sort_options_are_mutually_exclusive` |

### FR-4 — Partition evolution

| ID | Requirement | Verified by |
|---|---|---|
| FR-4.1 | Only partitions past the retention window are evolved | `test_plan_selects_only_aged_partitions`, `test_retention_window_protects_recent_data` |
| FR-4.2 | Days condense into months | `test_evolution_condenses_days_into_a_month` |
| FR-4.3 | New data keeps landing in the fine-grained spec | `test_new_data_still_lands_in_the_daily_spec` |
| FR-4.4 | Every manifest's spec matches the files inside it | `test_manifest_spec_matches_file_spec`, `test_snapshot_summary_labels_use_each_file_s_own_spec` |
| FR-4.5 | Partition values survive the rewrite | `test_partition_values_survive_a_round_trip` |
| FR-4.6 | A new partition field takes a fresh field id | `test_new_partition_field_gets_a_fresh_id` |
| FR-4.7 | Evolution can be disabled fleet-wide or per table | `test_disabled_evolution_plans_nothing`, `test_evolution_can_be_disabled_fleet_wide`, `test_a_table_can_opt_out_of_evolution` |
| FR-4.8 | A compound spec evolves with one new spec, the aged field coarsened and the rest carried through | `test_a_compound_spec_needs_one_new_spec_not_one_per_combination`, `test_the_carried_field_keeps_its_field_id_and_the_aged_one_gets_a_fresh_one`, `test_a_compound_spec_evolves_end_to_end` |
| FR-4.9 | Grouping keys on the whole output partition, not just the coarse window | `test_files_are_grouped_by_the_whole_output_partition` |
| FR-4.10 | Two fields of the rule's granularity are refused, naming them | `test_two_fields_of_the_same_granularity_are_still_refused` |

### FR-5 — Configuration

| ID | Requirement | Verified by |
|---|---|---|
| FR-5.1 | Unknown keys are rejected, not ignored — a typo must not silently unpartition a table | `test_typos_are_rejected_not_ignored` |
| FR-5.2 | The document round-trips through JSON | `test_round_trips_through_json`, `test_retention_parses_and_round_trips` |
| FR-5.3 | Contradictions are refused at load | `test_rejects_contradictory_blocks`, `test_evolution_must_go_coarser`, `test_one_evolution_rule_per_source_granularity` |
| FR-5.4 | A rule must name a partition field the table actually declares | `test_evolution_rule_must_match_a_declared_partition_field` |
| FR-5.5 | Table keys are namespace-qualified; defaults apply to unknown tables | `test_table_keys_must_be_qualified`, `test_unknown_table_falls_back_to_defaults` |
| FR-5.6 | The shipped example validates | `test_shipped_example_is_valid` |
| FR-5.7 | `x-iceberg` blocks are imported from stream metadata or the schema root | `test_imports_from_stream_metadata`, `test_imports_from_the_schema_root` |
| FR-5.8 | Streams without a block are reported, not silently skipped | `test_streams_without_the_block_are_reported_not_swallowed` |
| FR-5.9 | An unresolvable identifier or invalid block fails loudly | `test_unresolvable_identifier_is_skipped_not_guessed`, `test_invalid_block_fails_loudly` |
| FR-5.10 | Generated config is valid and carries the evolution default | `test_generated_config_is_valid_and_round_trips`, `test_generated_config_carries_the_evolution_default` |

### FR-6 — Operations

| ID | Requirement | Verified by |
|---|---|---|
| FR-6.1 | `describe` and `plan` change nothing | `test_describe_is_read_only`, `test_plan_is_read_only` |
| FR-6.2 | No mutating verb commits without `--yes`, and each says it is previewing | `test_no_verb_mutates_without_yes`, `test_compact_previews_without_consent`, `test_compact_dry_run_changes_nothing`, `test_every_mutating_verb_says_what_omitting_yes_does` |
| FR-6.3 | Bounded memory: peak ≈ one output file | `test_chunked_mode_splits_output_by_target_size`, `test_chunked_mode_with_sort_routes_through_duckdb` |
| FR-6.4 | Refuse to run on a PyIceberg build that would corrupt a table | `test_pruning_without_derivation_is_refused`, `test_missing_operation_argument_is_refused` |
| FR-6.5 | Non-zero exit on a blocked table | `test_blocked_table_exits_nonzero` |
| FR-6.6 | A concurrent write is refused, not lost | `test_concurrent_write_is_refused_not_lost` |
| FR-6.7 | A row-count mismatch aborts before committing | `test_row_count_mismatch_aborts_before_commit` |
| FR-6.8 | Format version 1 is blocked | `test_format_version_1_is_blocked` |
| FR-6.9 | Capabilities are probed, not inferred from a version | `test_probes_the_installed_build`, `test_equality_delete_blocker_follows_the_capability` |

### FR-7 — Reclaiming storage

| ID | Requirement | Verified by |
|---|---|---|
| FR-7.1 | The referenced-file set covers every category Iceberg metadata points at | `test_the_categories_that_apply_are_all_populated`, `test_every_referenced_file_exists_on_disk` |
| FR-7.2 | Retention follows the spec: heads kept, `min-snapshots-to-keep` beats age, `main` never expires | `test_min_snapshots_to_keep_beats_age`, `test_tag_and_branch_heads_are_retained`, `test_the_head_always_survives_however_old` |
| FR-7.3 | Expiry deletes only files the expired snapshots alone referenced | `test_expiry_deletes_only_what_it_orphans`, `test_expiry_never_deletes_a_file_a_surviving_snapshot_needs` |
| FR-7.4 | Time travel to a surviving snapshot still reads | `test_time_travel_still_works_for_surviving_snapshots` |
| FR-7.5 | Orphan removal lists storage **before** computing reachability | `test_live_data_is_never_touched` |
| FR-7.6 | An unreferenced file younger than the age guard is left alone | `test_a_recent_unreferenced_file_survives_the_guard` |
| FR-7.7 | A missing referenced file or an empty reference category **aborts** and deletes nothing | `test_a_missing_referenced_file_aborts`, `test_an_empty_reachable_category_aborts` |
| FR-7.8 | Orphan removal without expiry is refused at config load | `test_orphan_removal_without_expiry_is_rejected` |
| FR-7.9 | `expire` and `remove-orphans` are dry-run without `--yes` | `test_expire_dry_run_changes_nothing`, `test_dry_run_deletes_nothing` |
| FR-7.10 | A live merge-on-read delete file survives the sweep | `test_reclaim_now_keeps_live_delete_files` |
| FR-7.11 | Both reclaim verbs report a failed safety check as exit 4 | `test_expire_aborts_with_the_same_exit_code_as_orphans`, `test_remove_orphans_aborts_with_a_distinct_exit_code` |
| FR-7.12 | The listing and the referenced set are keyed identically on object storage | `test_canonical_is_not_idempotent_for_object_keys`, `test_listing_keys_match_reachable_keys` |
| FR-7.13 | Listing works on whichever FileIO the deployment forces | `test_fsspec_listing_uses_the_bucket_qualified_prefix_once`, `test_fsspec_entries_carry_a_deletable_location`, `test_an_io_that_can_neither_list_nor_be_understood_aborts` |
| FR-7.14 | A bad age guard is a usage error, not a traceback | `test_a_negative_age_guard_is_a_usage_error_not_a_crash` |
| FR-7.15 | A ref past `max-ref-age-ms` is dropped, so its snapshots stop being pinned | `test_a_stale_ref_is_dropped_and_stops_pinning_its_snapshots`, `test_a_stale_tag_is_dropped_and_its_snapshot_expires`, `test_a_stale_branch_is_dropped_too` |
| FR-7.16 | A ref's own `max-ref-age-ms` beats the table property, including when the table sets none | `test_a_refs_own_max_ref_age_beats_the_table_policy`, `test_a_refs_own_max_ref_age_applies_with_no_table_property` |
| FR-7.17 | No ref is dropped unless the age is configured, and `main` never is | `test_an_unconfigured_max_ref_age_never_drops_a_ref`, `test_an_unconfigured_policy_leaves_every_ref_alone`, `test_main_survives_however_stale_the_policy`, `test_dry_run_drops_no_refs` |
| FR-7.18 | Orphan removal refuses when another table shares this table's location | `test_maintaining_one_table_refuses_to_delete_a_colocated_table`, `test_the_colocation_guard_names_what_to_do`, `test_rename_then_recreate_really_does_collide`, `test_a_table_alone_in_its_location_is_unaffected` |

### FR-8 — Dangling deletes and manifests

| ID | Requirement | Verified by |
|---|---|---|
| FR-8.1 | A delete file that still applies is never removed | `test_a_delete_that_still_applies_is_never_removed` |
| FR-8.2 | Compaction makes its delete files dangling, and they are then dropped | `test_compaction_makes_the_delete_dangling`, `test_removal_drops_the_delete_file_from_the_table` |
| FR-8.3 | The removal is recorded in the snapshot summary, not silent | `test_removal_is_recorded_in_the_snapshot_summary` |
| FR-8.4 | A partially dangling delete manifest is retained, never rewritten | `test_a_partially_dangling_manifest_is_retained_not_rewritten`, `test_the_producer_refuses_to_rewrite_a_delete_manifest` |
| FR-8.5 | Simulated merge-on-read tables use real delete manifests | `test_delete_files_land_in_a_delete_manifest` |
| FR-8.6 | Manifest rewriting preserves sequence numbers and entry status | `test_sequence_numbers_survive_the_rewrite`, `test_entries_are_written_as_existing` |
| FR-8.7 | It adds and removes no files, and improves partition spread | `test_the_snapshot_says_replace_and_moves_no_files`, `test_partition_spread_improves` |
| FR-8.8 | Partitions are packed in value order, not name order | `test_partitions_are_packed_in_value_order_not_name_order` |
| FR-8.9 | Deletes still apply afterwards | `test_deletes_still_apply_after_a_rewrite` |
| FR-8.10 | A pure clustering improvement is not skipped as "no reduction" | `test_clustering_alone_is_worth_doing`, `test_an_already_clustered_table_is_skipped` |

### FR-9 — Metadata retention and format versions

| ID | Requirement | Verified by |
|---|---|---|
| FR-9.1 | Trimming the metadata log deletes nothing; orphan removal reclaims it | `test_trimming_the_log_deletes_nothing`, `test_orphan_removal_reclaims_the_stranded_files` |
| FR-9.2 | An undeclared property is left alone, not reset to its default | `test_unset_settings_touch_nothing` |
| FR-9.3 | V3 is blocked for row rewriting, with row lineage as the stated reason | `test_v3_is_blocked_for_row_rewriting`, `test_compaction_refuses_a_v3_table` |
| FR-9.4 | Metadata-only operations stay allowed on V3 | `test_metadata_only_operations_are_not_blocked_by_v3`, `test_manifest_rewriting_preserves_the_v3_row_lineage_field` |
| FR-9.5 | The upstream V3 guard is serialisation-only, so ours is needed | `test_the_upstream_block_is_serialisation_only`, `test_pyiceberg_cannot_write_v3_at_all` |

### FR-11 — The maintainer interface

[roadmap.md RM-2](roadmap.md), specified by [engine-comparison.md §6](engine-comparison.md).

| ID | Requirement | Verified by |
|---|---|---|
| FR-11.1 | Every engine declares support for every operation; an undeclared one is a construction error | `test_every_engine_declares_every_operation`, `test_capabilities_reject_a_missing_operation` |
| FR-11.2 | Support is three-valued, and a caveat that is not explained is refused | `test_a_partial_support_must_say_why`, `test_an_unsupported_operation_must_say_why`, `test_support_is_three_valued_in_practice_not_just_in_theory` |
| FR-11.3 | Previewability is per operation, not per engine | `test_preview_is_per_operation_not_per_engine`, `test_the_local_engine_previews_everything`, `test_trino_previews_nothing` |
| FR-11.4 | An engine that cannot preview refuses without `--yes` rather than running or claiming a dry run | `test_an_engine_that_cannot_preview_refuses_rather_than_running`, `test_an_engine_that_cannot_preview_refuses_without_yes`, `test_consent_makes_a_non_previewing_engine_runnable` |
| FR-11.5 | An unsupported operation is refused, with exit 3 | `test_trino_cannot_remove_dangling_deletes_at_all`, `test_an_unsupported_operation_exits_three` |
| FR-11.6 | Config is validated per engine at plan time, not passed through | `test_trino_rejects_our_default_retention_at_plan_time`, `test_trino_rejects_a_short_orphan_guard`, `test_a_config_below_trinos_floor_is_a_usage_error`, `test_the_local_engine_imposes_no_floors` |
| FR-11.7 | An operation may be fulfilled by another operation | `test_spark_fulfils_dangling_deletes_through_compaction`, `test_describe_says_when_an_operation_rides_on_another` |
| FR-11.8 | The declarations match the analysis they came from | `test_declarations_match_the_engine_comparison`, `test_describe_reports_limitations_so_they_are_discoverable` |
| FR-11.9 | The default engine is local, and every mutating verb accepts `--engine` | `test_the_default_engine_is_local`, `test_every_mutating_verb_accepts_an_engine`, `test_engines_reports_what_each_one_refuses` |

### FR-10 — Versioning and releases

The contract is in [releasing.md](releasing.md). Its substance is FR-10.5: for a
tool that deletes files, a changed default is a breaking change with no signature
moved, so the destructive defaults are named as public surface rather than left
implicit.

| ID | Requirement | Verified by |
|---|---|---|
| FR-10.1 | The version is declared once, in `pyproject.toml`, and derived everywhere else | `test_the_package_reports_the_declared_version`, `test_the_declared_version_is_semver` |
| FR-10.2 | `--version` reports zamboni, PyIceberg and Python, since one alone does not identify behaviour | `test_the_version_banner_names_all_three_versions`, `test_both_entry_points_report_the_version_and_exit_zero` |
| FR-10.3 | A changelog with somewhere to record the next change, and dated release sections | `test_the_changelog_has_somewhere_to_record_the_next_change`, `test_every_released_changelog_section_carries_a_date`, `test_the_changelog_checks_reject_a_malformed_changelog` |
| FR-10.4 | A release cannot ship with the changelog ahead of the declared version | `test_the_declared_version_is_not_behind_the_changelog` |
| FR-10.5 | The contract states what counts as breaking, including destructive defaults | `test_the_release_convention_is_documented` |

---

## 4. How it is verified

Three layers, because each catches what the others cannot.

**Unit and integration** — 314 of the 327 tests, against a SQL catalog over a temporary
directory. Fast, hermetic, and where every logic branch is exercised. Blind to anything about object storage.

**Safety by omission** — the tests that matter most assert the tool **refuses**.
`tests/test_orphans.py` monkeypatches each reference category away in turn and asserts
nothing is deleted. Without these, enabling orphan removal by default would be unjustified.

**Static** — mypy over `src` and `scripts`, in the `lint` job and pre-commit. Deliberately
not `strict`: this package drives private PyIceberg internals on purpose, and a strict run is
dominated by untyped-call noise from exactly those. The rules enabled are the ones that catch
what this codebase actually gets wrong — `None` reaching an attribute access, and a value
used as a type it is not. That is not a guess: reintroducing the `FileIO has no
_initialize_fs` bug from [live-verification.md](live-verification.md) is caught statically,
and two of the four bugs found there were of that shape.

**Automated** — [.github/workflows/ci.yml](../.github/workflows/ci.yml) runs all of the
above plus the dev stack on every push and pull request. Two details are load-bearing:
`ZAMBONI_REQUIRE_DEV_STACK=1` makes an unreachable stack a failure rather than a skip, and
the `executables` job runs `bin/zamboni` from outside the project directory, which is the
only way the two path bugs it guards were ever visible.

**Live** — [live-verification.md](live-verification.md) runs every operation against a real
Lakekeeper 0.13.1 and MinIO. This is not ceremony: it found four bugs the local suite could
not, all at the boundary between how metadata records a location and how a listing returns
one. Two of those would have made orphan removal silently reclaim nothing; the one that
could have deleted live data was stopped by the completeness invariant above.

The demo is the fourth check, and the one a person can read: five days of ingest, then
maintenance, with row-for-row comparison before and after.

---

## 5. Residual risk

**Enabled-by-default deletion is the most dangerous thing this codebase does.** Orphan
removal decides what to delete by subtracting a computed set from a directory listing. The
invariants in [design.md §6.6](design.md#66-safety-invariants-for-deletion) are what make
that defensible, and the per-category omission tests are what keep them honest. The live run demonstrated the argument holds: a real keying
bug produced an abort, not data loss.

Known and accepted:

| Risk | Mitigation |
|---|---|
| A warehouse without `sts-enabled` silently cannot reclaim storage | `dev-stack/bootstrap.py` warns; `tests/test_dev_stack.py` asserts the profile setting, the vended session token, and a real LIST and DELETE |
| A partially dangling delete manifest cannot be split | Reported and retained. Lifts automatically if PyIceberg gains a delete-manifest writer; the capability is probed, not assumed |
| Compaction is blocked on format version 3 | Row lineage cannot be preserved through the scan-and-rewrite path. Metadata-only operations are unaffected |
| A remote-signing Lakekeeper warehouse permits no reclamation | Documented with the exact storage-profile settings that cause it; needs STS-vended or direct credentials |
| `scripts/verify-live.py --map-host` takes a bridge IP | Only needed against a warehouse advertising an in-cluster endpoint. The dev stack advertises the pinned compose gateway instead, so the flag is unnecessary there |
| The dev stack pins subnet `172.31.0.0/24` | Collides if that range is already used; change it in `docker-compose.yaml` and `S3_GATEWAY` together. `bootstrap.py` fails loudly if they disagree |

---

## 6. Not in scope

Out of scope **for `v0.1.0`, which is PyIceberg-only**. Two of these are limitations of that
choice rather than of the operations, and [roadmap.md](roadmap.md) plans an engine that does
not share them — Spark's `rewrite_position_delete_files` rewrites partially dangling delete
files (ZMBNI-1505). What follows is the scope of *this* implementation.

- **Rewriting a partially dangling delete manifest** — blocked by
  `ManifestWriterV2.content()` returning `ManifestContent.DATA` unconditionally, so PyIceberg
  cannot write a delete manifest at all. Tracked as ZMBNI-604. Still true on unreleased
  `main`: `content()` is unchanged and there is no `ManifestWriterV3`, so PyIceberg 0.12 does
  not lift this.
- **Splitting one partition across manifests** — *not* an upstream limitation but a
  deliberate choice, and an earlier draft of this section wrongly conflated the two. A
  manifest records lower/upper partition bounds, so splitting one partition across manifests
  is exactly what stops the manifest evaluator pruning. Cancelled as ZMBNI-605.
- **Format-version 3 row rewriting** — see FR-9.3.
- **Server-side scan planning** — the REST specification defines it optionally; Lakekeeper
  0.13.1 does not expose it, so all pruning stays client-side.
