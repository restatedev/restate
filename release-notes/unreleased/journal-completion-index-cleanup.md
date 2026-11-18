# Release Notes: Clean up leftover journal completion-index entries in the background

## Improvement

### What Changed
Releases before v1.7.0 deleted invocation journals without some of their index entries, the
journal completion-index entries. Those leftovers waste disk space and stay until something
removes them. Restate v1.7.0 to v1.7.8 removed them once at startup; v1.7.9 stopped doing so,
because that cleanup couldn't account for a rollback to v1.6.

Restate now removes them in the background, while the partition keeps serving. The cleanup runs
once per partition replica, continues where it left off after a restart, and only removes entries
whose journal no longer exists: entries of live journals are never touched.

### Impact on Users
- The cleanup only runs on partitions on which the experimental entry indexes
  (`experimental-enable-track-entry-indexes`) or status gauges
  (`experimental-enable-track-status-gauges`) have ever been enabled. Other partitions are
  unaffected and keep their leftover entries for now.
- Its progress shows in `restate.cluster.migration_jobs` (kind `migration`, name
  `jc_orphan_cleanup/1`), and `restate.cluster.registry` lists it as `complete` once done. A
  partition replica without any journal completion-index entries lists it as complete at once,
  with no job.
- The background migration options under `[worker.storage]` apply: it can be paused with
  `disable-background-migrations`, slowed with `background-migrations-throttling`, and its slices
  kept small with `background-migrations-max-slice-entries`.

### Migration Guidance
No action needed. To follow the cleanup:

```sql
SELECT partition_id, plain_node_id, status, progress_percent, rows_filled, last_error
FROM restate.cluster.migration_jobs
WHERE name = 'jc_orphan_cleanup/1';
```
