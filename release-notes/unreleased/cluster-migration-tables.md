# Release Notes: Cluster tables for indexes and background migrations

## New Feature

### What Changed
Two new cluster tables show the indexes and statistics of each partition
replica, the one-time data migrations it runs in the background, and the
background jobs doing that work:

- `restate.cluster.registry`: one row per index or statistic on each replica,
  with its definition, its status (`backfilling`, `complete` or `dropped`), when
  it was registered, completed (`completed_at`) or was dropped, why it was
  dropped, and an estimate of the storage it takes, including writes not yet
  flushed (`approx_size_bytes`). One-time migrations are listed too, with kind
  `migration`, status `in_progress` or `complete`, when they completed, and
  when a rollback last set them back to in progress so that they run again
  (`reset_at`). Each open replica that has had indexes enabled also has one row
  of kind `registry` (status `present`), even before it has any index, and keeps
  it after indexes are disabled again.
- `restate.cluster.migration_jobs`: one row per background job on each
  replica, building an index (`index_backfill`) or running a one-time
  migration (`migration`), with its status (`pending`, `running`, `waiting`,
  `completed` or `cancelled`), its progress and an estimate of when the current
  pass ends (`eta`), the rows it examined and changed, its work time, and its
  failures with the last error.
  `blocked_by` gives the reason the replica last saw for it not to progress:
  `unknown-migration` (the node runs a release that doesn't know the
  migration, for example after a rollback; a release that knows it runs it),
  `background-migrations-disabled`, `snapshot-export`, `failure-backoff` or
  `throttled`. A snapshot export shows from the moment the replica next checks
  for one, and NULL only means no reason is known, not that the job is
  progressing. Finished jobs stay listed for
  `worker.storage.finished-migration-jobs-retention` (24 hours by default)
  after they finished.

Each worker node answers for the partition replicas it has open, so two
replicas of one partition can show different progress.

### Why This Matters
You can check whether an index or statistic is usable on every replica, follow
a backfill or a migration while it runs, and see why one is stuck.

### Impact on Users
- New tables only; existing queries are unchanged.
- If a node can't read the records of one of its replicas, it still returns
  its other replicas' rows. A query that reads the table to the end also gets a
  warning naming that replica; one that stops early, for example at a `LIMIT`,
  may not.
- The two tables are read separately, so a query joining them may see a
  replica at two slightly different moments.

### Examples
Indexes not yet usable everywhere:

```sql
SELECT partition_id, plain_node_id, definition, status
FROM restate.cluster.registry
WHERE status = 'backfilling';
```

The largest indexes. A dropped index may still show bytes until compaction
removes its data:

```sql
SELECT partition_id, plain_node_id, definition, status, approx_size_bytes
FROM restate.cluster.registry
ORDER BY approx_size_bytes DESC NULLS LAST
LIMIT 10;
```

Migrations not yet complete everywhere:

```sql
SELECT partition_id, plain_node_id, definition, status
FROM restate.cluster.registry
WHERE kind = 'migration' AND status <> 'complete';
```

Backfills and migrations in progress, and whether they fail:

```sql
SELECT partition_id, plain_node_id, name, status, progress_percent, eta,
       blocked_by, failures, last_error
FROM restate.cluster.migration_jobs
WHERE status IN ('pending', 'running', 'waiting');
```
