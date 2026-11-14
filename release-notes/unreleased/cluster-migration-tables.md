# Release Notes: Cluster tables for indexes and background migrations

## New Feature

### What Changed
Two new cluster tables show the indexes and statistics of each partition
replica, and the background migrations that build them:

- `restate.cluster.indexes`: one row per index or statistic on each replica,
  with its definition, its status (`backfilling`, `ready` or `dropped`), when it
  was registered, became ready or was dropped, and why it was dropped.
- `restate.cluster.migration_jobs`: one row per background migration on each
  replica, with its status (`pending`, `running`, `waiting`, `completed` or
  `cancelled`), its progress and an estimate of when the current pass ends
  (`eta`), the rows it examined and filled, its work time, and its failures
  with the last error. Finished migrations stay listed up to
  `worker.storage.migration-job-history-limit`.

Each worker node answers for the partition replicas it has open, so two
replicas of one partition can show different progress.

### Why This Matters
You can check whether an index or statistic is usable on every replica, follow
a backfill while it runs, and see why one is stuck.

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
FROM restate.cluster.indexes
WHERE status <> 'ready';
```

Backfills in progress, and whether they fail:

```sql
SELECT partition_id, plain_node_id, name, status, progress_percent, eta,
       failures, last_error
FROM restate.cluster.migration_jobs
WHERE status IN ('pending', 'running', 'waiting');
```
