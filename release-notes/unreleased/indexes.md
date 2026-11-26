# Release Notes: Indexes and statistics (experimental)

## New Feature

### What Changed

One experimental option, off by default, turns on indexes and statistics:

```toml
experimental-enable-indexes = true
```

With it, each partition maintains:

- **Entry indexes** of its invocations and state mutations:
  - all of them by the hour they were created in and by service, newest created first;
  - all of them in a known stage by stage and status, newest created first, and again by stage
    and status, newest by when they entered their stage.
- **Status gauges**: how many entries are in each stage and status, counted by service, by
  deployment and by virtual object. The status is the one `sys_vqueue_entry_status` shows, such
  as `ready` in the inbox or `running`. The counts include invocations and state mutations, and
  keep completed entries until they are deleted.

A partition starts building them once it has moved all its invocations to virtual queues (the
vqueues migration, which runs automatically since v1.8.0), right after that migration finishes.
From then on it indexes and counts what is written, and builds the indexes and counts for its
existing entries in the background (see the background migration options under
`[worker.storage]`). `restate.cluster.registry` shows each one's status on every replica:
`backfilling` while it is built, then `complete`. Until a partition's migration has finished, it
has none, and queries read it as before; statistics queries fail on it.

On the nodes that run queries, the same option makes queries use them:

- With query engine V2, a query on `sys_invocation_status` that asks for the newest or oldest
  invocations under a `LIMIT`, of every service or of the ones it names, like the UI's invocation
  list, reads only those invocations through an entry index instead of every invocation of every
  partition. On `sys_vqueue_entry_status`, a query for the newest or oldest entries of some stages
  under a `LIMIT` reads the stage-and-status indexes. Other queries read as before.
- Both query engines show the statistics tables `sys_service_stats`, `sys_deployment_stats` and
  `sys_virtual_object_stats`.

The option takes effect when partition processors start; on the query side, new query sessions see
the change.

### Why This Matters

Listing the newest invocations, or counting entries per stage and status, otherwise scans every
entry of every partition, which is too expensive for a busy cluster. Indexes and counters
maintained with every write make these queries cheap, and the background build keeps partitions
serving while they are built.

### Impact on Users

- Nothing changes unless the option is turned on.
- A partition reads through an index only once that index is complete there; until then, and
  while some worker node still runs an older version, queries read as before.
- A query of the statistics tables fails until the statistics are complete on every partition it
  reads. `sys_service_stats` and `sys_deployment_stats` fail before returning any row, so they
  never return a partial total. `sys_virtual_object_stats` returns each partition's rows as they
  are read, so a query can return some rows and then fail; check that `/version` reports
  `query_engine.stats_tables: true` before relying on it, as the Web UI does.
- With the option on, every invocation and state mutation also writes small index keys and counter
  updates.
- **Turning the option off drops everything** when each partition next starts: the indexes and
  counts are deleted (`drop_reason` `not-enabled` in `restate.cluster.registry`), and queries read
  as before. Turning it on again builds them from scratch. Invocation and entry data are never
  affected.
- **To stop queries from reading the indexes without dropping them**, for example if an index read
  misbehaves, set this on the nodes that run queries:

  ```toml
  [admin.query-engine]
  disable-index-reads = true
  ```

  Queries then read as before, while the indexes and counts stay built and up to date, so
  removing the setting uses them again right away. The statistics tables stay available. New
  query sessions see the change.
- **Turning the option on raises the partition's minimum Restate version to v1.8.2, for good:** a
  node that has run with it cannot be rolled back to an older version, also after the option is
  turned off.

### Migration Guidance

No action is needed. To try the indexes and statistics, first upgrade every node to v1.8.2 or
newer, then set the option on every node and restart the nodes. A node on an older version can't
open a partition store that ran with the option on, including one restored from a snapshot of such
a store. To remove them, remove the option and restart the nodes.
