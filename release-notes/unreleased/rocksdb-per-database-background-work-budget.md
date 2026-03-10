# Release Notes: Per-database RocksDB background work budgets

## Breaking Change

### What Changed

The `rocksdb-max-background-jobs` configuration option has been removed from the shared
`rocksdb` config block (which was applied to every database identically). Background work
is now controlled per-database with separate flush and compaction concurrency limits that
are automatically computed based on the node's CPU count and active roles.

Each database type now has its own `rocksdb-max-background-flushes` and
`rocksdb-max-background-compactions` options in its respective config section.

Additionally, a new `export-concurrency-limit` option has been added to the
`[worker.snapshots]` section to control the number of concurrent snapshot exports
(previously derived from `rocksdb-max-background-jobs`).

### Why This Matters

Previously, every database on a node (partition-store, log-server, metadata-server,
local-loglet) was independently allocated the full CPU count as its background job budget.
On a default all-roles node, this meant 4 databases collectively scheduling up to
`4 * CPU_COUNT` background jobs, all competing for the same shared RocksDB thread pool.
This could cause the partition-store (with many column families) to starve the log-server
of compaction resources, or vice versa.

The new approach splits background work into two dimensions:
- **Flushes** (latency-critical) are allocated equally across databases.
- **Compactions** (throughput-heavy) are weighted toward the partition-store (~65%)
  since it typically has many more column families generating compaction demand.

When a role is not active on the node, its budget is redistributed to the remaining databases.

### Impact on Users

- **Existing deployments using `rocksdb-max-background-jobs`**: This config key is no longer
  recognized. Users who had explicitly set this value will need to migrate to the new
  per-database options.
- **Existing deployments using defaults**: No action needed. The new defaults are computed
  automatically and should provide better behavior than the previous uniform allocation.
- **New deployments**: Use the automatic role-aware defaults.

### Migration Guidance

If you previously configured `rocksdb-max-background-jobs` globally or per-database section:

```toml
# Old (no longer recognized)
[worker.storage]
rocksdb-max-background-jobs = 8

# New: per-database control
[worker.storage]
rocksdb-max-background-flushes = 4
rocksdb-max-background-compactions = 8

[log-server]
rocksdb-max-background-flushes = 4
rocksdb-max-background-compactions = 4

[metadata-server]
rocksdb-max-background-flushes = 1
rocksdb-max-background-compactions = 1
```

For snapshot export concurrency (previously derived from background jobs):

```toml
[worker.snapshots]
export-concurrency-limit = 4
```

If you were relying on defaults, no configuration changes are needed.
