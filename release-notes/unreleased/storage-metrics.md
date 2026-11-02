# Release Notes: Changes to storage metrics

## Behavioral Change

### What Changed
- The RocksDB performance metrics (`restate_rocksdb_perf_*`, labeled by `name`) no longer have series named `flush-wal`, `bg-wal-sync`, `manual-flush`, `manual-compaction`, `flush-partition`, `export-column-family`, `import-column-family`, and `shutdown`.
- The in-flight gauge of background storage tasks (`restate_rocksdb_manager_bg_task_in_flight_total`) no longer has a `priority` label.

### Why This Matters
The performance metrics describe how RocksDB serves reads and writes, such as memtable lookups and block reads. For flushes, compactions, imports, exports, and shutting down they didn't apply, so their values were misleading.

### Impact on Users
- Dashboards and alerts that use the removed `restate_rocksdb_perf_*` series stop receiving data. How long these operations take is in `restate_rocksdb_manager_bg_task_run_duration_seconds`, under the operations `flush-wal`, `flush-memtables`, `compaction`, `export-column-family`, and `import-column-family`. There is no replacement for `shutdown`.
- Dashboards that filter or group the in-flight gauge by `priority` lose that breakdown.

### Migration Guidance
Update dashboards and alerts as described above. No configuration changes are needed.
