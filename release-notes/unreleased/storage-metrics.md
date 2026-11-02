# Release Notes: Changes to storage metrics

## Behavioral Change

### What Changed
- The RocksDB performance metrics (`restate_rocksdb_perf_*`, labeled by `name`) no longer have series named `flush-wal`, `bg-wal-sync`, `manual-flush`, `manual-compaction`, and `flush-partition`. Exporting a partition snapshot used to be measured twice under `export-column-family`, and is now measured once.
- The background storage task metrics (`restate_rocksdb_manager_bg_task_in_flight_total`, `restate_rocksdb_manager_bg_task_wait_duration_seconds`, and `restate_rocksdb_manager_bg_task_run_duration_seconds`) are labeled by `executor` and `operation`. `executor` is the thread pool that ran the task: `rs:io-hi` for high-priority work, `rs:io-lo` for low-priority work, and `rs:io-lifecycle` for opening and closing databases. The in-flight gauge no longer has a `priority` label: `executor` tells the two priorities apart.
- The wait duration measures how long a task waited from the moment it was submitted to its thread pool until it started.
- Closing databases, deleting column families, and shutting down storage are now included in the background task metrics, under the operations `close-db`, `drop-column-family`, and `shutdown`.

### Why This Matters
The performance metrics describe how RocksDB serves reads and writes, such as memtable lookups and block reads. For flushes and compactions they didn't apply, so their values were misleading. The background task metrics now show which thread pool is busy or backed up.

### Impact on Users
- Dashboards and alerts that use the removed `restate_rocksdb_perf_*` series stop receiving data. How long these operations take is in `restate_rocksdb_manager_bg_task_run_duration_seconds`, under the operations `flush-wal`, `flush-memtables`, and `compaction`.
- Dashboards that filter or group the in-flight gauge by `priority` need to use `executor` instead: `rs:io-hi` is high priority and `rs:io-lo` is low priority.

### Migration Guidance
Update dashboards and alerts as described above. No configuration changes are needed.
