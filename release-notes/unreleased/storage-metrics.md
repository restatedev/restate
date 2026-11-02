# Release Notes: Changes to storage metrics

## Behavioral Change

### What Changed
- The RocksDB performance metrics (`restate_rocksdb_perf_*`, labeled by `name`) no longer have series named `flush-wal`, `bg-wal-sync`, `manual-flush`, `manual-compaction`, and `flush-partition`. Exporting a partition snapshot used to be measured twice under `export-column-family`, and is now measured once.

### Why This Matters
The performance metrics describe how RocksDB serves reads and writes, such as memtable lookups and block reads. For flushes and compactions they didn't apply, so their values were misleading.

### Impact on Users
- Dashboards and alerts that use the removed `restate_rocksdb_perf_*` series stop receiving data. How long these operations take is in `restate_rocksdb_manager_bg_task_run_duration_seconds`, under the operations `flush-wal`, `flush-memtables`, and `compaction`.

### Migration Guidance
Update dashboards and alerts as described above. No configuration changes are needed.
