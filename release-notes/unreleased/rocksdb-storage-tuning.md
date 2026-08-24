# Configurable RocksDB storage tuning

## Behavioral Change / New Feature

### What Changed

Log-server and partition-store RocksDB configurations now support these options:

- `rocksdb-writable-file-max-buffer-size`, which defaults to `1 MiB`.
- `rocksdb-l0-num-compaction-trigger`, which defaults to `8` for `[log-server]` and `2` for `[worker.storage]`.
- `rocksdb-max-open-files`, which is unset by default so opened files are kept open.

Existing log-server BlobDB and background-compaction options are now included in the generated configuration schema.

Restate now derives each database's memtable count and write-buffer size from its memory budget and configured target SST size. Once individual write buffers reach the target SST size, increasing the log-server memtable budget adds more memtables instead of continuing to grow each write buffer. The minimum target SST size is lowered from `16 MiB` to `8 MiB`, and explicit log-server memtable budgets below `32 MiB` are raised to that minimum. When log-server blob separation is enabled, non-L0 SST files target `8 MiB` and the base level targets `80 MiB`.

### Why This Matters

The new settings let operators tune RocksDB file I/O and compaction behavior for their storage devices and workloads. Using larger budgets for more memtables rather than larger write buffers bounds the size of individual buffers while still letting operators increase total memtable memory.

### Impact on Users

Existing configurations remain accepted, but deployments will adopt the new write-buffer, SST, compaction, and log-server WAL sizing behavior after upgrading. The configurable L0 trigger defaults preserve the previous hard-coded values.

### Migration Guidance

No configuration migration is required. Operators with workload-specific RocksDB tuning should review the new sizing behavior and benchmark their existing `rocksdb-memory-budget` and `rocksdb-max-file-size` settings.

Changes to DB-level writable-file buffers, open-file limits, and partition-store L0 triggers take effect when the database is reopened. Log-server BlobDB and L0 options support live configuration updates.
