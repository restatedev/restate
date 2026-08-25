# Configurable RocksDB storage tuning

## Behavioral Change / New Feature

### What Changed

Log-server and partition-store RocksDB configurations now support these options:

- `rocksdb-writable-file-max-buffer-size`, which defaults to `1 MiB`.
- `rocksdb-l0-num-compaction-trigger`, which defaults to `8` for `[log-server]` and `2` for `[worker.storage]`.
- `rocksdb-max-open-files`, which is unset by default so opened files are kept open.
- `rocksdb-partition-sst-by-log`, which defaults to `true` and prevents compacted log-server SST files from spanning logs.
- `rocksdb-partition-sst-by-loglet`, which defaults to `false` and, when enabled, partitions compacted SST files by loglet instead of log.

Existing log-server BlobDB and background-compaction options are now included in the generated configuration schema.

Restate now derives each database's memtable count and write-buffer size from its memory budget and configured target SST size. The minimum target SST size is lowered from `16 MiB` to `8 MiB`, and explicit log-server memtable budgets below `32 MiB` are raised to that minimum. When log-server blob separation is enabled, non-L0 SST files target `8 MiB` and the base level targets `80 MiB`.

### Why This Matters

The new settings let operators tune RocksDB file I/O and compaction behavior for their storage devices and workloads. Deriving write-buffer sizing from the available memory budget keeps the configured memtable budget and target SST size aligned. Keeping SST boundaries aligned with logs or loglets reduces compaction overlap when their data progresses independently.

### Impact on Users

Existing configurations remain accepted, but deployments will adopt the new write-buffer, SST, compaction, and log-server WAL sizing behavior after upgrading. The configurable L0 trigger defaults preserve the previous hard-coded values. Log-server compaction output is partitioned by log by default; existing SST files are rewritten with these boundaries through normal compaction.

### Migration Guidance

No configuration migration is required. Operators with workload-specific RocksDB tuning should review the new sizing behavior and benchmark their existing `rocksdb-memory-budget` and `rocksdb-max-file-size` settings.

Changes to DB-level writable-file buffers, open-file limits, and partition-store L0 triggers take effect when the database is reopened. Log-server BlobDB and L0 options support live configuration updates.

To retain SST file boundaries based only on the configured target file size, disable log partitioning:

```toml
[log-server]
rocksdb-partition-sst-by-log = false
```

SST partitioning options take effect when the log-server database is reopened.
