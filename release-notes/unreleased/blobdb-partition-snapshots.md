# Release Notes: Blob files in partition stores

## New Feature (Experimental)

### What Changed

Partition stores can now store large values in separate RocksDB blob files instead of inline in
SST files. This is off by default and requires the multi-database layout:

```toml
[worker]
use-multi-db-layout = true

[worker.storage]
experimental-rocksdb-enable-blob-files = true
# Values at or above this size go to blob files (default: 512 KiB).
experimental-rocksdb-blob-min-size = "512 KiB"
```

A partition with blob files is snapshotted as a whole-database checkpoint. Its SST and blob files
are stored once in the shared `ssts/` prefix, and its database metadata lives with the snapshot.
Restoring such a snapshot opens the downloaded database directly, without importing a column
family.

### Impact on Users

- **Configuration**: enabling blob files without `worker.use-multi-db-layout` is rejected at
  startup.
- **Snapshots**: snapshots of blob-enabled partitions use a new snapshot format (`V3`). Servers
  running an older release reject these snapshots instead of failing partway through a restore.
  Snapshots without blob files keep their existing format, so older servers can still restore
  them.
- **Turning it off**: blob files stay in the database once written. With the option turned off,
  snapshots of such partitions fail instead of leaving data out. Older releases would take incomplete
  snapshots of them, so do not roll back. Treat enabling blob files as one-way for now.
- **Tooling**: `restate-doctor` cannot yet inspect blob-enabled snapshots.
- **Storage**: blob files may leave unreferenced data on disk until RocksDB garbage collection
  rewrites them. Snapshot pruning treats blob files like shared SST files.
