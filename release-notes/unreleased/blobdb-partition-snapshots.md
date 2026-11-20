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

### Snapshot triggers from newly durable files

Blob-enabled partitions can also snapshot less often and without the small flush that each
snapshot used to cause. This is experimental:

```toml
[worker.snapshots]
experimental-snapshot-trigger = "new-files"
# Upload only once this much of the partition's data is not in the repository yet (default: 64 MiB).
snapshot-min-new-bytes = "64 MiB"
# Flush and snapshot a partition that has not persisted writes for this long (default: 6h).
snapshot-max-unflushed-age = "6h"
```

With this trigger the node checkpoints each partition's database every five minutes (or the
configured `snapshot-interval`) without flushing it, and uploads the checkpoint only when enough of
its SST and blob files are new. In steady state this causes no snapshots and no extra flushes. A
low-traffic partition that stays unflushed for longer than `snapshot-max-unflushed-age` is flushed
and snapshotted once, so its state is captured even when no new files appear. Explicit snapshots
requested with `restatectl` keep flushing and publishing as before. `snapshot-interval-num-records`
is ignored by this trigger.

This trigger requires `experimental-rocksdb-enable-blob-files` and
`worker.use-multi-db-layout`; an invalid combination is rejected at startup.
