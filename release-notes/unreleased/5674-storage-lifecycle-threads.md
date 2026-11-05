# Release Notes for #5674: Configurable storage lifecycle thread pool

## New Feature

### What Changed

Restate now opens and closes its RocksDB databases, and creates, imports, exports, and deletes
partition stores, on a dedicated thread pool, separate from the thread pools used for regular
storage reads and writes. A new option controls its size, next to
`storage-high-priority-bg-threads` and `storage-low-priority-bg-threads`, which size the pools for
reads and writes:

- `storage-lifecycle-bg-threads`: threads for opening databases, flushing them on shutdown,
  closing them, creating, importing, exporting, and deleting partition stores, and flushing and
  compacting all databases on `SIGHUP`. Default: `8`.

### Why This Matters

Opening a database, creating a partition store, or importing or exporting a partition snapshot no
longer has to wait for a free storage I/O thread. Those threads also serve regular reads and
writes, so they can be busy for a long time.

### Impact on Users

- The option defaults to `8` threads. Nothing changes in your configuration unless you set it.
- The value must be a positive integer. `0` is rejected when the configuration is loaded.
- Changes take effect only after a restart.
- A `SIGHUP` compaction holds one of these threads until it finishes. With the option set to `1`,
  opening databases and partition stores, and exporting snapshots, wait for it.

### Migration Guidance

No action is needed. To change the pool size:

```toml
storage-lifecycle-bg-threads = 8
```

### Related Issues

- Issue #5192
