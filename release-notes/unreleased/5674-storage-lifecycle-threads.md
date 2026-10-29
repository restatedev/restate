# Release Notes for #5674: Configurable storage lifecycle thread pool

## New Feature

### What Changed

Restate now opens and closes its RocksDB databases on a dedicated thread pool, separate from the
thread pools used for regular storage reads and writes. A new option controls its size:

- `storage-lifecycle-threads`: threads for opening databases, flushing them on shutdown, and
  closing them. Default: `8`.

### Why This Matters

Opening a database no longer has to wait for a free storage I/O thread. Those threads also serve
regular reads and writes, so they can be busy for a long time.

### Impact on Users

- The option defaults to `8` threads. Nothing changes in your configuration unless you set it.
- The value must be a positive integer. `0` is rejected when the configuration is loaded.
- Changes take effect only after a restart.

### Migration Guidance

No action is needed. To change the pool size:

```toml
storage-lifecycle-threads = 8
```

### Related Issues

- Issue #5192
