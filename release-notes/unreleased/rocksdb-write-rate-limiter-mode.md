# Release Notes: Configurable RocksDB write rate limiter mode

## New Feature

### What Changed

A new option, `rocksdb-write-rate-limiter-mode`, controls how Restate applies
`rocksdb-max-write-rate-per-second` to RocksDB flushes and compactions:

- `auto-tuned` (default, unchanged behavior): the write rate follows recent background IO
  demand, between 1/20 of `rocksdb-max-write-rate-per-second` and the full value.
- `fixed`: flushes and compactions always share the full `rocksdb-max-write-rate-per-second`.

### Why This Matters

With auto-tuning, a node that has been quiet sits at 1/20 of the configured maximum, and the rate
climbs back by 5% every 10 seconds, so it takes minutes to reach the full rate. A sudden write
burst therefore meets a much lower flush budget than configured, and writes can stall until the
burst ends. This is most visible when `rocksdb-max-write-rate-per-second` is set close to the
storage device's bandwidth, since the lower bound is then far below what the device can do.

### Impact on Users

- **Existing deployments**: No change. The default stays `auto-tuned`.
- **Deployments with bursty write load**: `fixed` removes the ramp-up, so flushes can use the full
  configured bandwidth as soon as a burst starts.

### Migration Guidance

To switch, set the mode together with a maximum that the storage device can sustain, and restart
the node:

```toml
rocksdb-max-write-rate-per-second = "2400 MiB"
rocksdb-write-rate-limiter-mode = "fixed"
```

Or, through the environment: `RESTATE_ROCKSDB_WRITE_RATE_LIMITER_MODE=fixed`.
