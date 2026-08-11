# Release Notes: RocksDB rate limiter observability and auto-tune toggle

## New Feature

### What Changed
The shared RocksDB background-I/O rate limiter (which governs compaction and
flush write bandwidth, capped by `rocksdb-max-write-rate-per-second`) now
exposes its live state as Prometheus metrics:

- `restate_rocksdb_rate_limiter_current_bytes_per_second` (gauge) — the current,
  possibly auto-tuned, write-rate ceiling.
- `restate_rocksdb_rate_limiter_pending_requests{priority}` (gauge) — requests
  waiting for tokens. Absent (rather than zero) if the limiter cannot report it.
- `restate_rocksdb_rate_limiter_bytes_granted_total{priority}` (counter) — bytes
  admitted by the limiter (granted, not necessarily physically written).
- `restate_rocksdb_rate_limiter_requests_total{priority}` (counter) — number of
  requests admitted.

The `priority` label is the rocksdb IO priority: `high` (flush output), `low`
(compaction output), or `user` (flush/compaction elevated while the write
controller is stalling or stopping writes, i.e. exactly during write pressure).

A new configuration option `rocksdb-rate-limiter-auto-tuned` (default `true`)
controls whether the limiter auto-tunes its rate within
`[rocksdb-max-write-rate-per-second / 20, rocksdb-max-write-rate-per-second]`.
Set it to `false` to pin the limiter at `rocksdb-max-write-rate-per-second`.

### Why This Matters
The auto-tuned rate was previously invisible, making compaction backlogs hard to
diagnose. These metrics surface the limiter's live ceiling and per-priority
throughput.

### Impact on Users
- Existing deployments: no behavioral change; auto-tuning stays enabled by
  default. New metrics are emitted on the existing metrics scrape endpoint.
- New deployments: same defaults.

### Migration Guidance
No action required. To disable rate-limiter auto-tuning, set
`rocksdb-rate-limiter-auto-tuned = false` in the common configuration.
