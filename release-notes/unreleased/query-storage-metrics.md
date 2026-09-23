# Release Notes: Storage scan metrics in EXPLAIN ANALYZE

## New Feature

`EXPLAIN ANALYZE` now includes query-scoped iterator counters for native statistics
and entry-index scans. The metrics are collected locally and returned by remote
scanners that support metric reporting. `EXPLAIN ANALYZE VERBOSE` retains physical
Restate partition labels; the ordinary display aggregates counters across scans.

The counters include iterators opened/completed, keys visited, seek/next/prev calls,
key/value bytes visited, and native records emitted for Arrow conversion. Iterator
wall time includes consumer backpressure. Bytes visited are **not disk-read bytes**,
and a native record can produce multiple SQL rows for bucketed statistics.

Compare `storage_scans`, `storage_scans_reported`, and `storage_scans_completed`:

- A reported and completed scan has final accounting, including zero-result scans.
- A reported but incomplete scan contains progress only, for example after a parent
  LIMIT stops consumption before the background iterator finishes.
- An unreported scan has unavailable iterator accounting, for example on an older
  remote node or a scan path that has not yet adopted the probe. Zero counters in
  this case do not imply zero storage work.

No configuration change is required. Cheap iterator counters are unsampled and
published in batches. Detailed RocksDB block/cache profiling remains separate from
these counters; existing sampled process-wide metrics are not presented as exact
per-query I/O totals. No storage-format migration is required.
