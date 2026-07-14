# Release Notes for Issue #5034: Fix invalid Prometheus exposition for `restate_partition_shuffle_inflight_count`

## Bug Fix

### What Changed
The `restate_partition_shuffle_inflight_count` summary metric (introduced in 1.7) is renamed to `restate_partition_shuffle_inflight`. The underlying metric name previously ended in `.count`, which caused the Prometheus exporter to omit the `_count` suffix from the summary's observation-count series, producing an invalid sample line with no `quantile` label.

### Why This Matters
Strict Prometheus scrapers (e.g. Vector's `prometheus_scrape` source) reject the entire scrape payload when they encounter this malformed line, so no Restate metrics were collected at all on 1.7.x while this metric existed.

### Impact on Users
- Deployments scraping `/metrics` with a strict Prometheus text-format parser regain all Restate metrics.
- Any dashboards or alerts referencing `restate_partition_shuffle_inflight_count` must be updated to `restate_partition_shuffle_inflight` (the exporter will continue to expose `_sum`, `_count`, and `{quantile=...}` series under the new base name).

### Migration Guidance
Update dashboard/alert queries from `restate_partition_shuffle_inflight_count` to `restate_partition_shuffle_inflight`.

### Related Issues
- Issue #5034: `/metrics` emits invalid Prometheus exposition for `restate_partition_shuffle_inflight_count`, causing Vector to drop the whole scrape
