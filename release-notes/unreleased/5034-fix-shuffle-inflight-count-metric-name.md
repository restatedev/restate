# Release Notes for Issue #5034: Fix invalid Prometheus exposition for `restate_partition_shuffle_inflight_count`

## Bug Fix

### What Changed
The `restate_partition_shuffle_inflight_count` summary metric (introduced in 1.7) is replaced by the `restate_partition_shuffle_inflight` gauge. The underlying metric name previously ended in `.count`, which collided with the Prometheus exporter's summary suffix handling and produced an invalid sample line with no `quantile` label. A gauge also better represents the number of in-flight records sampled by shuffle events.

The `restate_partition_shuffle_message_count` counter is renamed to `restate_partition_shuffle_message_total` to follow the naming convention for counters.

### Why This Matters
Strict Prometheus scrapers (e.g. Vector's `prometheus_scrape` source) reject the entire scrape payload when they encounter this malformed line, so no Restate metrics were collected at all on 1.7.x while this metric existed.

### Impact on Users
- Deployments scraping `/metrics` with a strict Prometheus text-format parser regain all Restate metrics.
- The in-flight metric is now exposed as a gauge without the summary's `_sum`, `_count`, and `{quantile=...}` series.
- Dashboards and alerts referencing either renamed metric must be updated.

### Migration Guidance
Update dashboard and alert queries:
- `restate_partition_shuffle_inflight_count` to `restate_partition_shuffle_inflight`
- `restate_partition_shuffle_message_count` to `restate_partition_shuffle_message_total`

### Related Issues
- Issue #5034: `/metrics` emits invalid Prometheus exposition for `restate_partition_shuffle_inflight_count`, causing Vector to drop the whole scrape
