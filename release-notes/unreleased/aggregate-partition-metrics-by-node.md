# Release Notes: Report partition metrics as per-node aggregates

## Breaking Change

### What Changed

High-cardinality per-partition gauges for applied LSN lag, snapshot age, and time since the
last status update are now reported as per-node histograms. The effective-leader gauge is
replaced by a per-node active-leader count, and a new gauge counts partitions whose applied
LSN lag is unknown.

### Why This Matters

Metrics no longer leave stale per-partition series behind when partitions move between nodes.

### Impact on Users

Dashboards and alerts that query the affected per-partition gauges or their `partition` label
must be updated to use the new per-node gauges and histogram quantiles. The bundled Grafana
dashboards have been updated.

### Migration Guidance

Update custom Prometheus queries as follows:

- Replace `restate_partition_is_effective_leader` with
  `restate_num_active_partition_leaders`.
- Query `restate_partition_applied_lsn_lag` and `restate_partition_snapshot_age_seconds` as
  histograms without the `partition` label.
- Replace `restate_partition_time_since_last_status_update` with the histogram
  `restate_partition_time_since_last_status_update_seconds`.
- Use `restate_partition_num_unknown_applied_lsn_lag` to monitor partitions with unknown lag.

If you're using our official grafana dashboards, just re-import the latest version and it takes
care of this migration for you.
