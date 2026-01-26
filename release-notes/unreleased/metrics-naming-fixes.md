# Release Notes: Prometheus Metrics Naming Convention Fixes

## Breaking Change

### What Changed

Several Prometheus metric names have been corrected to follow proper naming conventions:

1. **RocksDB gauge metrics no longer have incorrect `_count` suffix**:
   - `restate_rocksdb_actual_delayed_write_rate_count` → `restate_rocksdb_actual_delayed_write_rate`
   - `restate_rocksdb_background_errors_count` → `restate_rocksdb_background_errors`
   - `restate_rocksdb_compaction_pending_count` → `restate_rocksdb_compaction_pending`
   - `restate_rocksdb_estimate_num_keys_count` → `restate_rocksdb_estimate_num_keys`
   - `restate_rocksdb_is_write_stopped_count` → `restate_rocksdb_is_write_stopped`
   - `restate_rocksdb_mem_table_flush_pending_count` → `restate_rocksdb_mem_table_flush_pending`
   - `restate_rocksdb_min_log_number_to_keep_count` → `restate_rocksdb_min_log_number_to_keep`
   - `restate_rocksdb_num_deletes_active_mem_table_count` → `restate_rocksdb_num_deletes_active_mem_table`
   - `restate_rocksdb_num_deletes_imm_mem_tables_count` → `restate_rocksdb_num_deletes_imm_mem_tables`
   - `restate_rocksdb_num_entries_active_mem_table_count` → `restate_rocksdb_num_entries_active_mem_table`
   - `restate_rocksdb_num_entries_imm_mem_tables_count` → `restate_rocksdb_num_entries_imm_mem_tables`
   - `restate_rocksdb_num_files_at_level{0-6}_count` → `restate_rocksdb_num_files_at_level{0-6}`
   - `restate_rocksdb_num_immutable_mem_table_count` → `restate_rocksdb_num_immutable_mem_table`
   - `restate_rocksdb_num_live_versions_count` → `restate_rocksdb_num_live_versions`
   - `restate_rocksdb_num_running_compactions_count` → `restate_rocksdb_num_running_compactions`
   - `restate_rocksdb_num_running_flushes_count` → `restate_rocksdb_num_running_flushes`

2. **Fixed duplicate `_bytes` suffix**:
   - `restate_rocksdb_estimate_pending_compaction_bytes_bytes` → `restate_rocksdb_estimate_pending_compaction_bytes`

3. **Fixed duplicate `_count` suffix on summary metrics**:
   - `restate_rocksdb_num_sst_read_per_level_count` → `restate_rocksdb_num_sst_read_per_level`
   - `restate_rocksdb_read_num_merge_operands_count` → `restate_rocksdb_read_num_merge_operands`

### Why This Matters

The previous metric names violated Prometheus naming conventions:
- The `_count` suffix should only be used for the count component of summary/histogram metrics, not for gauges that measure quantities
- Having `_bytes_bytes` or `_count_count` suffixes was incorrect and confusing

The new names follow [Prometheus best practices for metric naming](https://prometheus.io/docs/practices/naming/).

### Impact on Users

- **Prometheus/Grafana dashboards**: Any dashboards, alerts, or recording rules that reference the old metric names will need to be updated
- **Monitoring integrations**: External monitoring systems querying these metrics will need updates

### Migration Guidance

Update any Prometheus queries, Grafana dashboards, or alerting rules to use the new metric names. For example:

```promql
# Old query
sum(restate_rocksdb_background_errors_count)

# New query
sum(restate_rocksdb_background_errors)
```

If you have many dashboards to update, you can use find-and-replace to remove the `_count` suffix from RocksDB gauge metrics:
- Replace `_count{` with `{` for affected metrics
- Replace `_bytes_bytes` with `_bytes` for `estimate_pending_compaction_bytes`
