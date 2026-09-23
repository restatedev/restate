# Release Notes: VQueue statistics system tables

## New Feature

### What Changed

The new `sys_service_stats`, `sys_deployment_stats`, and `sys_virtual_object_stats` SQL tables expose
VQueue entry counts grouped by service, deployment, and virtual-object dimensions, respectively.
`sys_service_stats` and `sys_deployment_stats` combine partition-local gauges into cluster-wide
counts in `num_entries`; physical partition IDs are not part of those two public schemas.

`sys_virtual_object_stats` exposes one row per partition-local virtual-object/handler/operation-kind
group, including its scope, `partition_id`, and `partition_key`. Its counters are `num_inbox`,
`num_running`, `num_suspended`, `num_paused`, and `num_finished`, with zero for absent stages.
It has no `stage` or `num_entries` column; for example, use `num_running > 0` to find groups with
running entries. It does not perform cross-partition aggregation, allowing limited queries to
return results without first aggregating every matching virtual object.

In v1.8, collection is enabled with:

```toml
[common]
experimental-enable-service-stats = true
```

Existing VQueue entries are not currently backfilled when this option is enabled on a non-empty
partition store. Counts are reliable only for stores where collection was enabled before entries
were added. Collection becomes automatic for new stores in v1.9.

### Why This Matters

Operators can inspect per-service, per-deployment, and per-virtual-object queue load without
scanning every VQueue or individual entry.

### Migration Guidance

On v1.8, enable the option before adding VQueue entries to an empty partition store. Do not use the
table as an authoritative total after enabling the option on a non-empty store until backfill is
implemented.
