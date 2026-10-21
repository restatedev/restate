# Release Notes: VQueue statistics system tables

## New Feature

### What Changed

The new `sys_service_stats`, `sys_deployment_stats`, and `sys_virtual_object_stats` SQL tables expose
VQueue entry counts grouped by service, deployment, and virtual-object dimensions, respectively.
All three expose `stage`, `status`, and `num_entries`, so you can distinguish, for example,
succeeded and failed entries within the finished stage.
`sys_service_stats` and `sys_deployment_stats` combine partition-local gauges into cluster-wide
counts in `num_entries`; physical partition IDs are not part of any of these public schemas.

`sys_virtual_object_stats` exposes one row per non-empty stage/status bucket in each partition-local
virtual-object/handler/operation-kind group, including its scope and `partition_key`.
For example, use `stage = 'running'` to find groups with running entries, or
`stage = 'finished' AND status = 'failed'` to count failed entries.
It does not perform cross-partition aggregation, allowing limited queries to
return results without first aggregating every matching virtual object.

In v1.8.2, collection is enabled with:

```toml
[common]
experimental-enable-track-status-gauges = true
```

When collection is first enabled, each local partition counts its existing VQueue entries,
including retained completed entries, before starting. This can increase initial startup time
on large stores. Statistics queries report that a partition is not ready until its initial
counts are complete. Collection becomes automatic in v1.9.

### Why This Matters

Operators can inspect per-service, per-deployment, and per-virtual-object queue load without
scanning every VQueue or individual entry.

### Migration Guidance

On v1.8.2, enable the option and restart the affected nodes. Existing VQueue entries are included
automatically; entries that have already been purged and legacy invocations without VQueue entries
are not counted. Secondary indexes do not need to be enabled.
