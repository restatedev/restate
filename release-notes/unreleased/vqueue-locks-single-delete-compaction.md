# Release Notes: One-time vqueue/lock compaction on upgrade

## Behavioral Change

### What Changed
The partition store now uses RocksDB `SingleDelete` (instead of `Delete`) when removing vqueue
and lock keys. To make this safe on partitions that were previously written with `Delete`, each
partition runs a one-time forced compaction over the vqueue (`q*`) and lock (`lo`) key ranges the
first time it is opened by this version. Completion is recorded per partition in the node's
cluster marker so the compaction does not run again on subsequent startups.

### Why This Matters
`SingleDelete` is only correct when a key has not accumulated regular `Delete` tombstones or
multiple `Put`s. Clusters that enabled experimental vqueues in v1.7.0 wrote these keys with
`Delete`, so the ranges must be compacted once before `SingleDelete` is used to avoid stale or
resurrected values.

### Impact on Users
- New deployments: the compaction runs over empty ranges and is effectively a no-op.
- Existing deployments with vqueues data: a one-time compaction runs at partition startup, which
  may slightly increase startup time the first time the partition is opened on this version.
- The record is intentionally kept in the cluster marker rather than partition data: downgrading
  to an older binary drops the record, so a later re-upgrade safely recompacts.

### Migration Guidance
No action required; the compaction is automatic.
