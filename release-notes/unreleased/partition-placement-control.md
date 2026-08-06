# Partition Placement Control

## New Feature

### What Changed

Operators can freeze automatic replica-set changes for selected partitions during maintenance:

```bash
restatectl partitions placement freeze 0-4 --reason "Rolling restart"
restatectl partitions placement show 0-4
restatectl partitions placement unfreeze 0-4
restatectl partitions placement set 0 --replicas 1,2 --freeze "Pin after migration"
```

The freeze is stored in partition epoch metadata and survives cluster-controller changes. It blocks
the cluster controller from creating or retargeting a pending replica set, but allows an existing
reconfiguration to complete. Explicit `placement set` operations remain available as operator
overrides.

An explicit empty replica set can be used to stop all partition processors for a partition:

```bash
restatectl partitions placement set 0 --freeze --replicas
```

The command warns and asks for confirmation because the partition becomes unavailable while its
replica set remains empty. Pass `--yes` for non-interactive use. Freezing placement keeps the
replica set empty; without a freeze, the cluster controller may replace it with a non-empty set
when enough eligible workers are available. Unfreezing an empty placement resumes automatic
assignment. Stopping partition processors does not delete their local partition stores.

Partition administration commands are now grouped under `partitions placement` and
`partitions leadership`; the standalone `partitions reconfigure` command has been removed.

### Why This Matters

Temporarily unavailable nodes no longer need to cause partition movement during maintenance when
operators expect those nodes to return shortly.

### Impact on Users

Automatic placement remains enabled by default. Standalone freeze requires an existing valid
placement and does not affect initial assignment or leadership election. Use `placement set
--freeze <REASON>` to atomically set and freeze a placement.
