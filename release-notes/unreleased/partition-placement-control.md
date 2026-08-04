# Partition Placement Control

## New Feature

### What Changed

Operators can freeze automatic replica-set changes for selected partitions during maintenance:

```bash
restatectl partitions placement freeze 0-4 --reason "Rolling restart"
restatectl partitions placement show 0-4
restatectl partitions placement unfreeze 0-4
```

The freeze is stored in partition epoch metadata and survives cluster-controller changes. It blocks
the cluster controller from creating or retargeting a pending replica set, but allows an existing
reconfiguration to complete. Explicit `placement set` operations remain available as operator
overrides.

Partition administration commands are now grouped under `partitions placement` and
`partitions leadership`; the standalone `partitions reconfigure` command has been removed.

### Why This Matters

Temporarily unavailable nodes no longer need to cause partition movement during maintenance when
operators expect those nodes to return shortly.

### Impact on Users

Automatic placement remains enabled by default. A freeze also prevents the cluster controller from
assigning an initial replica set, but it does not affect leadership election.
