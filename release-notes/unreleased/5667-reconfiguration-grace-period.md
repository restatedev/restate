# Release Notes for Issue #5667: Grace period before moving replicas away from dead nodes

## Behavioral Change

### What Changed

When a node goes down, the cluster controller now waits for a grace period (5 minutes by default)
before it moves that node's partition replicas to other nodes. If the node comes back within the
grace period, its partitions keep their replicas where they are. Leadership still moves to another
replica right away, so partitions with at least one replica on a live node stay available.

The new option `admin.reconfiguration-grace-period` controls how long to wait.

### Why This Matters

Previously, Restate started moving replicas as soon as a node went down. Building a replica on
another node is expensive (for example, it may download a partition snapshot), and it was wasted work
when the node was just being restarted, such as during a rolling upgrade.

### Impact on Users

- Restarting a node, or a rolling upgrade, no longer moves replicas to other nodes as long as each
  node is back within the grace period.
- If a node stays down, its partitions run with fewer replicas until the grace period ends.
- If all of a partition's replicas are on down nodes (for example, with a replication factor of 1),
  Restate moves the partition right away instead of waiting.
- Moving replicas off nodes that you drain or disable is not delayed.

### Migration Guidance

No action is needed. To change the grace period:

```toml
[admin]
reconfiguration-grace-period = "2m"
```

or through the environment:

```sh
RESTATE_ADMIN__RECONFIGURATION_GRACE_PERIOD=2m
```

Set it to `"0s"` to restore the previous behavior of moving replicas as soon as a node goes down.

### Related Issues

- Issue #5667: Introduce a configurable reconfiguration grace period for dead replica set members
