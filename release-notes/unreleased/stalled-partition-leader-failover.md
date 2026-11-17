# Detect stalled nodes and move partition leaders away from them

## New Feature

### What Changed
The cluster controller now detects nodes that are running but can no longer write to their
storage, for example because their disk is too slow. Partition leaders on such a node stop
making progress, but previously kept their leadership because the node was still alive.

By default, the cluster controller only reports what it detects:

- It logs when a node stalls and which partition leaders it would move.
- `restate.cluster_controller.stalled_nodes.total` counts stalled nodes. The `outcome`
  label tells whether the node was `suspended` from leadership or `ignored` (see below).
- `restate.partition_leader.stall_failovers.total` counts the leaders that would have
  moved (`outcome="skipped"`) or that did move (`outcome="moved"`).

If you enable stall failover, the cluster controller moves the partition leaders away from
the stalled node. It is deliberately conservative, because a stall can also be caused by
the workload, in which case moving leaders only moves the problem:

- A node is only suspended from leadership if no other node stalled recently.
- A leader only moves to a replica that is caught up with it.
- The node leads partitions again once it has been free of stalls for ten times the stall
  threshold (10 minutes by default).

### Impact on Users
- No change in behavior by default; stalls are reported in logs and metrics.
- With stall failover enabled, partitions recover on their own when a single node stalls,
  as long as their other replicas are caught up.

Two new options control the behavior:

```toml
[admin]
# A node is considered stalled if one of its partition processors makes no progress for
# this long, while none of its partition processors applies any records. Set to "0s" to
# disable stall detection.
partition-leader-stall-threshold = "1m"
# Whether partition leaders move away from a stalled node. If disabled, stalls are only
# reported.
partition-leader-stall-failover = false
```
