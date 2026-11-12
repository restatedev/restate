# Move leadership away from stalled partition leaders

## Behavioral Change

### What Changed
The cluster controller now detects partition leaders that are running but have stopped
making progress, for example because their node's disk is too slow to accept writes. It
moves leadership of the affected partitions to a healthy replica. Previously, leadership
only moved when the leader's node was considered dead.

Leadership is now also sticky: a healthy leader keeps its leadership when another replica
comes back, for example after a node restart. The cluster controller moves leaders back to
their preferred replicas, including those selected by a leader affinity, periodically
instead of immediately. Changing a partition's leadership policy still takes effect
immediately.

### Impact on Users
- Partitions recover on their own when a single node stalls, as long as another replica is
  healthy. If all replicas of a partition are stalled, leadership does not move.
- After a node restart, it can take up to the rebalance interval (5 minutes by default)
  until the node leads partitions again.

Two new options control the behavior:

```toml
[admin]
# How long a partition processor may go without reporting progress before it is
# considered stalled. Set to "0s" to disable stall detection.
partition-leader-stall-threshold = "10s"
# How often leaders are moved back to their preferred replicas.
partition-leader-rebalance-interval = "5m"
```
