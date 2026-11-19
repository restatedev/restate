# Partition leadership moves only to replicas that have caught up

## Behavioral Change

### What Changed

When the cluster controller moves a partition's leadership away from a healthy leader, it now waits
until the new leader has nearly caught up with the partition's log. This happens, for example, when a
node comes back after an outage and its replicas are preferred again, or when nodes join and the
cluster rebalances. The same applies when a replica set change would remove the current leader: the
change completes only once a remaining replica has caught up.

A replica counts as caught up when it is estimated to replay the rest of the log within 3 seconds.
A replica gets up to 5 catch-up rounds; each ends when it has replayed the log up to where the log
was when the round started. If it still hasn't caught up after the last round, leadership moves to
it anyway, as before. This also applies to a replica that falls further behind while it catches up.

Leadership still fails over immediately when the leader's node goes down.

### Why This Matters

A partition has no leader from the moment the old leader steps down until the new leader has
replayed the log up to the point where it took over. Previously, leadership could move to a replica
that was hundreds of thousands of records behind, which made the partition unavailable for a minute
or more, for example right after a node came back from an outage under write load.

### Impact on Users

- Shorter partition unavailability when nodes return, join or are replaced under load.
- After a node returns, its partitions' leadership moves back to it later than before, once it
  has caught up. Until then the current leaders keep serving.
- If replicas replay more slowly than the leaders write, for example under sustained overload, a
  planned leadership move or replica set change can wait for several minutes before it happens.
- A replica that applies no records at all does not get leadership while that lasts. The cluster
  controller logs a warning ("candidate applied nothing since the last probe") in that case.
- When a cluster controller starts, it assigns partition leaders only after its first status poll
  of the cluster (by default 1.5 seconds), and keeps replicas that are already leading.

### Migration Guidance

No action is needed. The behavior is controlled by these options:

```toml
[admin]
# "catch-up-time" (default) or "immediate"
leader-handover-policy = "catch-up-time"
# a replica takes over only if it can replay the rest of the log within this time
leader-handover-max-catch-up-time = "3s"
# how often a catching-up replica's progress is checked
leader-handover-probe-interval = "10s"
# catch-up rounds before leadership is moved anyway
leader-handover-max-attempts = 5
```

To restore the previous behavior, set `leader-handover-policy = "immediate"`, or
`RESTATE_ADMIN__LEADER_HANDOVER_POLICY=immediate` in the environment.
