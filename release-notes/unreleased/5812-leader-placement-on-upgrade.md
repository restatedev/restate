# Release Notes for PR #5812: Partition leader selection prefers the first replica

## Behavioral Change

### What Changed

When several replicas of a partition are equally suited to lead it, the cluster controller now
picks the first replica of the partition's replica set as leader. Previously, it picked the last
one.

### Impact on Users

- Clusters with a replication factor of 2 or more: after the upgrade, once a node running the new
  version becomes the cluster controller, many partitions change their leader at once. Expect a
  short burst of leader changes. It happens once.
- During a rolling upgrade, if the cluster controller role moves back and forth between nodes on the
  old and the new version, leaders can move back and forth too. This stops once all nodes run the
  new version.
- Clusters with a replication factor of 1 are not affected.

### Migration Guidance

No action is needed. To avoid repeated leader changes, finish the rolling upgrade promptly.
