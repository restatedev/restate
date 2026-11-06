# Release Notes for Issue #4808: Experimental balanced placement strategy

## New Feature

### What Changed
Added an opt-in experimental placement strategy for small flat clusters. When
enabled, Restate balances partition replica placement, partition processor
leaders, and replicated-loglet nodeset membership using deterministic load-aware
selection.

Two new common configuration options control the behavior:

- `experimental-placement-strategy`: `legacy` (default) or `balanced-v2`
- `experimental-placement-rebalance-mode`: `repair-only` or `rebalance` (default)

The log-server role also now emits
`restate.log_server.nodeset_memberships`, a per-node gauge counting current
replicated log tails whose nodeset contains the local log-server.

### Why This Matters
The existing deterministic placement can create severe leader, follower, and
log-server nodeset skew in small clusters with tens or hundreds of partitions.
The experimental strategy is intended to make benchmark and load-test feedback
objective before changing the default placement behavior.

### Impact on Users
Existing deployments continue to use the legacy placement strategy unless they
opt in. Enabling `balanced-v2` can move existing partition replicas and loglet
nodesets, especially with `experimental-placement-rebalance-mode = "rebalance"`.
Repairs retain eligible existing members and prefer processors reported active,
including members of a pending placement. Optional balancing waits for processor
reports and never removes the only reported-active current replica. Single-copy
partitions are not moved solely to improve balance. These decisions use reported
state; they do not guarantee readiness to serve at the time of a failure.
Repair-only mode retains viable leaders and replica placements. Placement and
leadership freezes remain respected, and viable pending replica changes are
allowed to finish before another rebalance is planned.
Partition balancing starts from existing placements. It moves a replica or leader
only from a node with at least two more assignments than its destination, so an
already-balanced cluster is not reshuffled merely to restore an earlier placement.
After a restart, placement can settle on a different, equally fair distribution.
Loglet nodeset rebalancing only reconfigures existing loglets when the proposed
change reduces the current cluster-wide nodeset membership range.

### Migration Guidance
This is experimental and intended for controlled benchmark/load-test trials.
Use it only when all nodes in the cluster are configured consistently. If some
cluster controllers run `balanced-v2` while others run `legacy`, they can compute
different target placements and repeatedly reconfigure partitions or loglets
toward whichever strategy wins the latest metadata update.

### Related Issues
- Issue #4808: Improve default partition/log placement balance
