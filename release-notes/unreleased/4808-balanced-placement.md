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
