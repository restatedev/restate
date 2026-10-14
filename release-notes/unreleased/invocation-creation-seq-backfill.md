# Record where each invocation was created

## New Feature

### What Changed
Restate now records, for every new invocation, the point in the log where it was created.
The `sys_invocation_status` table shows it in a new `canonical_id` column: the invocation
ID followed by that position, e.g. `inv_..._42`.

A new experimental option, `experimental-enable-invocation-creation-seq`, adds this to
invocations that existed before the upgrade.

### Impact on Users
- Without the option, `canonical_id` is NULL for invocations created before the upgrade,
  and no action is needed.
- With the option enabled, each partition runs a one-time background step the next time
  it becomes leader. Partitions with many stored invocations (including completed
  invocations that are still retained) may take a little longer to become available.

### Migration Guidance
- Only enable the option once all nodes run v1.9.0 or newer.
- Once a partition has completed this step, it cannot be downgraded to a Restate version
  older than v1.9.0.
