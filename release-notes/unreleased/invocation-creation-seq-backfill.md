# Record where each invocation was created

## New Feature

### What Changed
Restate now records, for every new invocation, the point in the log where it was created.
The `sys_invocation_status` table shows it in a new `canonical_id` column: the invocation
ID followed by that position, e.g. `inv_..._42`.

A new experimental option, `experimental-enable-invocation-creation-seq`, adds this to
invocations that existed before the upgrade and had not completed yet.

### Impact on Users
- Without the option, `canonical_id` is NULL for invocations created before the upgrade,
  and no action is needed.
- With the option enabled, each partition runs a one-time background step the next time
  it becomes leader. Partitions with many running or queued invocations may take a little
  longer to become available.
- Invocations that had already completed before this step keep a NULL `canonical_id` until
  their retention period expires.

### Migration Guidance
- Only enable the option once all nodes run v1.8.1 or newer.
- Once a partition has completed this step, it cannot be downgraded to a Restate version
  older than v1.8.1.
