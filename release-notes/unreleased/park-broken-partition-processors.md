# Release Notes: Partition processors are parked instead of retried when the local store is sealed

## Behavioral Change

### What Changed

When a partition processor fails because its local partition store is sealed (the store's applied
LSN is ahead of the log tail, which indicates data loss in the log), the node no longer retries the
processor on a backoff. It parks the processor and reports it as **broken**:

- `restatectl partition list` shows `Broken (ahead-of-log)` in the `STATUS` column.
- `restatectl status` counts broken processors separately, e.g. `2 (1 broken)` under `FOLLOWERS`.
- The `partition_state` SQL table has a new `broken_reason` column (`NULL` while healthy). Its
  `replay_status` is `NULL` for a broken processor, since there is no replay in progress.
- The `restate.partition.blocked_flare{reason="ahead_of_log"}` gauge is still raised.

Other blocked states (version barrier, migration barrier, missing snapshot) are unchanged and keep
retrying, because a cluster upgrade or a newly published snapshot can resolve them on its own.

### Why This Matters

A sealed store cannot be repaired by retrying: the local data must be discarded and replaced from a
snapshot. Retrying every 30 seconds only produced log noise and made it impossible to tell a
transiently failing processor from one that will never recover.

### Impact on Users

- A parked processor stays down until an operator intervenes, the node restarts, or the node leaves
  the partition's replica set. Previously it would keep retrying (and keep failing).
- Nothing to do for healthy deployments; the new state is only reachable via a sealed partition
  store.

### Migration Guidance

None. Older `restatectl` builds ignore the new field and render a broken processor as a follower
that never becomes active.
