# Release Notes: VQueues and related features enabled by default

## Behavioral Change

### What Changed

The following experimental flags have been removed, and the features they gated are now always
enabled:

- `experimental-enable-vqueues`: Virtual queues (VQueues) are now enabled unconditionally. When a
  partition leader starts and its partition has not yet fully migrated to VQueues, it automatically
  proposes a `VersionBarrier` that enables VQueues and triggers the migration — the same behavior as
  previously running with the experimental flag turned on.
- `experimental-enable-vqueues-migration-skip-completed`: The migration always includes completed
  invocations. Partitions that previously performed a partial migration (skipping completed
  invocations) are automatically upgraded to a full migration on the next leader election.
- `experimental-enable-invoker-yield`: Invocations that exhaust the global invoker memory pool now
  always yield back to the scheduler instead of consuming retry attempts.
- `experimental-enable-scoped-virtual-objects`: Scope is now accepted on Virtual Object targets.
  Scoped invocations in general no longer require an opt-in (previously rejected with
  "scoped invocations require vqueues to be enabled" unless the vqueues flag was set).

### Why This Matters

VQueues are the foundation for node-level invoker concurrency, scoped invocations, and fair
scheduling. Enabling them by default removes the operator opt-in step and guarantees all partitions
converge to the same execution model.

### Impact on Users

- On upgrade, each partition that has not yet migrated to VQueues will run the migration when its
  leader is elected. The migration includes completed invocations; partitions with a large
  completion-retention backlog may take longer to migrate.
- The `VersionBarrier` raises the partition's minimum required Restate version. All nodes in the
  cluster must be upgraded before the barrier is applied; do not enable v1.8.0 nodes alongside nodes
  that cannot satisfy the barrier.
- Setting the removed `experimental-enable-*` flags in configuration no longer has any effect.
- Memory-pool exhaustion now yields instead of retrying, so invocations no longer consume retry
  attempts (or hit retry policies) due to memory pressure alone.
- Scoped Virtual Object invocations, previously rejected at the HTTP ingress, Kafka ingress, and
  service-to-service call path, are now accepted.

### Migration Guidance

- Remove any `experimental-enable-vqueues`, `experimental-enable-vqueues-migration-skip-completed`,
  `experimental-enable-invoker-yield`, or `experimental-enable-scoped-virtual-objects` entries from
  your configuration.
- Upgrade all nodes in the cluster to v1.8.0 before expecting partitions to make progress past the
  automatically proposed `VersionBarrier`.
- Review `worker.invoker.concurrent-invocations-limit`: with VQueues, the limit applies per node
  instead of per partition processor (see the corresponding release note about the new default).

#### Reducing migration downtime for large invocation histories

If you are concerned about how long the migration may take — as a guideline, if your partitions hold
more than 10 million invocations in their histories — you can stage the migration to avoid migrating
the completed-invocation backlog:

1. First upgrade the cluster to v1.7.10 and enable VQueues with the skip-completed migration:

   ```toml
   experimental_enable_vqueues = true
   experimental_enable_vqueues_migration_skip_completed = true
   ```

2. After another rolling restart, the cluster migrates only the "running" invocations. Completed
   invocations keep their existing status and fall out of retention over time — check your retention
   settings to know how long this takes.

3. Once the completed backlog has drained, disabling
   `experimental_enable_vqueues_migration_skip_completed` or upgrading to v1.8 completes the full
   migration with much less downtime, since few or no completed invocations remain to migrate.
