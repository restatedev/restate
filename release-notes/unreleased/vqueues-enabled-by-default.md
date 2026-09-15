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

The partition leader no longer runs the legacy, non-VQueues invocation path. Invocations are always
handed to the invoker by the VQueues scheduler, so the invoker's on-disk spill queue has been removed
together with its configuration options `worker.invoker.tmp-dir` and
`worker.invoker.in-memory-queue-length-limit`.

### Why This Matters

VQueues are the foundation for node-level invoker concurrency, scoped invocations, and fair
scheduling. Enabling them by default removes the operator opt-in step and guarantees all partitions
converge to the same execution model.

### Impact on Users

- On upgrade, each partition that has not yet migrated to VQueues will run the migration when its
  leader is elected. The migration includes completed invocations; partitions with a large
  completion-retention backlog may take longer to migrate.
- The VQueues migration is one-way: once a partition has migrated, it cannot be migrated back. The
  migration happens in a coordinated fashion between leaders and followers, even if followers are
  still on v1.7. It requires all other nodes in the cluster to be running v1.7.3 or higher; we
  strongly recommend upgrading the cluster to v1.7.10 before going to v1.8.0.
- Setting the removed `experimental-enable-*` flags in configuration no longer has any effect.
- Setting `worker.invoker.tmp-dir` or `worker.invoker.in-memory-queue-length-limit` (or the
  `RESTATE_WORKER__INVOKER__TMP_DIR` environment variable) no longer has any effect, and restate-server
  no longer writes invoker spill files to the temporary directory.
- The metrics `restate.invoker.concurrency_limit`, `restate.invoker.concurrency_slots.acquired` and
  `restate.invoker.concurrency_slots.released` now track the node-wide permits handed out by the
  VQueues scheduler instead of a per-partition invoker quota. `acquired - released` therefore also
  counts permits the scheduler holds for invocations about to start, not only attempts already
  running in the invoker.
- Memory-pool exhaustion now yields instead of retrying, so invocations no longer consume retry
  attempts (or hit retry policies) due to memory pressure alone.
- Scoped Virtual Object invocations, previously rejected at the HTTP ingress, Kafka ingress, and
  service-to-service call path, are now accepted.
- During a rolling upgrade, scoped requests accepted by an upgraded node may reach a partition that
  has not yet migrated to VQueues; such invocations execute without scope-based concurrency limits.
  Start using scoped requests only after the whole cluster has been fully upgraded to v1.8.
- Pausing an in-flight invocation via the Admin API now aborts the running attempt immediately
  instead of waiting for it to reach a suspension point. SDKs observe the attempt's connection
  closing abruptly, and the invocation resumes from its journal. Note that a `ctx.run` closure that
  already started keeps executing inside the SDK process, but its result can no longer be recorded
  in the journal, so it will re-execute when the invocation is resumed. A graceful (drain) pause
  variant that preserves the previous behavior is planned as a follow-up
  ([#5321](https://github.com/restatedev/restate/issues/5321)).

### Migration Guidance

- Remove any `experimental-enable-vqueues`, `experimental-enable-vqueues-migration-skip-completed`,
  `experimental-enable-invoker-yield`, or `experimental-enable-scoped-virtual-objects` entries from
  your configuration.
- Remove any `worker.invoker.tmp-dir` and `worker.invoker.in-memory-queue-length-limit` entries from
  your configuration.
- Make sure all nodes run v1.7.3 or higher before upgrading to v1.8.0; we strongly recommend
  upgrading to v1.7.10 first.
- Review `worker.invoker.concurrent-invocations-limit`: with VQueues, the limit applies per node
  instead of per partition processor (see the corresponding release note about the new default).

#### Rolling back to v1.7

Rolling back from v1.8 to a recent v1.7 release (v1.7.3 or higher, ideally v1.7.10) remains
possible after partitions have migrated — the migration itself is one-way, but v1.7 can operate
migrated partitions. However, v1.7 still gates the now-always-on behavior behind its experimental
flags, so rolled-back nodes must run with them set:

```toml
experimental-enable-vqueues = true
```

Without it, a rolled-back node rejects scoped invocations at the ingress and applies the invoker
concurrency limit per partition processor again. Add `experimental-enable-scoped-virtual-objects =
true` as well if you use scope on Virtual Object targets, and
`experimental-enable-invoker-yield = true` to keep the yield-on-memory-pressure behavior.

#### Reducing migration downtime for large invocation histories

If you are concerned about how long the migration may take — as a guideline, if your partitions hold
more than 10 million invocations in their histories — you can stage the migration to avoid migrating
the completed-invocation backlog:

1. First upgrade the cluster to v1.7.10 and enable VQueues with the skip-completed migration:

   ```toml
   experimental-enable-vqueues = true
   experimental-enable-vqueues-migration-skip-completed = true
   ```

2. After another rolling restart, the cluster migrates only the "running" invocations. Completed
   invocations keep their existing status and fall out of retention over time — check your retention
   settings to know how long this takes.

3. Once the completed backlog has drained, disabling
   `experimental-enable-vqueues-migration-skip-completed` or upgrading to v1.8 completes the full
   migration with much less downtime, since few or no completed invocations remain to migrate.
