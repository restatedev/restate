# Release Notes: Enabling vqueues requires a partition with no in-flight invocations

## Behavioral Change

### What Changed

When a partition processor receives a `VersionBarrier` command that turns on
the `EnableVqueues` feature change, it now checks whether the partition holds
any pre-existing in-flight data. If it does, the partition processor refuses
to apply the change and surfaces a `MigrationRequired` error that names the
feature(s) whose application would otherwise leave that data inconsistent.

A partition is considered to hold in-flight data if either of the following is
true:

- The inbox table contains any entry (queued invocations or state mutations).
- The invocation status table contains any entry that is not `Completed`
  (i.e. `Scheduled`, `Inboxed`, `Invoked`, `Suspended`, or `Paused`). This
  transitively covers held virtual-object locks and any scheduled-invocation
  timers, since both map back to a non-`Completed` invocation status.

### Why This Matters

Enabling vqueues routes invocations, state mutations, and virtual-object locks
through a different set of storage tables. Without an explicit migration step,
pre-existing in-flight data ends up stranded on the legacy code path and
cannot be progressed once vqueues is on. This release ships the safety gate;
the migration that rewrites pre-existing in-flight data into vqueue form is
planned for a later patch release.

### Impact on Users

- **Fresh deployments** and partitions that hold only completed invocations
  (or no invocations at all) are unaffected — the barrier applies cleanly and
  vqueues turns on.
- **Existing deployments with in-flight invocations** that opt into vqueues
  via `experimental.is_vqueues_enabled` will see the affected partition halt
  with the `MigrationRequired` error in its logs. No state is lost: the apply
  transaction rolls back, the partition stays at its pre-apply state, and the
  same `VersionBarrier` envelope is re-applied automatically once the cluster
  is rolled to a server version that ships the migration.

### Migration Guidance

If the gate trips, you need to upgrade to a Restate version that supports the migration. It will be at least Restate v1.7.1.
