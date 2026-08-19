# Release Notes for Issue #5193: Disable the VQueue Scheduler

## New Feature

### What Changed

Workers can now be started without running their VQueue scheduler:

```toml
[worker]
disable-scheduler = true
```

The worker continues processing partitions and can perform partition migrations, but it does not
produce new VQueue run or yield decisions. Decisions committed before the disabled worker became
leader can still be applied during replay. Other VQueue invocations and state mutations remain in
their current inbox or running stages until scheduling is re-enabled. Legacy invocations on
partitions that have not migrated to VQueues are unaffected.

### Impact on Users

The option defaults to `false`, preserving existing behavior. Configure it consistently on every worker that can lead
a partition.

The option only affects workers on which it is configured. Stop or isolate incoming traffic before
restarting every worker with scheduling disabled. Scheduler and user-limit introspection return no
rows for partitions led by those workers while the scheduler is disabled.

### Migration Guidance

After validating a migration, set `disable-scheduler = false` and restart the workers to resume
VQueue scheduling. The scheduler then recovers persisted running entries before processing queued
work.

### Related Issues

- [#5193](https://github.com/restatedev/restate/issues/5193)
