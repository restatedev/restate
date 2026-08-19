# Release Notes for Issue #5193: Disable Invocation Execution

## New Feature

### What Changed

Workers can now be started without executing service invocations:

```toml
[worker.invoker]
disable-invocation-execution = true
```

The worker continues processing partitions and can perform partition migrations, but it does not
start or resume service invocation attempts. VQueue state mutations remain enabled, subject to
their existing queue ordering with service invocations.

### Impact on Users

The option defaults to `false`, preserving existing behavior. It is evaluated when the worker
starts, so changing it requires a restart. Configure it consistently on every worker that can lead
a partition.

The option only affects workers on which it is configured. Stop or isolate incoming traffic before
restarting every worker with invocation execution disabled.

### Migration Guidance

After validating a migration, set `disable-invocation-execution = false` and restart the workers to
resume invocation execution.

### Related Issues

- [#5193](https://github.com/restatedev/restate/issues/5193)
