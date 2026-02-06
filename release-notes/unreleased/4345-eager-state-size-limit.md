# Release Notes for Issue #4345: Eager state size limit

## New Feature / Deprecation

### What Changed

1. **Eager state size limit**: Added a new configuration option `eager-state-size-limit` to control
   the amount of state sent eagerly to service endpoints in the StartMessage.

2. **Deprecated `disable_eager_state`**: The private config option `disable_eager_state` is deprecated
   in favor of `eager-state-size-limit = "0"`. Existing configs using `disable_eager_state: true`
   continue to work and are internally treated as `eager-state-size-limit = "0"`.

### Why This Matters

When eager state is enabled (the default for Virtual Objects and Workflows), the server sends all
state entries to the service endpoint in the StartMessage. For services with large state, this forces
the deployment to hold the entire state in memory, which can be problematic for the service endpoint.

This new option allows operators to cap the eager state size. When exceeded, the server sends partial
state and the service fetches remaining keys lazily on demand, reducing memory pressure on the
deployment.

### Configuration

```toml
[worker.invoker]
# Maximum total size of state entries to send eagerly.
# Set to "0" to disable eager state entirely (equivalent to lazy state).
# Defaults to message-size-limit if unset, and is always clamped to message-size-limit.
eager-state-size-limit = "10MB"
```

### Impact on Users

- **Default behavior unchanged**: If unset, eager state is capped at the message size limit (existing behavior for most users)
- **When limit is set**: Services with state exceeding the limit will receive partial state and
  use `GetEagerState`/`GetEagerStateKeys` commands to fetch remaining keys lazily
- **SDK compatibility**: All SDKs already support partial state — no changes required
- **`disable_eager_state` users**: The option still works but is deprecated. Migrate to
  `eager-state-size-limit = "0"` for the same behavior.

### Related Issues

- #4344 - Stream state entries to service endpoints (longer-term solution)
