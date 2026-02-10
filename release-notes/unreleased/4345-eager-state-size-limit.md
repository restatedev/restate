# Release Notes for Issue #4345: Add configurable eager state size limit

## New Feature

### What Changed

Added a new configuration option `eager-state-size-limit` to limit the amount of state sent eagerly 
to service endpoints in the StartMessage.

### Why This Matters

When eager state is enabled (the default for Virtual Objects and Workflows), the server sends all 
state entries to the service endpoint in the StartMessage. For services with large state, this forces
the deployment to hold the entire state in memory, which can be problematic for the service endpoint.

This new option allows operators to cap the eager state size. When exceeded, the server sends partial
state and the service fetches remaining keys lazily on demand, reducing memory pressure on the 
deployment. As a side effect, this also reduces memory usage on the server side during state collection.

### Configuration

```toml
[worker.invoker]
# Maximum total size (in bytes) of state entries to send eagerly
# When exceeded, partial state is sent and the service fetches remaining state lazily
eager-state-size-limit = "10MB"
```

### Impact on Users

- **Default behavior unchanged**: If unset, all state entries are sent eagerly (existing behavior)
- **When limit is set**: Services with state exceeding the limit will receive partial state and 
  use `GetEagerState`/`GetEagerStateKeys` commands to fetch remaining keys lazily
- **SDK compatibility**: All SDKs already support partial state - no changes required

### Related Issues

- #4344 - Stream state entries to service endpoints (longer-term solution)
