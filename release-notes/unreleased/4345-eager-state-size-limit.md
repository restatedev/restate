# Release Notes for Issue #4345: Eager state size limit and internal unification

## New Feature / Deprecation

### What Changed

1. **Eager state size limit**: Added a new configuration option `eager-state-size-limit` to limit the
   amount of state sent eagerly to service endpoints in the StartMessage.

2. **Internal unification of eager state controls**: The three independent eager-state controls
   (`disable_eager_state`, `enable_lazy_state`, and `eager_state_size_limit`) have been unified
   internally into a single `eager_state_size_limit` field that flows through the schema and invoker.

3. **Deprecated `disable_eager_state`**: The private config option `disable_eager_state` is deprecated
   in favor of setting `eager-state-size-limit` to `0`. Existing configs using `disable_eager_state: true`
   continue to work and are internally treated as `eager_state_size_limit = 0`.

4. **Schema-level `eager_state_size_limit`**: The new field is now part of the per-service and
   per-handler schema metadata, laying the groundwork for future per-handler configuration via
   the endpoint manifest and Admin API. Currently it is not user-configurable and defaults to `None`
   (use server-level setting).

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
- **`disable_eager_state` users**: The option still works but is deprecated. Consider migrating to
  `eager-state-size-limit = "0"` for the same behavior.

### Related Issues

- #4344 - Stream state entries to service endpoints (longer-term solution)
