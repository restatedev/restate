# Release Notes: Manual Pause and Resume for Invocations

## New Feature

### What Changed

You can now manually **pause** running invocations and **resume** them later, optionally on a different deployment. This provides explicit control over invocation execution independent of the automatic retry mechanism.

### Why This Matters

- **Debugging**: Pause invocations stuck in retry loops to investigate the root cause without losing progress
- **Managed deployments**: Pause and resume capabilities integrate with deployment flows. See the [deployment flows documentation](https://docs.restate.dev/services/deployment/overview) for details.
- **Resource management**: Temporarily stop invocations consuming resources during maintenance
- **Investigation**: Pause to examine journal state before deciding to resume, restart-as-new, or kill

### Impact on Users

- **New CLI commands**: `restate invocations pause` and `restate invocations resume`
- **New API endpoints**: `PATCH /invocations/{id}/pause` and `PATCH /invocations/{id}/resume`
- **Default behavior**: By default, invocations are now paused (not killed) when `max-attempts` is exhausted. This can be configured via `on-max-attempts` in the retry policy.

### Usage

**UI:**

Pause and resume invocations directly from the Restate UI invocation detail page. See the [managing invocations documentation](https://docs.restate.dev/services/invocation/managing-invocations#pause) for more details.

**CLI:**
```bash
# Pause a running invocation
restate invocations pause <invocation_id>

# Pause all running invocations matching a query
restate invocations pause MyService/myHandler

# Resume a paused invocation
restate invocations resume <invocation_id>

# Resume on a different deployment
restate invocations resume <invocation_id> --deployment latest
restate invocations resume <invocation_id> --deployment <deployment_id>
```

**REST API:**

For REST API details, see the Admin API documentation for [resuming](https://docs.restate.dev/admin-api/invocation/resume-an-invocation) and [pausing](https://docs.restate.dev/admin-api/invocation/pause-an-invocation) invocations.

**Resume deployment options:**
- `latest`: Use the latest deployment for the service
- `keep`: Keep the currently pinned deployment
- `<deployment_id>`: Use a specific deployment ID

### Behavior Details

When an invocation is **paused**:
- The current execution attempt is gracefully terminated
- Any pending retry timer is cancelled
- The invocation will **not** retry or execute until explicitly resumed

When an invocation is **resumed**:
- Execution restarts from the last journal checkpoint
- If a deployment is specified, the invocation is redirected to that deployment

**Constraints:**
- Only `running` or `backing-off` invocations can be paused
- Paused and suspended invocations can be resumed
- If a cancel signal arrives while paused, the invocation automatically resumes to process the cancellation

### Related Issues

- [#3676](https://github.com/restatedev/restate/pull/3676): Pause and Resume
- [#3881](https://github.com/restatedev/restate/pull/3881): Manual pause an invocation
- [#3948](https://github.com/restatedev/restate/pull/3948): Add --deployment option to invocation resume CLI command
