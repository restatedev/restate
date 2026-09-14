# Release Notes: Drop support for service protocol <= v3

## Breaking Change

### What Changed
Restate no longer runs invocations against deployments that use service protocol
version 3 or lower. When such an invocation is attempted, the invoker now fails it
immediately with `RT0020` (`service <name> is exposed by the deprecated deployment
<id>, please upgrade the SDK used by the service`) instead of retrying.

### Why This Matters
Service protocol <= v3 relies on runner code that has been removed. Continuing to run
these invocations is no longer possible, so rather than retrying indefinitely they are
now failed fast and surfaced to you.

### Impact on Users
- **New deployments**: Register services with an SDK that speaks service protocol v4 or
  later. Older SDKs are rejected.
- **Existing deployments**: Any invocation still pinned to service protocol <= v3 will
  **fail** after upgrade. The invocation is not retried; it terminates with an error.

### Migration Guidance

**Before upgrading**, check whether you still have invocations pinned to service
protocol v3 or lower. The `sys_invocation` table exposes the negotiated protocol
version in the `pinned_service_protocol_version` column:

```sql
SELECT id, target, pinned_deployment_id, pinned_service_protocol_version
FROM sys_invocation
WHERE pinned_service_protocol_version <= 3;
```

> Note: `pinned_service_protocol_version` is only set after the first journal entry has
> been stored for an invocation, so newly created invocations that have not started yet
> will not appear here.

If the query returns rows, drain or complete those invocations before upgrading, or
upgrade the SDK behind the affected deployment so new attempts negotiate protocol v4+.

**After upgrading**, invocations that failed with this error can be re-run from the
beginning using **restart-as-new**, which starts a fresh invocation from scratch (the
new invocation records the original in the `restarted_from` column of `sys_invocation`).

### Related Issues
- Auto-fail invocations using service protocol <= 3
- Remove service protocol runner <= v3
