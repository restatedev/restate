# Release Notes for Issue #3184: Retire journal v1 (experimental)

## New Feature

### What Changed

Partitions can retire journal v1, the journal format of deployments that use a service protocol
version older than V4. A partition that retires it:

- **kills** every invocation that hasn't completed yet and is pinned to such a deployment,
- drops the journal of completed invocations whose journal is stored in journal v1; their result
  stays available.

Invocations of deployments that support service protocol V4 or newer are not affected.

### Impact on Users

Nothing changes unless you enable the option. To retire journal v1, upgrade all nodes to v1.8.2 or
newer, then enable:

```toml
experimental-enable-retire-journal-v1 = true
```

A partition retires journal v1 the next time a leader is elected on a node that has the option,
for example when its current leader's node restarts with it.

- Once a partition has retired journal v1, it stays retired, even if the option is removed.
- Once enabled, you **cannot** roll back to a Restate-server version older than v1.8.2.

### Migration Guidance

Before enabling the option, find the invocations that are pinned to a deployment using a service
protocol version older than V4 and haven't completed yet:

```sql
SELECT id, target, status, pinned_deployment_id
FROM sys_invocation
WHERE pinned_service_protocol_version < 4 AND status != 'completed'
```

Let them complete, or cancel them and invoke the services again after upgrading them to a newer
SDK and registering them as new deployments. The ones still running when the option takes effect
are killed.

### Related Issues

- Issue #3184: Deprecate service protocol <= 4
