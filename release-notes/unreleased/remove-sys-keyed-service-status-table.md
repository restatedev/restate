# Release Notes: Remove the `sys_keyed_service_status` SQL table

## Breaking Change

### What Changed

The `sys_keyed_service_status` SQL introspection table has been removed. It exposed the legacy
Virtual Object/Workflow lock state that predates VQueues. With VQueues now enabled unconditionally,
this state is tracked in the locks table and is exposed through `sys_locks`, which replaces
`sys_keyed_service_status`.

### Impact on Users

- Queries against `sys_keyed_service_status` fail with a "table not found" error.
- The table is no longer included in `restate-doctor snapshot` SQL exports.

### Migration Guidance

Replace `sys_keyed_service_status` with `sys_locks`. The columns map as follows:

| `sys_keyed_service_status`   | `sys_locks`                                                   |
|------------------------------|---------------------------------------------------------------|
| `service_name`, `service_key` | `lock_name` (formatted as `service/key`); `scope` for scoped Virtual Objects |
| `invocation_id`              | `acquired_by`                                                 |
| —                            | `acquired_at` (timestamp of lock acquisition, new)            |

```sql
-- Old
SELECT service_name, service_key, invocation_id FROM sys_keyed_service_status;

-- New
SELECT scope, lock_name, acquired_by, acquired_at FROM sys_locks;
```
