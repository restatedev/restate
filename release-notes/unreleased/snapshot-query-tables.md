# Release Notes: More tables available when querying snapshots

## Behavioral Change / New Feature

### What Changed

Offline snapshot queries now expose `sys_vqueue_entry_status`, allowing you to
inspect persisted queue-entry status.

The live-state tables `sys_scheduler` and `sys_user_limits` are also available,
but return no rows: snapshots do not contain live scheduler or limit-counter
state. Like `sys_invocation_state`, these tables can be referenced in offline
queries without a table-not-found error.

### Impact on Users

- Queries against `sys_invocation` continue to include persisted invocations,
  with unavailable live-state columns set to null.
- `sys_service`, `sys_deployment`, and `sys_rules` remain unavailable offline
  because partition snapshots do not contain cluster metadata.
- No configuration changes are required.
