# `scheduled_start_at` shows when queried on its own

## Bug Fix

### What Changed
In `sys_invocation` and `sys_invocation_status`, `scheduled_start_at`, the time a delayed invocation
is due to run, now shows whenever a query selects it. Since v1.5.0 it was empty for scheduled,
inboxed and completed invocations unless the query also selected `scheduled_at`, or all columns
with `SELECT *`.

### Impact on Users
- Queries such as `SELECT id, scheduled_start_at FROM sys_invocation` show each invocation's start
  time instead of nothing, and filters such as `WHERE scheduled_start_at < now()` match the
  invocations they should.
- Queries that also select `scheduled_at` show the same values as before.

### Migration Guidance
None.
