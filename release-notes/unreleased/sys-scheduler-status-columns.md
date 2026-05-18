# Release Notes: Expand `sys_scheduler` visibility

## New Feature

### What Changed
`sys_scheduler` now exposes scheduler state details per vqueue, not only the vqueue id.
The table includes queue depth/running counters, state fields (including scheduled/throttle metadata), a `head_entry_id` column identifying the entry currently at the head of the queue, and wait-time breakdown columns.

### Why This Matters
Operators can diagnose scheduler behavior directly from SQL without joining against other internals or relying only on metrics.

### Impact on Users
- Existing `SELECT id FROM sys_scheduler` queries keep working.
- New columns are available for deeper scheduler introspection.

### Migration Guidance
No migration is required.
