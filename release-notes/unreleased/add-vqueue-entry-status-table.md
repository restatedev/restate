# Release Notes: Add `sys_vqueue_entry_status` Table

## New Feature

### What Changed
Added a `sys_vqueue_entry_status` SQL table that exposes vqueue entry status-header fields from the partition store.

### Why This Matters
Operators can inspect the authoritative vqueue entry status header directly, including the entry id, vqueue id, stage, status, scheduling key fields, entry statistics, wait statistics, deployment, memory, and retry metadata.

### Impact on Users
- Existing deployments gain a new introspection table after upgrade.
- No migration or configuration changes are required.

### Migration Guidance
No action required.
