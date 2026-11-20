# `sys_invocation` shows the canonical invocation ID

## New Feature

### What Changed

`sys_invocation` gains the `canonical_id` column that `sys_invocation_status` has had since
v1.8.1: the invocation ID followed by `_` and the sequence number of the record that created the
invocation, such as `inv_..._42`. It is NULL where that sequence number isn't known.

### Why This Matters

Queries on `sys_invocation` can now match an invocation to its entry in `sys_vqueue_entry_status`
or `sys_vqueues` by `canonical_id` without joining `sys_invocation_status` for it.

### Impact on Users

- The column comes right after `id`. Queries that select columns by name are unaffected; tools that
  read `SELECT *` results by position need to account for it.
