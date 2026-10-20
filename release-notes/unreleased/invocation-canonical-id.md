# New `canonical_id` column in `sys_invocation_status`

## New Feature

### What Changed
Restate now records, for every new invocation, the point in the log where it was created.
The `sys_invocation_status` table shows it in a new `canonical_id` column: the invocation
ID followed by that position, e.g. `inv_..._42`.

### Impact on Users
- `canonical_id` is set for invocations created after upgrading to this version.
- Invocations that already exist when upgrading from v1.7 get a `canonical_id` during the
  upgrade. Their original log position isn't known, so the ID ends with a placeholder
  (usually `_0`) instead. It still matches the `canonical_id` of the invocation's entry in
  `sys_vqueue_entry_status`.
- No action is needed.
