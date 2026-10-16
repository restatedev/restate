# New `canonical_id` column in `sys_invocation_status`

## New Feature

### What Changed
Restate now records, for every new invocation, the point in the log where it was created.
The `sys_invocation_status` table shows it in a new `canonical_id` column: the invocation
ID followed by that position, e.g. `inv_..._42`.

### Impact on Users
- `canonical_id` is set for invocations created after upgrading to this version.
- Invocations created before the upgrade get a `canonical_id` the next time they start
  running, suspend, pause or resume. Until then, they show a NULL `canonical_id`.
- No action is needed.
