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
- Exception: if you already enabled `experimental_enable_vqueues` on v1.7, invocations
  created on v1.7 show a NULL `canonical_id` after the upgrade. They get one the next time
  they start running, suspend, pause or resume. Invocations that had already completed
  keep a NULL `canonical_id`.
- No action is needed.
