# Canonical entry identifiers in sys_vqueues and sys_vqueue_entry_status

## New Feature

### What Changed

The `sys_vqueues` and `sys_vqueue_entry_status` SQL tables now include a `canonical_id` column containing the entry's resource ID followed by `_` and its sequence number. Equality and `IN` filters on this column support indexed lookups.

### Why This Matters

The canonical identifier distinguishes a specific incarnation of an entry.

### Impact on Users

The existing `entry_id` column retains its resource ID format and lookup behavior. Existing queries using `entry_id` require no migration.
