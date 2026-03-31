# Release Notes for Issue #4519: Fix orphaned journal index entries

## Bug Fix

### What Changed

Fixed a bug where journal index entries were not cleaned up when journals were deleted,
causing them to accumulate over time and waste disk space.

A one-time background cleanup task now runs automatically on partition processor startup
to remove any orphaned entries that accumulated due to this bug. The cleanup runs in the
partition processor runtime and does not block partition processing.

### Impact on Users

- **Existing deployments**: A background cleanup task runs automatically on the first
  startup after upgrading. It scans for orphaned entries and deletes them without
  blocking the partition processor. The cleanup is tracked per partition store and only
  runs once. If interrupted (e.g., by a restart), it will retry on the next startup.
- **New deployments**: No impact. The bug fix prevents orphaned entries from being
  created in the first place.

### Migration Guidance

No action required. The cleanup happens automatically on upgrade.

### Related Issues

- Issue #4519
