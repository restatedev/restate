# Release Notes for PR #5283: Defer orphaned journal index cleanup

## Behavioral Change

### What Changed

Restate 1.7.9 disables the one-time cleanup of orphaned journal completion-id (`jc`) index
entries. Normal journal deletion continues to remove its completion-id index entries.

### Why This Matters

The existing completion marker cannot account for entries created after rolling back to Restate
1.6. If cleanup completed before a rollback, Restate 1.6 could create new orphaned entries and a
subsequent Restate 1.7 startup would skip cleanup because the marker was already present.

Cleanup is deferred until Restate 1.8, where it can safely account for this rollback scenario.

### Impact on Users

Restate 1.7.9 does not scan for or delete historical orphaned completion-id index entries during
partition startup. No configuration or user action is required.

### Migration Guidance

Upgrade to Restate 1.8 once its revised cleanup is available to reclaim historical orphaned index
entries.

### Related Issues

- Issue #5238
- PR #5283
