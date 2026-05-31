# Release Notes for Issue #4838: Fix orphan-cleanup poisoning the partition store after a snapshot restore

## Bug Fix

### What Changed

Fixed a crash loop that could take down a node after upgrading to the version that
introduced the one-time orphaned journal completion-id (`jc`) index cleanup (#4519).

The cleanup ran as a detached background task on a clone of the partition store. If the
partition processor restarted and restored from a snapshot (which drops and re-imports the
partition's column family) while that cleanup was still running, the cleanup would issue a
delete against the dropped column family. Because all partitions share a single RocksDB
instance, the resulting fatal background error put the entire database into read-only mode
and crash-looped every partition on the node. Each restart then re-downloaded the snapshot
into a fresh staging directory without removing the previous one, eventually filling the
data volume.

Three changes address this:

1. **Lifecycle.** The cleanup is now awaited before the partition processor starts, so it
   can no longer outlive the partition attempt or run concurrently with a snapshot restore.
   It still runs on a blocking thread (it does not stall the async runtime) and is checked
   for cancellation on every entry so shutdown remains prompt.
2. **Defense in depth.** The cleanup's deletes now tolerate a missing column family, so a
   stray write to a dropped CF is ignored instead of poisoning the shared database.
3. **Staging cleanup.** Downloaded snapshot staging directories are now removed on every
   error/abort path (not only after a successful import), and any directories left behind by
   a previous run are swept on startup, so a failing restore can no longer leak a copy of
   the snapshot per retry.

### Impact on Users

- **Existing deployments**: No action required. The one-time `jc` cleanup still runs
  automatically on the first startup after upgrade and is still tracked per partition store
  so it only runs once; it is now performed before the partition starts serving, which may
  add a short, one-time startup delay on stores that have orphaned entries to remove.
- **New deployments**: No impact.

### Migration Guidance

No action required.

### Related Issues

- Issue #4838
- Issue #4519
