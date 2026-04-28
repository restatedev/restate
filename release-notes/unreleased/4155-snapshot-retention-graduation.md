# Release Notes for Issue #4155: Graduate snapshot retention out of experimental

## Behavioral Change / Breaking Change

### What Changed

The opt-in snapshot retention feature introduced in v1.6 is now a stable, always-on
part of the snapshot repository:

1. **Config option renamed and made non-optional.** The `worker.snapshots.experimental-num-retained`
   option (an optional integer, disabled by default) has been replaced by
   `worker.snapshots.num-retained`, a non-optional integer with a default of `1`.
2. **`latest.json` is always written in V2 format.** Restate no longer gates the
   on-disk format on the running version or on whether retention was opted in.
   Every snapshot upload writes a V2 `latest.json` pointer.
3. **Legacy V1 pointers continue to be readable** and are upgraded to V2 the next
   time a snapshot is uploaded for that partition — no action is required from
   operators on existing repositories.

### Why This Matters

Restate is now responsible for cleaning up snapshots that are no longer used
for bootstrapping partition processors because the log has been trimmed beyond
their archived lsn. Users can configure how many snapshots should be retained
before deleting them. This will also prevent unbounded snapshot storage growth
as only `num-retained` snapshots per partition are stored.

### Impact on Users

- **Existing repositories**: no migration required. The first upload after the
  upgrade rewrites `latest.json` from V1 to V2 in place. Snapshot data files
  themselves are unchanged.
- **Existing configurations**: any deployment that explicitly set
  `experimental-num-retained` in its `restate.toml` (or via the
  `RESTATE_WORKER__SNAPSHOTS__EXPERIMENTAL_NUM_RETAINED` env var) **must rename
  the key to `num-retained`** before upgrading. Restate will ignore the old key.
- **Default behavior**: `num-retained = 1` keeps exactly one snapshot per partition 
  and cleans up older snapshots which are no longer usable due to log trimming.
- **Downgrade**: rolling back to a binary older than v1.6 is not supported once
  V2 pointers have been written. Rolling back to v1.6.x is safe.

### Migration Guidance

If your config sets the experimental key:

```toml
[worker.snapshots]
experimental-num-retained = 3
```

rename it to:

```toml
[worker.snapshots]
num-retained = 3
```

If your config does not set the key, no action is needed.

### Related Issues

- #3942 — original experimental introduction in v1.6
