# Release Notes for PR #4205: Incremental snapshots

## New Feature (Experimental)

### What Changed

Restate now supports incremental snapshots, which use content-addressed SST file naming to deduplicate data across snapshots. When enabled, SST files are uploaded to a shared `ssts/` directory using their content hash (xxh3-128) as the filename. Files with identical content are automatically skipped during upload.

New configuration options:

```toml
[worker.snapshots]
# Enable incremental snapshots (default: "full")
experimental-snapshot-type = "incremental"

# destination, frequency settings as usual
```

### Why This Matters

Incremental snapshots provide significant cost savings for deployments with frequent snapshotting:

- **Reduced upload costs**: Only new or changed SST files are uploaded; unchanged files are skipped
- **Reduced storage costs**: Identical SST files are stored once regardless of how many snapshots reference them
- **Works with retention**: When combined with `num-retained`, old snapshots can be pruned while shared SSTs remain available to newer snapshots

### Bucket Layout Change

When `experimental-snapshot-type = "incremental"` is enabled, the object store layout changes:

| Mode        | SST file location                                            |
| ----------- | ------------------------------------------------------------ |
| Full        | `<partition_id>/<lsn>_<snapshot_id>/<filename>.sst`          |
| Incremental | `<partition_id>/ssts/<content_hash>.sst` (content-addressed) |

The snapshot metadata file (`metadata.json`) now includes a `file_keys` field that maps original filenames to their repository keys, enabling the repository to locate files during restore.

### Impact on Users

- **Existing deployments**: No change unless you enable `experimental-snapshot-type = "incremental"`
- **New deployments**: Opt-in feature, defaults to `full` mode
- **Enabling incremental**: Safe once every node runs this version (see Migration Guidance below); existing full snapshots continue to work
- **Mixed modes**: The repository supports both full and incremental snapshots coexisting
- **Downgrades become unsafe**: see below
- **New `check-interval` option**: `worker.snapshots.check-interval` sets how often snapshot triggers are checked. The default, `1s`, keeps the current behavior; setting it also removes the 30 to 60 second delay before the first check after startup.

### Downgrade Caveat

Incremental snapshots are marked with a new snapshot format version. A node running a version
without incremental support refuses to restore from an incremental snapshot with an
unrecognised snapshot format error, rather than failing later on missing files. Full
snapshots keep the existing format, so older nodes can still restore from them.

Once a partition has published its first incremental snapshot, do not downgrade the cluster below
this version and expect restores from that snapshot to work. If you need to downgrade, first set
`experimental-snapshot-type = "full"`, then wait until every incremental snapshot has aged out of
`num-retained` before rolling back.

### Migration Guidance

- **Enabling**: First upgrade every node in the cluster to this version, then add
  `experimental-snapshot-type = "incremental"` to your configuration. Do not enable it during a
  rolling upgrade: a node on an older version cannot restore from incremental snapshots, and
  retaining more snapshots does not help because new snapshots can rotate older full ones out
  before the upgrade finishes.
- **Reverting to full**: Set `experimental-snapshot-type = "full"`; new snapshots will use full mode, existing incremental snapshots remain readable
- **Cleanup coordination**: When using incremental mode with `num-retained`, SST cleanup is coordinated to ensure shared files are only deleted when no retained snapshot references them

### Related Issues

- [#4205](https://github.com/restatedev/restate/issues/4205): Incremental snapshots (tracking issue)
- [#4198](https://github.com/restatedev/restate/pull/4198): Add incremental snapshot support
- [#3942](https://github.com/restatedev/restate/pull/3942): Snapshot retention (companion feature)
