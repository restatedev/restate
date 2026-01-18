# Release Notes for PR #4205: Incremental snapshots

## New Feature (Experimental)

### What Changed

Restate now supports incremental snapshots, which use content-addressed SST file naming to deduplicate data across snapshots. When enabled, SST files are uploaded to a shared `ssts/` directory using their content hash (xxh3-128) as the filename. Files with identical content are automatically skipped during upload.

New configuration options:

```toml
[worker.snapshots]
# Enable incremental snapshots (default: "full")
experimental-snapshot-kind = "incremental"

# destination, frequency settings as usual
```

### Why This Matters

Incremental snapshots provide significant cost savings for deployments with frequent snapshotting:

- **Reduced upload costs**: Only new or changed SST files are uploaded; unchanged files are skipped
- **Reduced storage costs**: Identical SST files are stored once regardless of how many snapshots reference them
- **Works with retention**: When combined with `experimental-num-retained`, old snapshots can be pruned while shared SSTs remain available to newer snapshots

### Bucket Layout Change

When `experimental-snapshot-kind = "incremental"` is enabled, the object store layout changes:

| Mode        | SST file location                                            |
| ----------- | ------------------------------------------------------------ |
| Full        | `<partition_id>/<lsn>_<snapshot_id>/<filename>.sst`          |
| Incremental | `<partition_id>/ssts/<content_hash>.sst` (content-addressed) |

The snapshot metadata file (`metadata.json`) now includes a `file_keys` field that maps original filenames to their repository keys, enabling the repository to locate files during restore.

### Impact on Users

- **Existing deployments**: No change unless you enable `experimental-snapshot-kind = "incremental"`
- **New deployments**: Opt-in feature, defaults to `full` mode
- **Enabling incremental**: Safe to enable at any time; existing full snapshots continue to work
- **Mixed modes**: The repository supports both full and incremental snapshots coexisting

### Migration Guidance

- **Enabling**: Add `experimental-snapshot-kind = "incremental"` to your configuration
- **Reverting to full**: Set `experimental-snapshot-kind = "full"`; new snapshots will use full mode, existing incremental snapshots remain readable
- **Cleanup coordination**: When using incremental mode with `experimental-num-retained`, SST cleanup is coordinated to ensure shared files are only deleted when no retained snapshot references them

### Related Issues

- [#4204](https://github.com/restatedev/restate/pull/4205): Add incremental snapshot support
- [#3942](https://github.com/restatedev/restate/pull/3942): Snapshot retention (companion feature)
