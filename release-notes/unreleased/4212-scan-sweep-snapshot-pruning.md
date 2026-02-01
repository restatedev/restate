# Release Notes for PR #4212: Scan-sweep snapshot pruning

## Improvement

### What Changed

Added a scan-and-sweep cleanup mechanism that automatically prunes orphaned SST files and snapshot directories from the object store. The system scans for and deletes files that are no longer referenced by any retained snapshot after every successful snapshot upload.

A new configuration option `experimental-orphan-cleanup` (default: `true`) allows disabling the cleanup if the S3 LIST overhead is a concern.

### Why This Matters

When using incremental snapshots with retention policies, certain edge cases (crashes during upload, failed cleanups, etc.) could leave orphaned SST files in the shared `ssts/` directory. These files consume storage but are never cleaned up by normal retention. The new scan-sweep mechanism ensures these orphans are eventually removed.

### What Gets Scanned

Explicit regex patterns determine which files are eligible for orphan sweep. Only files matching these exact patterns under `{partition_id}/` are considered:

| Pattern | Regex | Description |
|---------|-------|-------------|
| `ssts/{hex}.sst` | `^ssts/[0-9a-f]+\.sst$` | Content-addressed SST files (xxh3-128 hash) |
| `lsn_{lsn}-snap_1{base62}/metadata.json` | `^lsn_\d{20}-snap_1[A-Za-z0-9]+/metadata\.json$` | Snapshot metadata |
| `lsn_{lsn}-snap_1{base62}/{num}.sst` | `^lsn_\d{20}-snap_1[A-Za-z0-9]+/\d+\.sst$` | RocksDB SST files |

**Never swept** (safe for other uses):
- `latest.json` - pointer file
- SST files with non-hex names (e.g., `legacy_name.sst`)
- Snapshot directories with non-standard IDs (e.g., `snap1` instead of `snap_1...`)
- Any files not matching the strict patterns above

### Safety Measures

- **Pattern-based boundary**: Only files matching explicit regex patterns are considered for deletion. This provides an auditable safety boundary—unexpected files are never touched.
- Only files older than 24 hours are considered for deletion
- Validates all retained snapshots before deleting any shared SSTs
- Aborts scan if any retained snapshot's metadata cannot be read (prevents data loss from transient failures)

Performance optimizations:
- Referenced SST keys are computed once per cleanup and reused for all snapshot deletions and the orphan sweep
- Avoids redundant metadata fetches from the object store

### Impact on Users

- **Existing deployments**: No configuration changes required; orphan cleanup happens automatically after every snapshot upload
- **Without retention configured**: When `experimental-num-retained` is not set, the retained set is effectively just the latest snapshot. Older `lsn_*` snapshot directories will be cleaned up once they exceed the 24-hour age threshold.
- **New configuration**: Set `worker.snapshots.experimental-orphan-cleanup: false` to disable orphan cleanup if the S3 LIST overhead (~11 API calls/partition/cleanup) causes issues
- **Observability**: New metrics available:
  - `restate.partition_store.snapshots.orphan_scan.total` - count of scan operations
  - `restate.partition_store.snapshots.orphan_scan.failed.total` - count of scan failures (transient errors, missing metadata)
  - `restate.partition_store.snapshots.orphan_files_deleted.total` - count of orphaned files deleted

### Related Issues

- [#4212](https://github.com/restatedev/restate/pull/4212): Add scan-and-sweep pruning of orphaned snapshot objects
- [#3942](https://github.com/restatedev/restate/pull/3942): Snapshot retention
- [#4205](https://github.com/restatedev/restate/pull/4205): Incremental snapshots
