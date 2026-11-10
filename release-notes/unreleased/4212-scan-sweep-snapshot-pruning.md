# Release Notes for PR #4212: Scan-sweep snapshot pruning

## Improvement

### What Changed

Restate can now delete orphaned snapshot files from the object store: SST files and snapshot directories that no retained snapshot references. Orphans are left behind by crashes, interrupted uploads and failed cleanups, and were previously never removed.

The sweep runs at most once a day per partition, after a successful snapshot. It only deletes objects older than 24 hours, and it never touches `latest.json` or files it does not recognise as snapshot files. If the metadata of any retained snapshot cannot be read, the sweep deletes nothing.

### Impact on Users

- **Incremental snapshots**: the sweep is on by default when `experimental-snapshot-type` is `incremental`.
- **Full snapshots**: the sweep is off by default. Turn it on with `worker.snapshots.experimental-orphan-cleanup = true`. Expect it to delete snapshot directories older than 24 hours left over from earlier failed or interrupted uploads.
- **Cost**: each sweep lists the partition's snapshot prefix once, so about one LIST request per partition per day, plus one DELETE per orphaned file.
- **Turning it off**: set `worker.snapshots.experimental-orphan-cleanup = false`.
- **Retention**: the sweep keeps everything referenced by the snapshots retained under `num-retained`.
- **Abandoned multipart uploads** are not visible to the sweep. Configure a lifecycle rule on the bucket to abort incomplete multipart uploads.
- **Metrics**:
  - `restate.partition_store.snapshots.orphan_scan.total`
  - `restate.partition_store.snapshots.orphan_scan.failed.total`
  - `restate.partition_store.snapshots.orphan_files_deleted.total`

### Related Issues

- [#4212](https://github.com/restatedev/restate/pull/4212): Add scan-and-sweep pruning of orphaned snapshot objects
- [#4205](https://github.com/restatedev/restate/issues/4205): Incremental snapshots
