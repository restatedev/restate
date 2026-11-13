# Release Notes for PR #4212: Scan-sweep snapshot pruning

## Improvement

### What Changed

Restate can now delete orphaned snapshot files from the object store: SST files and snapshot directories that no retained snapshot references. Orphans are left behind by crashes, interrupted uploads and failed cleanups, and were previously never removed.

The sweep runs at most once a day per partition, after a successful snapshot. It only deletes objects older than 24 hours, and it never touches `latest.json` or files it does not recognise as snapshot files. If the metadata of any retained snapshot cannot be read, the sweep deletes nothing.

### Impact on Users

- **When it runs**: the sweep runs whenever `experimental-snapshot-type` is `incremental`, and never for full snapshots, which delete the files of a failed upload right away. There is no separate option for it.
- **Cost**: about one LIST request per partition per day, plus one more LIST for each orphaned snapshot directory it removes, and delete requests for orphaned files (up to 1,000 files per request on stores with bulk deletes, such as S3).
- **Retention**: the sweep keeps everything referenced by the snapshots retained under `num-retained`.
- **Abandoned multipart uploads** are not visible to the sweep. Configure a lifecycle rule on the bucket to abort incomplete multipart uploads.
- **Metrics**:
  - `restate.partition_store.snapshots.orphan_scan.total`
  - `restate.partition_store.snapshots.orphan_scan.failed.total`
  - `restate.partition_store.snapshots.orphan_files_deleted.total`

### Related Issues

- [#4212](https://github.com/restatedev/restate/pull/4212): Add scan-and-sweep pruning of orphaned snapshot objects
- [#4205](https://github.com/restatedev/restate/issues/4205): Incremental snapshots
