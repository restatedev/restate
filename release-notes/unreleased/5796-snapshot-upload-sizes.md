# Fewer object store requests for snapshot uploads

## Behavior Change

### What Changed

Two new options in the `[worker.snapshots]` section control how snapshot files are uploaded to the
snapshot destination:

- `upload-part-size`: the size of each part when a file is uploaded in multiple parts.
  Files that would need more than 10,000 parts automatically use larger parts.
- `upload-single-put-threshold`: files smaller than this are uploaded with a single request instead
  of in multiple parts.

`upload-part-size` now defaults to `32 MiB`, up from the fixed `5 MiB` Restate used so far, and
`upload-single-put-threshold` defaults to the part size. Changes take effect after a restart.

### Why This Matters

Every part of a multipart upload, and the calls that start and finish it, is a request to the
object store. Larger parts and a higher threshold mean fewer requests, which lowers the request
cost of snapshots on S3 and GCS.

### Impact on Users

- Snapshot uploads make fewer requests by default. A typical 64 MiB snapshot file now takes 4
  requests instead of 15, and files under 32 MiB take one.
- Snapshot uploads use more memory: up to about `32 MiB` per upload in progress, instead of
  `5 MiB`. With the default `automatic-snapshot-concurrency-limit` of 4 that is about `128 MiB`
  per node. To keep the old behavior, set both options to `5 MiB`.
- `upload-part-size` must be at least `5 MiB`. `upload-single-put-threshold` must be at
  least the part size and at most `5 GiB`. Invalid values are rejected when the configuration is
  loaded.
- A file below the threshold is read into memory in full while it is uploaded. Worst-case memory
  use is the threshold times the number of snapshots uploading at once, which is limited by
  `automatic-snapshot-concurrency-limit` plus any snapshots requested with `restatectl`. We
  recommend thresholds of at most `256 MiB`.
- Each upload in progress also buffers one part in memory.

### Migration Guidance

No action is needed. On nodes with memory to spare, a threshold above the RocksDB SST target size
(`rocksdb-max-file-size`, default `64 MiB`) uploads most snapshot files with a single request:

```toml
[worker.snapshots]
upload-part-size = "32 MiB"
upload-single-put-threshold = "128 MiB"
```
