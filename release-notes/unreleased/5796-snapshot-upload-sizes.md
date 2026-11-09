# New options to tune snapshot upload requests

## New Feature

### What Changed

Two new options in the `[worker.snapshots]` section control how snapshot files are uploaded to the
snapshot destination:

- `upload-part-size`: the size of each part when a file is uploaded in multiple parts.
  Files that would need more than 10,000 parts automatically use larger parts.
- `upload-single-put-threshold`: files smaller than this are uploaded with a single request instead
  of in multiple parts.

Both default to `5 MiB`, which is what Restate has used so far.

### Why This Matters

Every part of a multipart upload, and the calls that start and finish it, is a request to the
object store. Larger parts and a higher threshold mean fewer requests, which lowers the request
cost of snapshots on S3 and GCS.

### Impact on Users

- Nothing changes unless you set the options.
- `upload-part-size` must be at least `5 MiB`. `upload-single-put-threshold` must be at
  least the part size and at most `5 GiB`. Invalid values are rejected when the configuration is
  loaded.
- A file below the threshold is read into memory in full while it is uploaded. Worst-case memory
  use is the threshold times the number of snapshots uploading at once, which is limited by
  `automatic-snapshot-concurrency-limit` plus any snapshots requested with `restatectl`. We
  recommend thresholds of at most `256 MiB`.
- Each upload in progress also buffers one part in memory.

### Migration Guidance

No action is needed. To make fewer requests, for example:

```toml
[worker.snapshots]
upload-part-size = "32 MiB"
upload-single-put-threshold = "128 MiB"
```
