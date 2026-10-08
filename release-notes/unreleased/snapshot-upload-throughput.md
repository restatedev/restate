# Release Notes: Faster, bounded partition snapshot uploads

## Behavioral Change

### What Changed

Partition snapshot uploads now run in parallel and share a node-wide budget. Previously each
snapshot uploaded one file at a time and one 5 MiB part at a time, so large partitions could take
hours to snapshot and held a snapshot slot all along.

Uploads now send several requests at once, with larger parts, across all snapshots on a node.
Snapshots in progress take turns in a shared pool, so one large partition no longer slows down the
others.

New `[worker.snapshots]` options:

| Key | Default | Purpose |
| --- | --- | --- |
| `automatic-snapshot-concurrency-limit` | `4` | Snapshots in progress on a node; automatic snapshots are only scheduled below this bound. Previously fixed at 4. |
| `upload-parallelism` | `8` | Upload requests in flight on a node, shared by all snapshots. Also bounds upload memory to `upload-parallelism * upload-part-size`. |
| `upload-part-size` | `16 MiB` | Multipart part size. Files smaller than this are uploaded in a single request. Values below 5 MiB are raised to 5 MiB. |
| `upload-max-rate-per-second` | unlimited | Caps how fast a node reads snapshot files for upload. |

New metric: `restate.partition_store.snapshots.upload.requests.active`, the upload requests in flight
on a node. If it sits at `upload-parallelism`, uploads are limited by this setting.

### Why This Matters

Snapshots that take hours to upload hold back log trimming, which keeps the log growing, and keep
already-compacted data files on disk until the upload finishes.

### Impact on Users

- **Existing deployments**: snapshots upload faster with no configuration change. Snapshot upload
  memory per node rises to at most 128 MiB at the defaults, from roughly 20-40 MiB.
- **Disk reads**: faster uploads read snapshot files from the partition store's disk faster. On
  nodes where disk bandwidth is tight, set `upload-max-rate-per-second` so uploads leave room for
  the partition store's own background work.
- **Buckets holding snapshots should expire incomplete multipart uploads.** A snapshot upload
  interrupted by a crash or shutdown can leave uploaded parts that S3 and GCS do not reclaim on
  their own. This was already true before; larger parts make it more costly to leave unset.
- No change to snapshot format, layout or compatibility.

### Migration Guidance

None required. For nodes with large partitions and spare memory and network bandwidth, for example:

```toml
[worker.snapshots]
upload-parallelism = 32
upload-part-size = "32 MiB"
upload-max-rate-per-second = "300 MiB"
```
