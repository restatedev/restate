# Release Notes for PR #5665: Metrics for in-progress snapshot transfers and their throughput

## New Feature

### What Changed
Restate now exports four new metrics for partition snapshot uploads and downloads:

- `restate_partition_store_snapshots_upload_inflight` (gauge): number of partition snapshot uploads currently in progress
- `restate_partition_store_snapshots_download_inflight` (gauge): number of partition snapshot downloads currently in progress
- `restate_partition_store_snapshots_upload_bytes_total` (counter): bytes of snapshot data uploaded to the snapshot repository
- `restate_partition_store_snapshots_download_bytes_total` (counter): bytes of snapshot data downloaded from the snapshot repository

### Why This Matters
You can now see how many snapshot transfers a node is running at any time, and plot upload and download throughput, for example with `rate(restate_partition_store_snapshots_upload_bytes_total[1m])`. This helps you spot slow or stuck transfers and size the bandwidth to your snapshot repository.

### Impact on Users
- The metrics are reported by nodes that upload or download partition snapshots. No configuration is needed.
- The byte counters count data as it is transferred, so a transfer that fails partway still contributes the bytes moved before the failure.

### Migration Guidance
None.
