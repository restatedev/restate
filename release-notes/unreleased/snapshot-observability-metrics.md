# Release Notes: Partition snapshot freshness and upload metrics

## New Feature

### What Changed

Partition snapshot freshness and upload progress can now be monitored per partition and per node.

| Metric | Type | Answers |
| --- | --- | --- |
| `restate.partition.latest_snapshot.age.seconds{partition}` | gauge | How long ago the latest snapshot of a partition was created ("partition 118 has not snapshotted since X"). |
| `restate.partition.latest_snapshot.lsn_lag{partition}` | gauge | How many log records the partition leader has applied since its latest snapshot. |
| `restate.partition.snapshot_in_progress{partition}` | gauge | Whether this node is currently snapshotting the partition (1 or 0). |
| `restate.num_active_snapshots` | gauge | Snapshots in progress on this node, automatic or requested. |
| `restate.partition_store.snapshots.export.active` | gauge | Snapshot exports currently running on this node. |
| `restate.partition_store.snapshots.export.queued` | gauge | Snapshot exports waiting for an export concurrency slot. |
| `restate.partition_store.snapshots.upload.bytes.total` | counter | Snapshot data uploaded, updated as each part completes; its rate is the upload throughput. |

The per-partition gauges are reported by the node that currently leads the partition, once a latest
snapshot is known (they require a configured snapshot repository). A node that stops leading a
partition reports NaN age and LSN lag for it, and 0 in progress. When aggregating age or LSN lag
across nodes, use `max by (partition) (...)`, which picks the current leader's value over NaN; `sum`
and `avg` return NaN once any node has stopped leading a partition. While a partition has no snapshot at all, its age is NaN
and its LSN lag counts from the start of the log.

### Impact on Users

No behavior change. The new series add one set of three gauges per partition led by each node.
