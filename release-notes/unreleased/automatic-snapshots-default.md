# Configuring a snapshot destination now enables automatic snapshots

## Behavioral Change

### What Changed

Setting `worker.snapshots.destination` is now enough to get automatic partition snapshots. When
neither `snapshot-interval` nor `snapshot-interval-num-records` is set, Restate snapshots a
partition once at least an hour **and** at least 100,000 log records have passed since its
previous snapshot.
Previously, a destination without either setting only allowed snapshots created manually with
`restatectl snapshots create`.

### Why This Matters

In a multi-node cluster, the log is only trimmed up to the latest snapshot. With a destination but
no snapshot interval, no snapshots were created, so the log was never trimmed and kept growing on
the log servers, without any warning.

### Impact on Users

- Deployments that set a destination but neither interval option start uploading snapshots to
  the destination after the upgrade, and multi-node clusters start trimming their logs. Expect
  the first snapshots within about an hour of the upgrade, for partitions that have received at
  least 100,000 records since their previous snapshot.
- Deployments that set `snapshot-interval` or `snapshot-interval-num-records` keep their settings.
  If only one of them is set, the other one stays unset.
- Deployments without a destination are not affected.
- `snapshot-interval = "0"` now disables automatic snapshots. Previously, it created snapshots
  continuously.

### Migration Guidance

No action is needed. To keep creating snapshots only manually, disable automatic snapshots:

```toml
[worker.snapshots]
snapshot-interval = "0"
```

In a multi-node cluster, the log is then only trimmed up to the snapshots you create with
`restatectl snapshots create`.
