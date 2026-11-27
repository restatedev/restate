# Check the partition features of the latest snapshots

## New Feature

### What Changed

Partition snapshots record which partition features their data has. You can now see them for each
partition's latest snapshot, and check them before an upgrade:

- `restatectl snapshots check-features <FEATURE>...` checks that the latest snapshot of every
  partition has the given features. It lists the partitions whose latest snapshot lacks one of
  them, or that no worker node reports, and fails if there are any.
- The `latest_snapshot_features` column of `partition_state` lists the features of the
  partition's latest snapshot, as each replica reports it.

```sql
SELECT partition_id, plain_node_id, latest_snapshot_features
FROM partition_state
WHERE NOT array_has(latest_snapshot_features, 'journal_v1_retired')
```

### Impact on Users

A release that requires a partition feature restores a partition that lacks it from its latest
snapshot with the feature. Its release notes name the features to check before upgrading to it.

The features are empty for partitions without a snapshot repository or snapshot, and for snapshots
taken by servers older than v1.8.2. Create new snapshots with `restatectl snapshots create`.
