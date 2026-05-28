# Release Notes: Expose active partition processor features

## New Feature

### What Changed
The set of state-machine features currently enabled on each partition processor is now
observable through `restatectl` and the `partition_state` SQL system table.

- `restatectl partition list` shows a new `FEATURES` column listing the enabled feature
  names (comma-separated), or `-` when no features are enabled.
- The `partition_state` table gains a new `enabled_features` column of type `List<Utf8>`,
  queryable with the standard DataFusion array functions.

The information is surfaced via a new `repeated string enabled_features` field on
`PartitionProcessorStatus`. Carrying feature names as strings (rather than typed
booleans) means older `restatectl` clients can render features they don't recognise.

### Usage
```sql
-- Is vqueues enabled on partition 0?
SELECT array_has(enabled_features, 'vqueues') AS vqueues_enabled
FROM partition_state
WHERE partition_id = 0;

-- Which partitions have vqueues enabled?
SELECT partition_id
FROM partition_state
WHERE array_has(enabled_features, 'vqueues');

-- Partitions with no features enabled
SELECT partition_id
FROM partition_state
WHERE cardinality(enabled_features) = 0;
```
