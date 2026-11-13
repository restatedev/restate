# Release Notes for PRs #5848 and #5850: Status gauges for VQueue entries

## New Feature (experimental)

### What Changed
Each partition can keep statistics of its VQueue entries: how many entries are in each stage and
status, counted by service, by deployment, and by virtual object. The counts include invocations
and state mutations, and keep completed entries until they are deleted. Two experimental options
control them:

- `experimental-enable-track-status-gauges` registers the statistics on each partition and keeps
  them up to date with every entry write. Entries that existed before are added by a background
  backfill; until it completes, queries of the statistics tables fail on that partition.
- `experimental-enable-status-gauges-reset` deletes a partition's statistics and stops maintaining
  them. While it is set, it takes precedence over the first option.

The statistics stay maintained across restarts, also after the first option is removed, until they
are reset. Whether the statistics tables are visible to SQL is controlled separately, on the query
coordinator.

### Why This Matters
Counting entries per stage and status on the fly, by scanning the entries, is too expensive for a
busy partition. Maintained counters make such queries cheap, and the backfill keeps the partition
serving while the counters are built.

### Impact on Users
- Nothing changes unless one of the options is set.
- Enabling `experimental-enable-track-status-gauges` raises the partition's minimum Restate
  version to v1.8.2, for good: a node that has ever run with it cannot be rolled back to an older
  version, also after a reset or after the option is removed.

### Migration Guidance
To enable the statistics:

```toml
experimental-enable-track-status-gauges = true
```

To delete them and rebuild from the retained entries: set `experimental-enable-status-gauges-reset
= true`, restart the nodes, then remove it and restart again with the first option set.

### Related Issues
- PR #5848: Declare the statistics and maintain registered indexes on writes
- PR #5850: Reconcile the index registry at open and backfill indexes with walks
