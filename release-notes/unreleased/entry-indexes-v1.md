# Release Notes: Secondary indexes of VQueue entries (experimental)

## New Feature (experimental)

### What Changed
The experimental option `experimental-enable-indexes-v1` makes each partition maintain secondary
indexes over its VQueue entries, so that queries can find the entries of a service, or of one
virtual object, in a stage by newest transition first or by earliest next transition first,
without scanning every entry. Entries that existed before are added by a background backfill;
until it completes, reads of the entry indexes fail on that partition. The busy-vqueue index of
the same option is not backfilled: a queue written before the option was enabled is missing from
it until its next change.

The indexes stay maintained across restarts, also after the option is removed.

### Impact on Users
- Nothing changes unless the option is set. The set of indexes is provisional and may change in a
  later release.
- Enabling the option raises the partition's minimum Restate version to v1.8.2, for good: a node
  that has ever run with it cannot be rolled back to an older version.

### Migration Guidance
To enable the indexes:

```toml
experimental-enable-indexes-v1 = true
```

### Related Issues
- PR #5848: Declare the statistics and maintain registered indexes on writes
