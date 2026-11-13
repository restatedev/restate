# Release Notes: Records of background migrations

## New Feature

### What Changed
Each partition replica now keeps a record of its background migrations, the
backfills that build query indexes and statistics. A record shows when the
migration was created, started and finished, how many entries it examined and
filled, how much work time it took, and its failures with the last error.
Unfinished migrations are always recorded. Finished ones are kept up to a
limit, set by a new option under `[worker.storage]`:

- `migration-job-history-limit` (default `100`): how many finished migrations
  each partition replica keeps a record of. When one more finishes, the record
  of the oldest finished migration is dropped. `0` keeps none.

The option takes effect when partitions open.

### Why This Matters
Operators need to see how an index or statistic build is going, whether it is
stuck, and what it did once it finished.

### Impact on Users
- Records of unfinished migrations are always kept, so a stuck or failing
  backfill stays visible until it finishes.
- The records are diagnostics only: they never decide whether an index can be
  read, and losing them, for example after a downgrade, loses no data.

### Migration Guidance
No action needed. To keep more history, for example:

```toml
[worker.storage]
migration-job-history-limit = 500
```
