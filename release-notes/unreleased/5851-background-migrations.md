# Release Notes for PR #5851: Background migrations of partition stores

## New Feature

### What Changed
A partition now builds its query indexes and statistics in the background while it keeps
processing commands: the backfill runs in small slices between the partition processor's turns,
yields to the processor's other work, and resumes where it left off after a restart. Two new
options under `[worker.storage]` control it:

- `disable-background-migrations` (default `false`) pauses the backfills. Indexes and statistics
  keep being maintained for the entries written from then on, and the ones already complete stay
  complete; pending backfills wait until the option is removed.
- `background-migrations-throttling` (unset by default) caps the entries a partition replica's
  backfills examine, as a token bucket with one token per entry. The backfills already yield to
  the partition's commands; this limit further bounds their I/O and CPU use, at the cost of a
  proportionally longer backfill.

Both take effect when partition processors start.

### Why This Matters
Enabling an index or a statistic on a partition with many entries must not stop the partition from
serving, and operators need a way to pause or slow the work on a loaded cluster.

### Impact on Users
- Nothing runs in the background until an index or a statistic is enabled on the partition.
- With the defaults, backfills take about a tenth of the processor's time while it is busy and up
  to half while it is idle.

### Migration Guidance
No action needed. To pause backfills on a loaded cluster:

```toml
[worker.storage]
disable-background-migrations = true
```

To cap them at, for example, a thousand entries per second with a burst of five thousand:

```toml
[worker.storage]
background-migrations-throttling = { rate = "1000/s", capacity = 5000 }
```

### Related Issues
- PR #5851: Drive resumable walks in the background
- PR #5852: Run background migrations between processor turns
