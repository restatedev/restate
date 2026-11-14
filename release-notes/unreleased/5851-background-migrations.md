# Release Notes for PR #5851: Background migrations of partition stores

## New Feature

### What Changed
When a query index or a statistic is enabled on a partition, the partition now builds it from the
existing entries in the background while it keeps serving. The work continues where it left off
after a restart, and the index or statistic becomes usable once it is complete. Three new options
under `[worker.storage]` control this background work:

- `disable-background-migrations` (default `false`) pauses it. Indexes and statistics stay up to
  date for entries written from then on, and the ones already complete stay usable; the ones still
  being built wait until the option is removed.
- `background-migrations-throttling` (unset by default) limits how many existing entries each
  partition replica processes per unit of time, with an optional burst. A lower rate reduces the
  disk and CPU load of the background work, and the build takes correspondingly longer.
- `background-migrations-max-slice-entries` (default `1000`) caps how many existing entries one
  slice of an index or statistic build processes. The build runs in short slices between the
  partition's commands, which wait while a slice runs, so a lower value keeps that wait shorter.

All three take effect when partition processors start. A slice's writes also stay within
`max-command-batch-bytes` under `[worker]` (default 1 MiB), which slices follow the same way
batches of commands do. A slice always processes its first available entry, even one larger than
that budget.

### Why This Matters
Enabling an index or a statistic on a partition with many entries must not stop the partition from
serving, and operators need a way to pause or slow the work on a loaded cluster.

### Impact on Users
- Nothing runs in the background unless an index or a statistic is being built.
- By default, the background work aims to use about a tenth of a partition's processing time while
  the partition is busy, and up to half while it is idle. These are pacing targets, not guarantees
  of resource use.
- The background work reads existing entries without filling RocksDB's block cache, so building an
  index or a statistic doesn't push the data that commands read out of the cache.

### Migration Guidance
No action needed. To pause the background work on a loaded cluster:

```toml
[worker.storage]
disable-background-migrations = true
```

To limit it to, for example, a thousand entries per second with bursts of up to five thousand:

```toml
[worker.storage]
background-migrations-throttling = { rate = "1000/s", capacity = 5000 }
```

To keep each slice smaller, for example at most 200 entries:

```toml
[worker.storage]
background-migrations-max-slice-entries = 200
```

### Related Issues
- PR #5851: Drive resumable walks in the background
- PR #5852: Run background migrations between processor turns
