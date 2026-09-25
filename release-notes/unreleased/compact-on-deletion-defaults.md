# Release Notes: Less aggressive compact-on-deletion defaults for partition stores

## Behavioral Change

### What Changed

The defaults of the partition-store compact-on-deletion triggers have changed. This mechanism
marks an SST file for early compaction when it contains many tombstones, so that space is reclaimed
sooner after bulk deletions such as journal retention expiry.

| Option (`worker.storage.*`)                        | Old default  | New default |
|----------------------------------------------------|--------------|-------------|
| `rocksdb-compact-on-deletions-window`              | 1000         | 50000       |
| `rocksdb-compact-on-deletions-count`               | 100          | 30000       |
| `rocksdb-compact-on-deletions-ratio`               | 0.25         | 0.5         |
| `rocksdb-compact-on-deletions-min-sst-file-size`   | 0 (disabled) | 32 MiB      |

With the new defaults a file is only marked when it is at least 32 MiB in size and either 60% of
the keys in a 50,000-key window are tombstones, or half of the whole file is.

The new values align the defaults with what Restate recommends internally for production
deployments.

### Why This Matters

The previous defaults marked a file as soon as any 1,000-key window contained 100 tombstones, with
no lower bound on file size. Small, freshly flushed files routinely met that condition because
ordinary workload churn writes short runs of adjacent deletes. Each marked file forced a compaction
into the bottom level that rewrote far more data than the file itself, which under sustained load
kept the disk busy with compaction and showed up as elevated read latency for invocations.

The new defaults keep the mechanism for its intended purpose, reclaiming space after genuine bulk
deletions, while ignoring small files whose tombstones are cleaned up by regular compaction anyway.

### Impact on Users

- Existing deployments that do not set these options explicitly pick up the new defaults on
  upgrade. Expect less background compaction write volume under steady load. Space reclamation after
  large deletions may take slightly longer for data that sits in small files, since those now wait
  for regular compaction.
- Deployments that set any of these options explicitly are unaffected.
- New deployments start with the new defaults.

### Migration Guidance

No action is required. To keep the previous behavior, set the old values explicitly:

```toml
[worker.storage]
rocksdb-compact-on-deletions-window = 1000
rocksdb-compact-on-deletions-count = 100
rocksdb-compact-on-deletions-ratio = 0.25
rocksdb-compact-on-deletions-min-sst-file-size = "0"
```

These options are applied when a partition store is opened, so a change takes effect after a node
restart. Files that were already marked for compaction before the restart are still compacted once.
