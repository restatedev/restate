# Release Notes: Less aggressive compact-on-deletion defaults for partition stores

## Behavioral Change

### What Changed

The defaults controlling when Restate prioritizes reclaiming partition-store disk space have
changed. Restate now waits for more deleted data and excludes files smaller than 32 MiB before
prioritizing reclamation.

| Option (`worker.storage.*`)                        | Old default  | New default |
|----------------------------------------------------|--------------|-------------|
| `rocksdb-compact-on-deletions-window`              | 1000         | 50000       |
| `rocksdb-compact-on-deletions-count`               | 100          | 30000       |
| `rocksdb-compact-on-deletions-ratio`               | 0.25         | 0.5         |
| `rocksdb-compact-on-deletions-min-sst-file-size`   | 0 (disabled) | 32 MiB      |

Together, these defaults make reclamation less aggressive during ordinary workload churn while
retaining earlier cleanup after large deletions such as journal retention expiry.

### Why This Matters

The previous defaults could generate substantial background disk I/O after ordinary deletion
workloads, increasing invocation read latency under sustained load. The new defaults reduce that
background work and prioritize steady-state latency, at the cost of potentially reclaiming disk
space more slowly after deletions.

### Impact on Users

- Existing deployments pick up the new default for each option they do not set explicitly. Values
  configured explicitly remain unchanged, so only deployments that set all four options retain the
  exact previous behavior.
- Existing and new deployments can expect less background disk I/O and lower invocation read
  latency under steady load. Disk space may be reclaimed more slowly after deletions.

### Migration Guidance

No action is required to use the less aggressive defaults. To keep the previous behavior, set all
four old values explicitly:

```toml
[worker.storage]
rocksdb-compact-on-deletions-window = 1000
rocksdb-compact-on-deletions-count = 100
rocksdb-compact-on-deletions-ratio = 0.25
rocksdb-compact-on-deletions-min-sst-file-size = "0"
```

These options are applied when a partition store is opened, so a change takes effect after a node
restart. Reclamation work already scheduled before the restart can still complete.
