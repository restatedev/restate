# Release Notes: Partition-Driven Log Trimming

## Behavioral Change

### What Changed

Log trimming is now handled directly by partition leaders rather than by a centralized cluster controller task. This provides more responsive trimming behavior - logs are trimmed immediately when durability conditions are met, rather than on an hourly schedule.

**Removed configuration options:**

| Removed Option | Previous Default |
|----------------|------------------|
| `admin.log-trim-check-interval` | 1 hour |
| `worker.experimental-partition-driven-log-trimming` | false |

**New configuration options:**

| New Option | Type | Default | Description |
|------------|------|---------|-------------|
| `worker.durability-mode` | string | `balanced` (with snapshots) / `replica-set-only` (without) | Controls when partition store state is considered durable enough to trim the log |
| `worker.trim-delay-interval` | duration | `0s` | Delay trimming after durability condition is met (useful for geo-replicated S3) |

#### Durability Modes

The `worker.durability-mode` option controls the criteria for determining when it's safe to trim the log:

| Mode | Description |
|------|-------------|
| `balanced` | Partition store is durable when covered by a snapshot **and** at least one replica has flushed to local storage. **Default when a snapshot repository is configured.** |
| `snapshot-only` | Partition store is durable only after a snapshot has been created, regardless of local replica state. |
| `snapshot-and-replica-set` | Partition store is durable when **all** replicas have flushed locally **and** a snapshot exists. |
| `replica-set-only` | Partition store is durable when all replicas have flushed locally, regardless of snapshot state. **Default when no snapshot repository is configured.** |
| `none` | Disables automatic durability tracking and trimming entirely. |

See the [documentation](https://docs.restate.dev/server/snapshots#log-trimming-and-durability) for more details on log trimming and durability.

### Why This Matters

- **More responsive**: Logs are trimmed as soon as they are safe to remove, reducing storage usage more quickly
- **Simpler operation**: No need to configure or tune trim check intervals
- **Better scalability**: Each partition leader independently manages its own log trimming
- **Flexible durability**: New durability modes allow fine-grained control over when logs can be trimmed

### Impact on Users

- **Configuration cleanup**: Remove `admin.log-trim-check-interval` if you had it configured
- **Requirements unchanged**: Multi-node clusters still require a configured snapshot repository (`worker.snapshots.destination`) for automatic log trimming. Without it, automatic trimming is disabled and a warning is logged.
- **Single-node deployments**: Can trim based on local durability without snapshots

### Migration Guidance

1. **Remove deprecated options** from your configuration if present:
   ```toml
   # Remove these if present:
   # [admin]
   # log-trim-check-interval = "1h"
   
   # [worker]
   # experimental-partition-driven-log-trimming = true
   ```

2. **Ensure snapshot repository is configured** for multi-node clusters:
   ```toml
   [worker.snapshots]
   destination = "s3://your-bucket/snapshots"
   ```

3. **(Optional) Configure durability mode and trim delay** if you need non-default behavior:
   ```toml
   [worker]
   # Controls when partition store state is considered durable enough to trim the log
   # Values: "balanced", "snapshot-only", "snapshot-and-replica-set", "replica-set-only", "none"
   durability-mode = "balanced"
   
   # Delay trimming after durability condition is met (useful for geo-replicated S3)
   trim-delay-interval = "5m"
   ```

### Related Issues

- [#3479](https://github.com/restatedev/restate/issues/3479) Partition-driven log trimming
- [#3842](https://github.com/restatedev/restate/pull/3842): Partition-driven trimming is now the default
