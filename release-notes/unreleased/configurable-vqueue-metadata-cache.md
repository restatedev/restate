# Release Notes: Configure the VQueue metadata cache size

## New Feature

### What Changed

Restate now supports the `worker.vqueue-metadata-cache-size` configuration option. It sets the
target number of VQueue metadata entries cached by each partition and defaults to `32000`.
Active VQueues remain cached when the configured target is exceeded.

### Why This Matters

Keeping VQueue metadata in memory reduces partition-store reads. Operators can now balance that
benefit against memory usage based on their partition count and workload. Each cached entry uses
approximately 300 bytes, so the default target uses approximately 9 MiB per partition.

### Impact on Users

- Existing and new deployments use the `32000` default without requiring configuration changes.
- Increasing the value can reduce disk reads for workloads with many VQueues at the cost of more
  memory per partition.
- Reducing the value can lower memory usage but may increase disk reads as inactive metadata is
  evicted more frequently.

### Migration Guidance

No action is required. To set a different target, add the following configuration and restart each
node so the setting is applied to all hosted partitions:

```toml
[worker]
vqueue-metadata-cache-size = 16000
```
