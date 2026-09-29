# Release Notes: Paced cleanup of completed invocations

## Behavioral Change / New Feature

### What Changed

The periodic cleanup of completed invocations (controlled by `worker.cleanup-interval`) is now
paced. Each partition keeps at most `worker.cleanup-max-in-flight-purges` purges in progress at a
time, and it defaults to `32`.

### Why This Matters

Previously, a cleanup run could purge a large backlog of completed invocations as fast as possible.
While it ran, other requests on the same partitions queued behind the purges, which can lead to higher tail latencies. With pacing, cleanup has a bounded impact on other requests.

### Impact on Users

- Existing and new deployments use the default of `32` without requiring configuration changes.
- Cleanup runs may take longer to purge large backlogs of completed invocations.
- Deployments with high log latency, for example clusters spanning multiple regions, purge fewer
  invocations per second. If cleanup can't keep up with the rate of completed invocations,
  completed invocations stay in storage longer than their retention.

### Migration Guidance

No action is required. If cleanup doesn't keep up with your workload, increase the limit and restart
each node so the setting is applied to all hosted partitions. Higher values purge faster, but add
more latency to other requests while cleanup runs.

```toml
[worker]
cleanup-max-in-flight-purges = 64
```
