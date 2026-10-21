# Release Notes: Reject new invocations when a partition runs out of storage

## New Feature

### What Changed
The new option `worker.storage.partition-capacity` sets how much storage each partition may use.
Once a partition reaches 90% of it, requests that would start a new invocation on that partition,
such as HTTP calls and sends or restarting an invocation as new, fail with
HTTP 507 (Insufficient Storage). The error names the partition and how much of its capacity is
used. Everything else keeps working: requests for existing invocations, cancellations, purges,
queries, and all other partitions.

### Why This Matters
A single partition that grows without bound can fill the disk it shares with other partitions.
Rejecting new work on that partition early leaves time to free space or grow the disk, while the
rest of the cluster keeps running.

### Impact on Users
- The option is unset by default, so existing deployments are unaffected.
- Invocations arriving through Kafka subscriptions, the ingestion API, or calls made by other
  Restate invocations are not rejected.

### Migration Guidance
To enable it, set the capacity to the storage you want each partition to have, for example:

```toml
[worker.storage]
partition-capacity = "50 GiB"
```
