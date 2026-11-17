# New option `worker.vqueue-inbox-cache-initial-capacity`

## New Feature

### What Changed
The new `worker.vqueue-inbox-cache-initial-capacity` option sets how many entries the partition
leader reserves up front for each VQueue it schedules. It defaults to 24, the previous fixed value.

### Why This Matters
Deployments with very many VQueues that each hold only one or a few waiting entries, for example
right after upgrading with long-lived Kafka subscriptions, can see high leader memory usage. Lowering
this value reduces the memory used per waiting VQueue.

### Impact on Users
- No change with the default value.
- Changes take effect when a partition processor becomes leader.

### Migration Guidance
If partition leaders use a lot of memory while many VQueues are waiting, lower the value, for example:

```toml
[worker]
vqueue-inbox-cache-initial-capacity = 0
```

A value of 0 reserves nothing until entries arrive.
