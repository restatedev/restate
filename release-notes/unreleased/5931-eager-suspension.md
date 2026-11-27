# Release Notes for Issue #5931: Idle invocations are suspended early when concurrency limits are reached

## Behavioral Change / New Feature

### What Changed

When invocations wait because the node's concurrent invocations limit
(`worker.invoker.concurrent-invocations-limit`) or a concurrency rule is reached, Restate now
suspends a running invocation that holds such a slot and is idle, waiting on an await point (for
example a call, a sleep or an awakeable). Its slot goes to a waiting invocation right away.
The suspended invocation resumes as usual once what it waits for completes.

Previously, an idle invocation kept its slot until its inactivity timeout fired (1 minute by
default), even when other invocations were waiting for one.

### Why This Matters

Under load, idle invocations could hold all slots while the invocations they wait for, such as
the services they call, could not start. Throughput then dropped sharply until inactivity
timeouts freed slots.

### Impact on Users

- Invocations of services that use service protocol V7 or newer can now be suspended before
  their inactivity timeout while they wait, but only when other invocations are waiting for a
  slot. Invocations that run code, including `ctx.run` blocks, are not suspended early.
- More invocations may suspend and resume under load. Resuming replays the invocation's journal.
- Services on older SDKs are not affected.

### Migration Guidance

No action is needed. To keep the previous behavior on a node, disable eager suspension:

```toml
[worker.invoker]
disable-eager-suspension = true
```

Changes to this option take effect without a restart.

To opt a single service or handler out, declare it as not preemptable in the SDK
(`preemptable: false`). This needs an SDK version that supports it.

### Related Issues

- PR #5931: Suspend idle invocations early when concurrency limits are reached
- PR #6026: Let services and handlers declare themselves non-preemptable
