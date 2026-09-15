# Release Notes: Increase default invoker concurrency limit

## Behavioral Change

### What Changed

The default value of `worker.invoker.concurrent-invocations-limit` has been increased from `1000`
to `24000`.

The semantics of the limit have also changed: it is now enforced per restate-server node. In prior
versions, the limit was applied per partition processor, so the effective per-node limit depended on
the number of partitions hosted by the node.

### Why This Matters

Restate v1.8.0 enables virtual queues (VQueues) by default. With VQueues, the invoker
concurrency limit is applied at the node level rather than per partition processor. Keeping the old
default of `1000` under the new semantics would have significantly reduced the effective concurrency
of a node: previously, a node hosting N partitions could process up to `N × 1000` concurrent
invocations, whereas the same setting now caps the entire node at `1000`. The new default of
`24000` restores a sensible node-wide capacity.

### Impact on Users

- **Deployments using the default**: The effective concurrency budget is now `24000` per node,
  independent of the number of partitions the node hosts. Depending on your partition count, this
  may be higher or lower than the previous effective limit of `1000 × partitions-per-node`.
- **Deployments with an explicit `concurrent-invocations-limit`**: The configured value is kept, but
  its meaning changes from per partition processor to per node. A value tuned for per-partition
  semantics will likely be too low once VQueues are enabled by default.

### Migration Guidance

If you have set `concurrent-invocations-limit` explicitly, re-evaluate the value for node-level
semantics. As a starting point, multiply your previous per-partition value by the typical number of
partitions hosted per node:

```toml
[worker.invoker]
concurrent-invocations-limit = 24000
```

If you rely on the default, no action is needed, but verify that your service deployments can handle
the higher node-wide concurrency (e.g. downstream connection pools and rate limits).

### Related Issues

- VQueues enabled by default in v1.8.0 (invoker concurrency limit becomes node-scoped).
