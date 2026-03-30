# Release Notes for Issue #4354: Invoker Memory Pool

## New Feature / Behavioral Change

### What Changed

The invoker now operates under a **bounded memory pool** that limits the total amount of memory
used for in-flight data flowing from the server to service deployments (the outbound path). This
includes journal entries being replayed and service state being sent. Previously, this memory
usage was unbounded and could grow without limit under high load or when processing large
payloads.

Key aspects of the new memory model:

- **Global memory pool**: A shared budget (`worker.invoker.memory-limit`, default **256 MiB**)
  limits the total invoker memory across all partitions on a node.

- **Per-invocation budgets**: Each invocation receives a budget capped at
  `worker.invoker.per-invocation-memory-limit` (defaults to `message-size-limit`, typically
  **32 MiB**). The outbound budget covers data flowing from the server to deployments (journal
  entries, state).

- **Scheduler-level gating**: The scheduler only dispatches new invocations when the global
  memory pool has enough capacity for the initial reservation
  (`worker.invoker.per-invocation-initial-memory`, default **32 KiB**). This prevents
  overcommitting memory.

- **Budget-aware storage reads**: Journal replay and state loading now acquire memory from the
  invocation's outbound budget before reading data from storage, providing backpressure before
  data is materialized.

- **Smart failure handling**: When an invocation exhausts its memory budget, the system
  distinguishes between two cases:
  - **Global pool exhaustion** (transient): Other invocations are consuming the available memory.
    The invocation retries after a delay, giving other invocations time to complete and release
    their memory.
  - **Per-invocation upper bound exceeded** (permanent): The invocation needs more memory than
    its configured limit allows. Retrying will never help, so the invocation is immediately
    paused or killed based on the service's retry policy (`on_max_attempts` setting) without
    wasting retry attempts. The error message tells you exactly what happened and which
    configuration knob to adjust.

### Why This Matters

Without memory limits, a burst of invocations processing large payloads (big journal entries
or large state) could cause the server to consume unbounded memory, potentially leading to OOM
kills or degraded performance for all invocations on the node.

The memory pool provides **backpressure** — invocations that cannot fit in memory are delayed
rather than allowed to consume resources unchecked. This makes the system more predictable and
resilient under load, especially in multi-tenant deployments where different services may have
very different memory profiles.

### Configuration

Three new configuration options control the memory pool:

```toml
[worker.invoker]
# Global memory budget shared across all invocations on this node.
# Controls how much memory can be used for in-flight journal entries, state,
# and protocol messages sent from the invoker to service deployments.
# Default: "256MB"
memory-limit = "256MB"

# Maximum memory a single invocation may use for its outbound data.
# Defaults to message-size-limit if unset. Clamped at message-size-limit.
# Increase this if invocations are paused with "upper bound exceeded" errors.
per-invocation-memory-limit = "32MB"

# Memory reserved from the global pool before an invocation starts.
# Smaller values allow more concurrent invocations but may cause frequent
# round-trips to the global pool. Larger values reduce contention but limit
# maximum concurrency.
# Default: "32KB"
per-invocation-initial-memory = "32KB"
```

**Relationship between the options:**

- The global pool (`memory-limit`) is shared across all active invocations. The maximum number
  of invocations that can run concurrently is roughly
  `memory-limit / per-invocation-initial-memory` (in addition to the existing
  `concurrent-invocations-limit`).
- Each invocation's budget can grow up to `per-invocation-memory-limit` by drawing from the
  global pool as needed.
- `eager-state-size-limit` is automatically clamped to `per-invocation-memory-limit` since
  eager state is sent via the outbound budget.

### Impact on Users

- **Most users will see no change**: The defaults are generous enough for typical workloads.
  The global pool (256 MiB) can support thousands of concurrent invocations with normal-sized
  payloads.

- **Users processing very large payloads or state**: Invocations that send data exceeding the
  per-invocation memory limit (default 32 MiB) may now be paused with an error message like:

  > memory budget exhausted (upper bound exceeded) while reading journal entries: needed
  > 45000000 outbound bytes

  This is an improvement over the previous behavior where such invocations could consume
  unbounded memory. The error message tells you exactly what to adjust.

- **High-concurrency deployments**: If many invocations run simultaneously and the global pool
  is exhausted, new invocations will wait for memory to become available rather than starting
  immediately. Under sustained load this may reduce throughput compared to the previous
  unbounded behavior. Increase `memory-limit` if needed.

### Migration Guidance

**If invocations are paused with "upper bound exceeded" errors:**

The invocation needs more memory than the per-invocation limit allows. Either:

1. Increase the per-invocation limit:
   ```toml
   [worker.invoker]
   per-invocation-memory-limit = "64MB"
   ```
2. Investigate why the invocation is processing such large data and consider reducing payload
   sizes or state size.

**If throughput drops under high concurrency:**

The global memory pool may be the bottleneck. Either:

1. Increase the global pool:
   ```toml
   [worker.invoker]
   memory-limit = "512MB"
   ```
2. Decrease the initial per-invocation reservation to allow more concurrent invocations:
   ```toml
   [worker.invoker]
   per-invocation-initial-memory = "16KB"
   ```

**If invocations are retrying due to "pool exhausted" errors:**

This is a transient condition — the global pool is temporarily full. The system will
automatically retry. If this happens frequently, increase `memory-limit` to give the pool
more capacity.

### Related Issues

- #4354 — Invoker memory pool
- #4345 — Eager state size limit (now clamped by per-invocation memory limit)
