# Release Notes: Immediate pause/kill for per-invocation memory limit exceeded

## Behavioral Change

### What Changed

When an invocation exceeds its per-invocation memory limit (`worker.invoker.per-invocation-memory-limit`),
the invoker now immediately pauses or kills the invocation (based on the retry policy's `on_max_attempts`
setting) instead of retrying fruitlessly.

Previously, hitting the per-invocation memory limit was treated identically to global memory pool exhaustion —
the invocation would either yield or retry, burning through retry attempts even though the limit would never
be satisfied without a configuration change. Now the invoker distinguishes between:

- **Per-invocation upper bound exceeded**: The invocation needs more memory than its configured limit allows.
  Retrying cannot help — the invocation is immediately paused or killed per the retry policy.
- **Global pool exhaustion**: The shared memory pool is temporarily full. Yielding or retrying may help once
  other invocations free their memory. This continues to work as before.

### Impact on Users

- Invocations that require more memory than `per-invocation-memory-limit` will now be paused (default)
  or killed immediately rather than exhausting all retry attempts first.
- The error message clearly indicates the memory direction (inbound/outbound), the amount needed, and
  suggests increasing `worker.invoker.per-invocation-memory-limit`.
- No configuration changes are required — the improvement is automatic.

### Migration Guidance

If you observe invocations being paused with an "upper bound exceeded" out-of-memory error after upgrading,
increase the per-invocation memory limit:

```toml
[worker.invoker]
per-invocation-memory-limit = "16MB"  # Increase from the default (equal to message-size-limit)
```

Alternatively, if the invocation is processing unexpectedly large payloads, investigate and reduce
the payload size.
