# Release Notes: Invoker memory pool backpressure

## New Feature

### What Changed

Added memory pool backpressure to the invoker. Each invoker now has a bounded
**inbound memory pool** that limits the total memory used by messages received
from service deployments across all concurrent invocations. When the pool is
exhausted, invocation tasks pause reading from the HTTP response stream until
previously received messages have been processed and their memory leases
released (typically after the effect is appended to Bifrost).

Two new configuration options control the pool sizes:

| Config key | Default | Description |
|---|---|---|
| `worker.invoker.invoker-inbound-memory-limit` | `128 MB` | Bounds memory used by inbound messages from deployments |
| `worker.invoker.invoker-outbound-memory-limit` | `128 MB` | Reserved for future outbound replay backpressure |

The `CachedJournal` optimization path (where the partition processor sent
cached journal entries to the invoker) has been removed. The invoker now always
reads journal entries directly from storage. This simplification was safe
because the partition processor commits the storage batch before executing
`Action::Invoke`.

### Why This Matters

Without memory backpressure, a burst of large messages from many concurrent
invocations could cause unbounded memory growth in the invoker, potentially
leading to OOM. The inbound memory pool provides a global bound that applies
backpressure through HTTP/2 flow control when memory is exhausted.

### Impact on Users

- **New deployments**: Use the 128 MB default automatically.
- **Existing deployments**: Will adopt the new defaults on upgrade. No action
  needed unless the server handles an unusually high number of concurrent
  invocations with very large payloads, in which case the limit can be
  increased.

### Migration Guidance

To adjust the inbound memory limit:

```toml
[worker.invoker]
invoker-inbound-memory-limit = "256 MB"
```
