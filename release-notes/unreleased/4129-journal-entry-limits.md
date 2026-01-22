# Release Notes for Issue #4129: Journal Entry Size Limits

## Breaking Change / Behavioral Change

### What Changed

Restate now enforces strict size limits on journal entries (such as state values, `ctx.run()` results, and request/response payloads). When a journal entry exceeds the configured `message-size-limit`, the invocation will fail with error code **RT0003** (`MessageSizeLimit`) and be retried according to the configured retry policy, rather than causing system instability.

The default limit is **32 MiB** (from `networking.message-size-limit`).

### Why This Matters

Previously, when services created journal entries larger than the internal message size limit (32 MiB), the system could enter an unrecoverable state:

- The `SequencerAppender` would retry indefinitely trying to replicate messages that would never succeed
- This blocked all subsequent appends to the log
- Eventually, the entire cluster could become unresponsive

This was particularly problematic in distributed clusters where log replication between nodes failed due to network message size constraints, while local in-memory replication (which has no size limit) would succeed.

### Impact on Users

- **Services with large state values**: If your services store state entries larger than 32 MiB, these operations will now fail with error code `RT0003` (`MessageSizeLimit`) instead of causing cluster instability
- **Large `ctx.run()` results**: Side effect results exceeding the limit will cause the invocation to fail
- **Large request/response payloads**: Payloads exceeding the limit will be rejected

The invocation will fail and be retried according to the configured retry policy. If the payload size issue is not resolved (e.g., by updating the service code), the invocation will eventually be paused or killed after exhausting retries.

### Configuration

The limit is controlled by `networking.message-size-limit` (default: 32 MiB). Component-specific limits are automatically clamped to this value:

```toml
[networking]
message-size-limit = "32MiB"  # default

# Component-specific limits (optional, clamped to networking limit)
[worker.invoker]
message-size-limit = "32MiB"

[bifrost]
# record-size-limit is derived from networking.message-size-limit
```

### Migration Guidance

1. **Review your payload sizes**: If you have services that store large state values or return large results, ensure they stay under 32 MiB per entry

2. **Split large data**: Instead of storing one large state entry, consider:
   - Splitting data across multiple state keys
   - Using external storage for large blobs and storing references in Restate state
   - Chunking large results into smaller pieces

3. **Monitor for errors**: After upgrading, watch for `RT0003` error codes which indicate message size limit exceeded

4. **Increase limit if needed**: If 32 MiB is insufficient, increase the limit:
   ```toml
   [networking]
   message-size-limit = "64MiB"
   ```
   Note: Increasing this limit increases memory pressure on the cluster during replication.

### Related Issues

- [#4129](https://github.com/restatedev/restate/issues/4129): Improve handling of large messages
- [#4132](https://github.com/restatedev/restate/issues/4132): Make Bifrost batching aware of message size limits
- [#4137](https://github.com/restatedev/restate/pull/4137): Unify message_size_limit configuration across gRPC services
