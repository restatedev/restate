# Release Notes: gRPC Message Size Configuration

## Breaking Change / New Feature

### What Changed

1. **Configuration key renamed**: `networking.max-message-size` is now `networking.message-size-limit`
2. **New default**: The default message size limit is now **32 MiB** (previously varied by component)
3. **Unified configuration**: A single setting now controls message size limits across all gRPC services

**Renamed configuration options:**

| Old Name | New Name |
|----------|----------|
| `networking.max-message-size` | `networking.message-size-limit` |
| `common.metadata-client.max-message-size` | `common.metadata-client.message-size-limit` |

### Why This Matters

- **Larger payloads supported**: The 32 MiB default allows for larger state entries, `ctx.run()` results, and request/response payloads
- **Simplified configuration**: One setting applies uniformly to all cluster communication
- **Consistency**: Sub-limits (`worker.invoker.message-size-limit`, `common.metadata-client.message-size-limit`) are automatically clamped to not exceed the networking limit

### Impact on Users

- **Configuration migration**: If you explicitly configured `networking.max-message-size`, rename it
- **Larger limits**: If you previously hit message size limits, the new 32 MiB default may resolve the issue

### Migration Guidance

**Before:**
```toml
[networking]
max-message-size = "16MiB"

[common.metadata-client]
max-message-size = "16MiB"
```

**After:**
```toml
[networking]
message-size-limit = "16MiB"

[common.metadata-client]
message-size-limit = "16MiB"
```

**When to increase the limit**: Consider increasing `networking.message-size-limit` if you have:
- Large virtual object state entries (> 32 MiB)
- Large `ctx.run()` results
- Large request/response payloads

### Related Issues

- [#4100](https://github.com/restatedev/restate/pull/4100): Configurable gRPC max encoding/decoding message size
- [#4137](https://github.com/restatedev/restate/pull/4137): Unify message_size_limit configuration across gRPC services
