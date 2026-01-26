# Release Notes: jemalloc Memory Metrics and Purge Endpoint

## New Feature

### What Changed

Added jemalloc memory statistics as Prometheus metrics and a new debug endpoint to manually trigger memory purging.

**New Metrics:**
- `restate_jemalloc_allocated_bytes` - Total bytes allocated by the application
- `restate_jemalloc_active_bytes` - Total bytes in active pages (multiple of page size)
- `restate_jemalloc_metadata_bytes` - Bytes dedicated to jemalloc metadata
- `restate_jemalloc_mapped_bytes` - Total bytes in chunks mapped for the application
- `restate_jemalloc_retained_bytes` - Bytes in virtual memory mappings retained (not returned to OS)
- `restate_jemalloc_resident_bytes` - Maximum bytes in physically resident data pages

**New Debug Endpoint:**
- `POST /debug/jemalloc/purge` or `PUT /debug/jemalloc/purge` - Triggers jemalloc arena purging to release retained memory back to the OS

### Why This Matters

jemalloc retains memory in "dirty pages" for performance optimization, which can make it appear that the process is using more memory than actually needed. The new metrics provide visibility into this behavior, and the purge endpoint allows operators to manually reclaim retained memory when needed.

### Impact on Users

- **Improved observability**: The jemalloc metrics help distinguish between actual memory usage (`allocated`) and memory held by the allocator (`resident`, `retained`)
- **Manual memory management**: The purge endpoint can be useful during debugging or when the system needs to free memory quickly
- **No action required**: These are additive features with no changes to existing behavior

### Usage

To check jemalloc metrics:
```bash
curl http://localhost:9070/metrics | grep jemalloc
```

To trigger a manual purge:
```bash
curl -X POST http://localhost:9070/debug/jemalloc/purge
```

The purge endpoint returns before/after statistics showing memory reclaimed.

> **Warning**: Purging is an expensive operation that forces jemalloc to immediately return unused memory to the OS. This can cause significant latency spikes as it may block allocations during the purge. Only use this for debugging or one-off memory reclamation when the system is under memory pressure. Do not call this endpoint regularly or automate it in production.
