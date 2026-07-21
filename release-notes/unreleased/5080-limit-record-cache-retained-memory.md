# Release Notes: Limit RecordCache Retained Memory

## Bug Fix

### What Changed

RecordCache now copies raw record bodies when admitting them to the cache. This prevents a small
cached body that is a slice of a larger buffer from retaining the full backing allocation.

### Why This Matters

The record cache's memory budget now more accurately bounds the memory retained for raw records,
reducing the risk of unexpected memory growth and out-of-memory failures under sustained log
traffic.

### Impact on Users

- Existing deployments gain tighter record-cache memory bounds after upgrading.
- New deployments receive the same protection automatically.
- No configuration or application changes are required.

### Migration Guidance

No migration is required.
