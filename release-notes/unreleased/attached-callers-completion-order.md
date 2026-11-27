# Callers waiting on the same invocation no longer lose its result

## Bug Fix

### What Changed

Some invocations have more than one caller waiting on their result. This happens when
another invocation attaches to them, or calls them again with the same idempotency key.
Every replica of a partition now hands those results out in the same order.

### Why This Matters

Replicas of a partition could hand those results out in a different order. After a
failover, or after a restart that replayed the log, one caller could then never receive
the result, and another could receive it twice. A caller that never receives its result
stays suspended forever.

### Impact on Users

- Callers waiting on the same invocation each receive its result exactly once.
- The fix applies to results handed out after all replicas of a partition run this version.

### Migration Guidance

No action is needed.
