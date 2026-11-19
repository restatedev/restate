# Release Notes: Objects with large state no longer pause their invocations

## Bug Fix

### What Changed
When the state of a Virtual Object or Workflow was larger than the invoker's per-invocation memory
limit, invocations on it were paused at start with an out-of-memory error (`RT0001`, "memory budget
exhausted (upper bound exceeded) while reading service state"). With default settings the limit is
the message size limit, 32 MiB, so this affected any object whose state grew past about 32 MiB.

Restate now sends as much of the state as fits and the handler fetches the remaining keys on
demand, as `eager-state-size-limit` intends. The same fix applies to services that preload only
selected state keys, when those keys together exceed the limit.

### Impact on Users
- Invocations on objects with large state run instead of pausing. Before, they paused on the first
  attempt without retrying, or failed if the retry policy sets `on-max-attempts = "kill"`. While the
  paused invocation held the object, later calls to the same object waited behind it.
- Handlers that read keys that didn't fit in the preloaded state now fetch them from the server, one
  round trip per key.

### Migration Guidance
None required. Invocations paused by this problem can be resumed after upgrading.
