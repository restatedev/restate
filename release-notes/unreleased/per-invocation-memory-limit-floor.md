# Release Notes: Large state values and journal entries no longer pause invocations

## Bug Fix

### What Changed
Restate accepts messages from a service that are slightly larger than `message-size-limit`, so that
a payload of exactly that size still fits. The per-invocation memory limit, however, defaulted to
exactly `message-size-limit` and could not be set higher. A state value or journal entry close to
the limit could therefore be written but not read back: the invocation that tried to read it was
paused with an out-of-memory error (`RT0001`, "memory budget exhausted (upper bound exceeded) while
reading journal entries" or "while reading service state"). With default settings this affected
values of about 32 MiB.

`worker.invoker.per-invocation-memory-limit` is now never smaller than the largest message Restate
accepts from a service, plus room for how Restate stores it. Smaller configured values are raised
with a warning at startup. It can also be set above `message-size-limit` now; before, it was capped
at that value.

A message from a service that exceeds `message-size-limit` now pauses the invocation (or kills it, if
the retry policy sets `on-max-attempts = "kill"`) instead of being retried, since the service sends
the same message on every retry.

### Impact on Users
- Invocations that read such a large state value or journal entry run instead of pausing.
- The default per-invocation memory limit is 512 KiB above `message-size-limit` instead of equal to
  it.
- Invocations whose service sends a message above `message-size-limit` pause after the first
  attempt instead of exhausting the retry policy first.

### Migration Guidance
None required. Invocations paused by this problem can be resumed after upgrading.
