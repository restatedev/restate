# Release Notes for Issue #5425: Pending sleeps no longer leak into a later run with the same ID

## Bug Fix

### What Changed
When a workflow run, or an invocation with an idempotency key, is killed, completed or purged,
Restate now removes the sleeps that were still pending in its journal.

Previously these pending sleeps stayed scheduled. A workflow run always gets the same ID for a
given workflow key, and an idempotent invocation gets the same ID for a given idempotency key. So
if the invocation was purged and then started again before the old sleep was due, the old sleep
fired into the new run. The new run then either woke up from its own sleep too early, or failed
repeatedly with `RT0007 Unexpected variant in async result`.

### Impact on Users
- Restarting a workflow with the same key after purging it no longer resolves the new run's
  sleeps early or makes it fail with `RT0007`. The same applies to reusing an idempotency key.
- Invocations without a workflow key or idempotency key are not affected.
- The fix only prevents new leftover sleeps. Sleeps already left behind by runs purged before the
  upgrade still fire when they are due.

### Migration Guidance
No action required.

### Related Issues
- Issue #5425: Purging a workflow run leaves its sleep timers, which then fire into the next run of the same key
