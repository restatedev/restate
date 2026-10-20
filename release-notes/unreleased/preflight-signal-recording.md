# Signals sent to invocations that haven't started yet are no longer lost

## Bug Fix

### What Changed
Signals sent to an invocation that is still scheduled (e.g. a delayed send) or waiting
in a virtual object's queue were silently discarded. That includes awakeable
completions. With the new experimental option enabled, Restate records these signals
and the handler receives them once the invocation starts.

### Impact on Users
- Until the option is enabled, the behavior is unchanged: such signals are still discarded.
- Enabling it is one-way: removing the option later doesn't turn recording off again.
- Only signals delivered after the option takes effect are recorded. Signals that were
  already discarded can't be recovered.
- Invocations created on Restate versions older than v1.6 that are still scheduled or
  queued keep discarding these signals.

### Migration Guidance
Upgrade all nodes to v1.8.1 or newer, then enable the option:

```toml
experimental-enable-preflight-signal-recording = true
```

Once enabled, you **cannot** roll back to a Restate-server version older than v1.8.1.
