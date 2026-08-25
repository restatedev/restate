# Release Notes: Retention is respected when terminating an invocation before it starts

## Behavioral Change

### What Changed

Killing or cancelling an invocation that has not started running yet — one that is
still queued (waiting for its Virtual Object / Workflow key or for a concurrency
slot) or scheduled for a later time (a delayed call) — now honours the invocation's
completion retention and journal retention, exactly like terminating an already
running invocation does.

Previously, terminating such an invocation freed its invocation status immediately
and dropped its journal, regardless of the retention configured for the service or
handler. The terminal result was therefore not observable afterwards.

The change is gated behind an opt-in experimental flag and, once enabled, you can't go back to any version below 1.8:

```toml
[common]
experimental-enable-preflight-invocation-termination-retention = true
```

This flag will be enabled by default in 1.9 such that this behavior becomes the default.

### Why This Matters

Whether a terminal result was retained used to depend on *when* the invocation was
terminated rather than on the retention you configured. An invocation killed one
moment before it was dequeued behaved differently from the same invocation killed
one moment after. Retention semantics are now uniform across an invocation's
lifetime.

### Impact on Users

With the flag enabled:

- **Idempotent invocations**: re-submitting the same idempotency key within the
  completion-retention window now returns the retained `KILLED` / `CANCELED`
  failure for those queued/delayed invocations, instead of starting a fresh invocation.
- **Workflow submissions**: cancelling a queued or delayed workflow submission now
  keeps that workflow key occupied for the completion-retention window (24h by
  default). Re-submitting the same workflow key during that window fails with
  `409 Conflict` / "the workflow method was already invoked" instead of starting
  the workflow.
- **Introspection**: terminated pre-flight invocations now show up as completed
  invocations (with their journal, when journal retention is non-zero) instead of
  disappearing. They are reclaimed when their retention expires.

Without the flag, or before the partition's feature barrier has been applied,
behaviour is unchanged.

### Migration Guidance

No action is required — the flag is off by default. However, we're planning to make
it the default in 1.9. Once the flag is enabled, you can't go back to any version below 1.8.
