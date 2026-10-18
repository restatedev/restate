# Respect retry limits when a retry interval is overridden

## Behavioral Change

### What Changed

Retry interval overrides now consume attempts from the invocation retry policy and
increment the reported retry attempt count. The supplied interval still determines
how long to wait when another retry is allowed.

### Why This Matters

Previously, supplying a retry interval could bypass the invocation's maximum
attempt limit indefinitely.

### Impact on Users

SDK-supplied retry intervals, including `ctx.run` retry options and `RetryableError`
with `retry_after`, and HTTP 429 `Retry-After` responses now consume attempts from
the invocation retry policy. Once `max_attempts` is reached, `on_max_attempts`
determines whether the invocation is paused or failed, even if another retry
interval is supplied. A policy that permits no retries takes that action
immediately.

### Migration Guidance

Review invocation-level `max_attempts` and `on_max_attempts` settings if your
application relies on SDK-controlled retries or HTTP 429 responses. Increase the
invocation retry budget if those retries must continue for longer.
