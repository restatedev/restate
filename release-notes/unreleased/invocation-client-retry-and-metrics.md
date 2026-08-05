# Release Notes: Reduce invocation-client retry pressure and improve ingress observability

## Improvement

### What Changed

HTTP ingress requests now retry retryable invocation-client failures with exponential backoff,
starting at 50 milliseconds and capped at one second between attempts. Previously, retries used a
fixed 50-millisecond delay.

A new `restate.invocation_client.requests.total` counter records partition-processor RPC attempts
by `partition_id` and `status`. Status values distinguish completed requests from routing,
availability, overload, protocol, internal, and shutdown errors.

The `status` label on `restate.ingress.requests.total` now distinguishes `completed`,
`request_error`, `invocation_error`, and `ingress_error` outcomes. Ingress request duration is also
recorded for unsuccessful requests.

### Why This Matters

Exponential backoff reduces repeated traffic to stale partition leaders during leadership changes.
The new metric and status labels make it easier to identify retry pressure and distinguish invalid
requests, invocation failures, and failures within Restate's ingress processing.

### Impact on Users

- Existing and new deployments adopt the new retry behavior automatically.
- Custom dashboards and alerts can use the new statuses to separate application-level invocation
  failures from ingress failures.

### Migration Guidance

rometheus queries that use `status="completed"` to calculate the total
processed ingress request rate should include `request_error`, `invocation_error`, and
`ingress_error` as appropriate.
