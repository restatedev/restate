# Release Notes for PR #5178: Retrieve invocation status from the HTTP ingress

## New Feature

### What Changed

The HTTP ingress now exposes a **non-blocking** endpoint to retrieve the current status of an
invocation. Unlike `attach`/`output`, which block until the invocation completes, `status` returns
immediately with the invocation's current lifecycle stage.

New routes:

- `GET /restate/invocation/{invocation_id}/status`
- `GET /restate/invocation/{service}/{handler}/{idempotency_id}/status` (and the keyed variant
  `.../{service}/{key}/{handler}/{idempotency_id}/status`) to look up by idempotency id
- `GET /restate/workflow/{name}/{key}/status`
- `GET /restate/status/{invocation_id}` and `POST /restate/status` with a body describing the target

The response is a JSON body with a coarse-grained lifecycle `stage` and, when the invocation has
completed with a failure, the terminal `error`:

```json
{
  "stage": "created"
}
```

- `stage`: one of `created` (accepted but not yet started — scheduled or inboxed), `started`
  (running — invoked, suspended or paused), or `completed` (finished — succeeded, failed or killed).
- `error`: present only when `stage` is `completed` and the invocation failed; omitted otherwise.

The response also carries the `x-restate-id` header with the resolved invocation id. Unknown
invocations return `404 Not Found`.

### Why This Matters

Until now, clients could only observe an invocation by attaching to it or fetching its output, both
of which block until the invocation terminates. There was no way to cheaply poll whether an
invocation had started or already completed without waiting for its result. The new `status`
endpoint fills that gap, enabling lightweight progress polling and dashboards.

The `stage` values are intentionally coarse-grained to mirror the invocation lifecycle documented at
https://docs.restate.dev/services/invocation/managing-invocations#lifecycle, keeping the public
contract stable even if the internal states evolve.

### Impact on Users

- **Existing deployments**: purely additive. Existing routes and behavior are unchanged.
- **New deployments**: the new `status` routes are available out of the box.
- **Migration considerations**: none required.

### Related Issues

- PR #5178: `/restate/invocation/:invocation_id/status` to retrieve invocation status