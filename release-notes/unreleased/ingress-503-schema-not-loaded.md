# Release Notes: Ingress no longer returns 404 while the schema is loading

## Bug Fix

### What Changed
Right after a node starts, its HTTP ingress could answer `404 service not found` for services that
are registered, because the node had not loaded the service schema yet. The ingress now answers
`503 Service Unavailable` in that window instead, and the request can be safely retried.

The gRPC ingestion API had the same problem and reported `ERROR_KIND_NOT_FOUND`. It now reports the
new `ERROR_KIND_UNAVAILABLE`, which means: reconnect and resend the same records later.

Attaching to or fetching the output of an invocation or workflow no longer returns `404` when its
service or handler was removed after the invocation completed. The result is returned without a
`Content-Type` header instead. While the schema is still loading, these requests return `503`.

### Impact on Users
- Clients no longer see spurious 404s during node startup or restarts.
- Calls to services that are not registered keep returning `404` once the node is up.
- On existing clusters where no deployment has ever been registered, calls to unknown services return
  `503` instead of `404` until the first deployment is registered.

### Migration Guidance
None.
