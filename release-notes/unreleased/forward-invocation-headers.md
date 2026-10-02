# Release Notes: Forward selected ingress headers to services as HTTP headers

## New Feature

### What Changed

Two new invoker options let operators choose which headers from an ingress request reach the
service deployment as HTTP request headers (event headers for Lambda deployments):

- `worker.invoker.forward-invocation-headers`: the header names to forward. Default: empty.
- `worker.invoker.header-forwarded-prefix`: an optional prefix added to the forwarded names.

```toml
# Required, see below.
experimental-enable-invocation-source-ingestion = true

[worker.invoker]
forward-invocation-headers = ["x-forwarded-user", "x-request-id"]
header-forwarded-prefix = "restate-forwarded-"
```

With this config, an ingress request carrying `x-forwarded-user: alice` reaches the service
with the HTTP header `restate-forwarded-x-forwarded-user: alice`.

### Why This Matters

Until now, ingress headers reached a service only inside the invocation's input. A proxy or
sidecar in front of the service, such as Envoy running an authorization check, could not see
them.

Forwarding is limited on purpose. Only invocations that came in through the HTTP ingress
forward headers. Calls from other services, Kafka subscriptions, restarts and the gRPC
ingestion API never do. This matters because a service controls the headers on the calls it
makes, so it could otherwise pass an identity header it made up downstream.

### Impact on Users

- No change unless `forward-invocation-headers` is set.
- Only the first value of each listed header is forwarded. Listed headers missing from the
  request are skipped.
- A forwarded header never replaces a header Restate or the deployment already sets on the
  request.
- The configuration is rejected, at startup or on reload, if a forwarded name, after the
  prefix is applied, is one Restate sets itself: `content-type`, `accept-encoding`,
  `traceparent`, `x-restate-*`, hop-by-hop headers, or a name in `additional-request-headers`.
- `forward-invocation-headers` requires `experimental-enable-invocation-source-ingestion`.
  Without it, the ingestion API marks its invocations as ingress invocations, and their headers
  come from the request body, where a proxy can't check them. Set it on every node that runs
  the ingress. Invocations ingested before it was set still count as ingress invocations.

### Migration Guidance

No migration needed. To turn forwarding on:

1. Set `experimental-enable-invocation-source-ingestion = true` on every node that runs the
   ingress. Restate versions before v1.8.0 don't know the ingestion invocation source this
   flag enables, so check your rollback plan first.
2. Wait for invocations submitted through the ingestion API before that to finish.
3. Set `forward-invocation-headers`.

If a proxy in front of the service strips or overwrites a header you forward, set
`header-forwarded-prefix` so the header arrives under a name the proxy leaves alone.
