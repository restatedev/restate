# Release Notes for Issue #5066: gRPC Ingestion API (`IngestionSvc`)

## New Feature

### What Changed

Restate now exposes an experimental gRPC service, `dev.restate.ingress.ingestion.v1.IngestionSvc`,
for external ingestion. Integrations open a bidirectional stream and push invocations into Restate
over it, with flow control and optional offset-based deduplication.

The service is hosted on the **existing ingress socket** — requests with a `content-type` of
`application/grpc` are routed to it, everything else keeps flowing through the regular HTTP ingress.
No new port or listener is introduced.

The wire protocol is documented in
[`crates/ingress-http/protobuf/ingestion_svc.proto`](../../crates/ingress-http/protobuf/ingestion_svc.proto).
Most users will not talk to it directly: it is the transport for the integration bridges we ship
(for example a Kafka-to-Restate bridge).

Invocations created through the API can be identified in `sys_invocation_status` by
`invoked_by = 'ingestion-api'`, and are counted by a new metric
`restate.ingress.ingestion.ingested.total`.

### Why This Matters

Until now, external systems could get data into Restate either through the HTTP ingress (one
request per invocation) or through built-in Kafka subscriptions. The ingestion API adds a streaming,
back-pressured, deduplicating path for integrations that replay an ordered sequence of records,
which is what our integration bridges are built on.

### Impact on Users

- The API is enabled by default on the ingress port, but only reachable over gRPC, which previously
  had no handler there. HTTP ingress traffic is unaffected.
- `ingress.request-size-limit` does not apply to ingestion streams, since a stream is continuous.
  Ingestion is bounded instead by `ingress.ingestion-api.max-window-size` and
  `ingress.ingestion-api.max-concurrent-streams`.
- Ingress concurrency accounting changed slightly: a load-shed permit is now held until the response
  body is dropped rather than when the response is produced, so long-lived streams count against the
  limit for their whole duration. This also affects `ingress.concurrent-api-requests-limit`
  (unlimited by default), where permits are now released marginally later.
- Once invocations are recorded with the dedicated ingestion source (see below), older Restate
  versions cannot decode it. Do not enable that flag if you may need to roll back.

### Migration Guidance

No action is required. To tune or disable the API:

```toml
[ingress.ingestion-api]
# Turn the gRPC ingestion API off entirely. Default: false
disable = false
# Bytes an ingestion stream may have in flight before the server applies back pressure.
# Default: 128 KiB
max-window-size = "128 KiB"
# Concurrent ingestion streams before the server starts rejecting them. Default: 1000
max-concurrent-streams = 1000
```

To record ingested invocations with the dedicated source (and see
`invoked_by = 'ingestion-api'` in `sys_invocation_status`):

```toml
[common]
experimental-enable-invocation-source-ingestion = true
```

While this flag is disabled, invocations created through the ingestion API are recorded with the
`ingress` source, exactly as HTTP ingress invocations are.

### Related Issues

- Issue #5066: Ingestion API
- PR #5026: Ingestion API implementation
