# Release Notes: Per-service OpenAPI documents the `/restate/` ingress endpoints

## New Feature

### What Changed

The per-service OpenAPI contract (served from the admin API at `GET /services/{service}/openapi`) now
describes the full set of `/restate/` ingress endpoints, organized under three tags:

- **Request Response** — `POST /restate/call/{service}[/{key}]/{handler}` and its scoped variant
  `POST /restate/scope/{scope}/call/{service}[/{key}]/{handler}`.
- **Send** — `POST /restate/send/{service}[/{key}]/{handler}` and its scoped variant
  `POST /restate/scope/{scope}/send/{service}[/{key}]/{handler}`.
- **Invocations output and status** — for each of `attach`, `output` and `status`, two operations:
  - a `GET /restate/{attach,output,status}/{invocationId}` addressing the invocation by id, and
  - a `POST /restate/{attach,output,status}` whose body (`RestateInvocationTarget`) selects the
    target by invocation id, workflow key, or idempotency target.

The scoped `call`/`send` operations carry a required `scope` path parameter (the concurrency scope)
and an optional `x-restate-limit-key` header (also accepted as a `limit-key` query parameter; the
header takes precedence).

The old, path-based per-handler `attach`/`output` routes
(`/restate/workflow/{name}/{key}/{attach,output}` and
`/restate/invocation/{service}/{handler}/{idempotencyKey}/{attach,output}`) are no longer emitted in
the contract; the id-based and target-based operations above replace them.

### Why This Matters

Clients and code generators that consume the per-service OpenAPI can now discover and call the whole
ingress surface — including the scope/limit-key concurrency-control flow and the invocation
attach/output/status endpoints — directly from the generated spec, with a clean tag grouping instead
of one deprecated route per handler.

### Impact on Users

- **Existing deployments**: the generated contract reflects the change after a re-fetch; no runtime
  behavior changes.
- **New deployments**: the reorganized contract is present out of the box.
- **Migration considerations**: none required. Scoped invocations still require the vqueues
  experimental feature to be enabled at runtime (scoped Virtual Object targets additionally require
  the corresponding experimental flag).
