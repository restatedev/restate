# Release Notes: New ingress endpoints under `/restate/`

## New Feature

### What Changed

The ingress HTTP server gained a new set of endpoints under the reserved `/restate/` prefix that support scope and limit-key for vqueue-based concurrency control, plus endpoints to attach to or look up running invocations.

### New URL Patterns

```
# Non-scoped service calls
POST /restate/call/{service}/{handler}
POST /restate/call/{service}/{key}/{handler}
POST /restate/send/{service}/{handler}
POST /restate/send/{service}/{key}/{handler}

# Scoped service calls (for vqueue concurrency control)
POST /restate/scope/{scopeKey}/call/{service}/{handler}
POST /restate/scope/{scopeKey}/call/{service}/{key}/{handler}
POST /restate/scope/{scopeKey}/send/{service}/{handler}
POST /restate/scope/{scopeKey}/send/{service}/{key}/{handler}

# Attach to / read the output of an invocation by its id
GET  /restate/attach/{invocation_id}
GET  /restate/output/{invocation_id}

# Look up the deterministic invocation id for a workflow or idempotency target
POST /restate/lookup
```

### Limit Key

A limit key can be provided via header or query parameter on `call`/`send`:

```
# Via header
POST /restate/scope/myScope/call/MyService/myHandler
x-restate-limit-key: tenant1/user42

# Via query parameter (useful for webhook URL templates)
POST /restate/scope/myScope/call/MyService/myHandler?limit-key=tenant1/user42
```

If both are present, the header takes precedence.

### Lookup

`POST /restate/lookup` accepts a JSON body describing a workflow or an idempotency target and returns its invocation id, which can then be passed to `/restate/attach/{invocation_id}` or `/restate/output/{invocation_id}`.

Workflow target:

```json
{
  "type": "workflow",
  "name": "MyWorkflow",
  "key": "wf-1",
  "scope": "tenant-a"
}
```

Idempotency target:

```json
{
  "type": "idempotency",
  "service": "greeter.GreeterObject",
  "serviceKey": "obj-1",
  "handler": "greet",
  "idempotencyKey": "K1",
  "scope": "tenant-a"
}
```

`scope` is optional. The response is `{"invocationId": "..."}`.

### Attach / get output by target (one-shot)

For callers that only have a workflow or idempotency target, `/restate/attach` and `/restate/output` also accept a `POST` with the same body shape as `/restate/lookup`:

```
POST /restate/attach
POST /restate/output
```

The server resolves the target into an invocation id and serves the request in a single round-trip, avoiding the explicit `/restate/lookup` step. The `GET /restate/{attach,output}/{invocation_id}` variants remain unchanged for callers that already have an invocation id.

### Impact on Users

- **Existing deployments**: No impact. The old unversioned paths (`/{service}/{handler}`, `/restate/invocation/...`, `/restate/workflow/...`, etc.) continue to work unchanged.
- **New usage**: Users who want scope-based partitioning, limit-key concurrency control, or programmatic attach/output by target should use the new `/restate/` paths above.
