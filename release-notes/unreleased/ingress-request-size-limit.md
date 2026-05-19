# Release Notes: Ingress request size limit

## New Feature / Behavioral Change

### What Changed

The HTTP ingress now enforces a maximum request body size. Requests whose body
exceeds the limit are rejected with `413 Payload Too Large` before reaching the
handler.

A new configuration option `ingress.request-size-limit` controls the cap. If
unset, it defaults to `networking.message-size-limit` (32 MiB by default). If
set, the value is always clamped at `networking.message-size-limit`, since
larger requests cannot be transmitted over the cluster-internal network.

### Why This Matters

Previously, the ingress accepted request bodies of unbounded size and only
failed later — often deep in the pipeline — once cluster-internal size limits
were hit. The errors surfaced to clients were confusing and the server had
already paid the cost of buffering the oversized payload.

With this change, oversized requests are rejected early with a standard 413
response, protecting the server from unbounded buffering and giving clients an
unambiguous error.

### Configuration

```toml
[ingress]
# Maximum request body size accepted by the HTTP ingress.
# Defaults to `networking.message-size-limit` if unset.
# Always clamped at `networking.message-size-limit`.
request-size-limit = "10MB"
```

### Impact on Users

- **Default behavior changes**: requests larger than `networking.message-size-limit`
  (32 MiB by default) are now rejected with 413 instead of being accepted and
  failing later. Clients that relied on submitting larger payloads must either
  reduce request size or raise `networking.message-size-limit`.
- **New deployments**: no action required; the default 32 MiB cap matches what
  the cluster-internal network can carry.
- **Existing deployments**: review whether any clients send bodies larger than
  32 MiB. If so, either shrink the payload or increase
  `networking.message-size-limit` (and optionally set an explicit
  `ingress.request-size-limit`).

### Migration Guidance

To raise the ingress limit, raise the networking limit as well:

```toml
[networking]
message-size-limit = "64MB"

[ingress]
request-size-limit = "64MB"
```

Setting `ingress.request-size-limit` higher than `networking.message-size-limit`
has no effect — the value is clamped to the networking limit.
