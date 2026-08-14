# Release Notes for Issue #1958: Deployment status in the Admin API

## New Feature

### What Changed
The Admin API can now report the computed status of a deployment, either `active` or `drained`.

- `GET /deployments?include=status` includes the status of every registered deployment.
- `GET /deployments/{deployment}?include=status` includes the status of a single deployment.

The status is computed on demand and cached. Responses that carry a status include an `Age` header (in seconds) indicating how old the cached value is, together with `Cache-Control: no-store`. Adding `&refresh=true` forces a recomputation, bypassing the cached value.

The cache is refreshed when it expires or when the schema registry changes. The maximum age of a cached status is controlled by the new `admin.deployment-status-cache-ttl` configuration option (default: `1h`).

### Why This Matters
Operators can now discover which deployments are still actively serving invocations (`active`) and which have been fully drained (`drained`), for example to decide when a deployment can be safely removed.

### Impact on Users
- New deployments and existing deployments: the API is opt-in via the `include=status` query parameter, so existing clients that do not request the status are unaffected.
- Because the status is cached, it may lag behind the current state by up to `admin.deployment-status-cache-ttl`. Use `refresh=true` when an up-to-date value is required.

### Migration Guidance
No action required. To tune how long the status is cached, set:

```toml
[admin]
deployment-status-cache-ttl = "1h"
```

### Related Issues
- Issue #1958: Add an API to get the status of a deployment
- PR #5169: `[admin]` `/deployments?include=status` API
