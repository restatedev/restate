# Release Notes: Optional Rate limiting for storage query execution

## New Feature

### What Changed
Datafusion query execution (the admin REST `/query` endpoint and the
cluster-controller gRPC query service) can now be rate limited via a new,
optional `rate-limiting` knob on the query engine options. The limiter is a
token bucket: each query consumes one token, and queries that exceed the limit
fail fast instead of executing.

When the limit is exceeded, the admin REST endpoint responds with `429 Too Many Requests` and a `Retry-After` header.

If `rate-limiting` is unset (the default), no rate limiting is applied and
behavior is unchanged.

### Why This Matters
Expensive or high-volume ad-hoc SQL queries can compete with the node's core
workload for CPU and memory. Operators can now cap query throughput on demand to protect
the rest of the system.

### Impact on Users
- New deployments: no rate limiting unless explicitly configured.
- Existing deployments: no change on upgrade unless explicitly configured.

### Migration Guidance
To rate limit queries, configure the rate (and optionally a burst capacity) in
the query engine options:

```toml
[admin.query-engine.rate-limiting]
rate = "100/s"      # <rate>/<unit>, unit is s|sec|second, m|min|minute, or h|hr|hour
capacity = 200      # optional burst capacity; defaults to the rate value
```

## Behavioral Change

### What Changed
The admin REST `/query` endpoint now returns `400 Bad Request` (instead of
`500 Internal Server Error`) for client-side query errors — Datafusion `Plan`,
`SchemaError`, and `SQL` errors (e.g. invalid SQL or unknown columns/tables).

### Impact on Users
- Clients that distinguish failures by status code will now see `4xx` for
  malformed queries instead of `5xx`. Internal/execution failures continue to
  return `500`.
