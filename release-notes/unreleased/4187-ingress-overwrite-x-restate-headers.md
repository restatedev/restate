# Release Notes for Issue #4187: Ingress drops client-supplied `x-restate-*` headers

## Bug Fix

### What Changed
When the HTTP ingress receives a request, it now drops any client-supplied
`x-restate-*` headers before forwarding the request to the service. The
`x-restate-*` namespace is reserved for the ingress (for example,
`x-restate-ingress-path`), so callers can no longer inject or override these
headers.

### Why This Matters
Previously, a caller could set `x-restate-*` headers on an incoming request and
have them forwarded to the service (or override the values the ingress sets),
allowing reserved ingress metadata to be spoofed.

### Impact on Users
- Existing deployments: requests that relied on passing custom `x-restate-*`
  headers through the ingress to a service will no longer receive them. This is
  intentional; the namespace is reserved.
- New deployments: no action needed.

### Migration Guidance
If you were forwarding custom metadata to a service via `x-restate-*` headers,
rename those headers to a non-reserved prefix (any name that does not start with
`x-restate-`).

### Related Issues
- Issue #4187: Ingress should overwrite `x-restate-*` headers if present
