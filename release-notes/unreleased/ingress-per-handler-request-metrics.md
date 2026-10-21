# Release Notes: Opt-in per-handler ingress request metrics

## New Feature

### What Changed
The ingress request metrics `restate.ingress.requests.total` and
`restate.ingress.request_duration.seconds` can now be broken down per handler. Setting
`ingress.detailed-request-metrics = true` adds the `rpc.method` label, holding the invoked
handler name, next to the existing `rpc.service` and `rpc.type` labels. Requests for handlers
that do not exist are reported with the value `<unknown>`.

The option is off by default, so nothing changes unless you enable it.

### Why This Matters
Until now, ingress latency and request counts were only available per service. Services with a
small, fixed set of handlers can now observe latency and error rates for each handler
individually.

### Impact on Users
- Existing deployments: no change unless the option is enabled.
- Enabling the option multiplies the number of exported time series for these two metrics by
  the number of registered handlers per service. Only enable it if that number is manageable.
- The option is read at startup; changing it requires a restart.
