# Release Notes: Configurable ingress HTTP/2 max concurrent streams

## New Feature

### What Changed
Added `ingress.http2-max-concurrent-streams`, an optional configuration setting
for the max concurrent HTTP/2 streams advertised per inbound ingress connection.

### Why This Matters
The default remains unchanged and is left to hyper. Operators running
high-concurrency or long-poll workloads behind HTTP/2-aware clients such as
Linkerd can now raise the per-connection stream limit instead of being silently
bounded by the server's advertised default.

### Impact on Users
No action is required for existing deployments. Set
`RESTATE_INGRESS__HTTP2_MAX_CONCURRENT_STREAMS` or the corresponding config file
key only when the ingress needs to accept more concurrent streams per HTTP/2
connection.
