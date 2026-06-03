# Release Notes: HTTP/2 connection pool for service invocations

## New Feature

### What Changed

When the invoker talks to an HTTP/2 service deployment, it now uses a dedicated,
purpose-built HTTP/2 connection pool instead of the generic legacy client. The
pool keeps a set of multiplexed HTTP/2 connections per `(scheme, authority)` and
scales the number of connections up under load rather than funneling all streams
onto a single connection.

Concretely, the pool:

- **Multiplexes and scales per authority.** Each request is routed to a per-authority
  connection set. New connections are opened proactively as the available stream
  capacity across existing connections drops, instead of queueing everything behind
  one connection.
- **Caps streams per connection.** The remote peer's advertised
  `max_concurrent_streams` is bounded by a configurable upper limit (default 128).
- **Evicts idle connections.** Connections idle longer than the configured timeout
  (default 5 minutes) are closed and removed from the pool.
- **Tunable HTTP/2 flow control.** Per-stream and per-connection receive windows and
  the maximum accepted DATA frame size are now configurable.

This pool is used only for traffic where HTTP/2 is explicitly requested for a
deployment. HTTP/1.1 and ALPN-negotiated traffic are unaffected.

### Why This Matters

The legacy client kept at most a single HTTP/2 connection open to each backend. Since
all streams shared that one connection, the number of concurrent invocations was always
capped by the peer's advertised `max-concurrent-streams` — regardless of whether the
backend sat behind a load balancer or not. Once that limit was reached, further
invocations had to queue and wait for an in-flight stream to free up.

With the pool, Restate can open additional connections to the same backend as needed,
so the number of concurrent invocations is no longer bound by a single connection's
`max-concurrent-streams`. Capping per-connection stream concurrency also helps L4 load
balancers spread streams across backends instead of pinning them to one.

### New Configuration

The following options are available under `worker.invoker` (all new in v1.7.0):

| Option                                  | Default | Description                                                              |
|-----------------------------------------|---------|--------------------------------------------------------------------------|
| `http2-streams-per-connection-limit`    | `128`   | Upper bound on per-connection concurrent HTTP/2 streams; caps the peer's advertised `max_concurrent_streams`. |
| `http2-idle-connection-timeout`         | `5m`    | How long a connection may stay idle before it is evicted. `0` disables eviction. |
| `http2-initial-stream-window-size`      | `2 MiB` | Initial flow-control window for received data on each stream (valid range 65535 B .. 2 GiB). |
| `http2-initial-connection-window-size`  | `5 MiB` | Initial connection-level flow-control window for received data; should be ≥ the per-stream window (valid range 65535 B .. 2 GiB). |
| `http2-max-frame-size`                  | `16 KiB`| Largest HTTP/2 DATA frame payload the client will accept (valid range 16 KiB .. 16 MiB). |

Example:

```toml
[worker.invoker]
http2-streams-per-connection-limit = 64
http2-idle-connection-timeout = "10m"
http2-initial-stream-window-size = "4 MiB"
http2-initial-connection-window-size = "8 MiB"
http2-max-frame-size = "32 KiB"
```

### Impact on Users

- **New deployments**: the pool is used automatically for HTTP/2 deployments with the
  defaults above; no configuration is required.
- **Existing deployments**: adopt the new behavior on upgrade. The defaults are chosen
  to be safe for typical setups; the new options only need to be set to depart from them.
- **No protocol or wire change**: services do not need to change. This affects only how
  the invoker manages its outbound HTTP/2 connections.

### Migration Guidance

No action is required. To tune behavior — for example, to lower the per-connection
stream cap when fronting deployments with an L4 load balancer, or to widen the flow-control
windows for large payloads — set the options listed above under `worker.invoker`.
