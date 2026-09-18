# Release Notes: HTTP/2 keep-alive on the ingress endpoint

## Behavioral Change

### What Changed

The ingress endpoint now sends HTTP/2 PING frames to connected clients and closes connections whose
pings go unanswered. Two new configuration options control this:

- `ingress.http2-keep-alive-interval` — how often a PING is sent. Defaults to `40s`. Setting it to
  `0s` disables keep-alive pings entirely.
- `ingress.http2-keep-alive-timeout` — how long to wait for the acknowledgement before closing the
  connection. Defaults to `20s`.

Both options are read when a connection is accepted, so a configuration reload applies to new
connections without restarting the server.

### Why This Matters

The ingress now serves long-running gRPC ingestion streams. Such a stream stays open across idle
stretches where no frame flows in either direction, and idle connections are exactly what the
network reclaims: NATs, load balancers and firewalls commonly drop a flow that has been quiet for a
few minutes, without notifying either side. Periodic PING frames keep the connection continuously
active, so an ingestion stream survives its idle periods instead of being silently torn down
mid-session.

Detecting peers that are already gone falls out of the same mechanism. Previously the ingress had no
liveness check on inbound HTTP/2 connections. When a client disappeared without closing its
connection cleanly — a crashed process, a laptop going to sleep, a NAT or load balancer silently
dropping the flow — the server kept the connection and all of its in-flight streams open until the
operating system's TCP timeout expired, which can take many minutes, consuming stream slots and
holding request state that could not make progress. With keep-alive enabled, such connections are
now detected within roughly a minute and their resources are released.

### Impact on Users

- **Existing deployments**: keep-alive is enabled by default after upgrading. Clients that respond
  to HTTP/2 PING frames — which every conformant HTTP/2 client does, including the Restate SDKs,
  `curl`, and common gateways — are unaffected.
- **Deployments behind intermediaries**: most gateways and proxies terminate HTTP/2 and do not
  propagate keep-alive between downstream and upstream hops. In those setups the pings verify the
  connection to the gateway, not to the end client.

### Migration Guidance

No action is needed for the default behavior.

To make detection more aggressive, or to relax it on high-latency networks:

```toml
[ingress]
http2-keep-alive-interval = "40s"
http2-keep-alive-timeout = "20s"
```

To restore the previous behavior and disable keep-alive pings:

```toml
[ingress]
http2-keep-alive-interval = "0s"
```
