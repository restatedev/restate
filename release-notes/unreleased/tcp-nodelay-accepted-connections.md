# Release Notes: No more delayed-ACK stalls on accepted TCP connections

## Bug Fix

### What Changed

Restate now disables Nagle's algorithm (sets `TCP_NODELAY`) on every TCP connection it accepts:
the node-to-node fabric port, the HTTP ingress port, and the admin API port. Outgoing
connections already did this.

### Why This Matters

Restate writes responses as several small frames. With Nagle's algorithm on, a small frame that
follows an unacknowledged one waits for the peer's delayed acknowledgement, which takes up to
40 ms on Linux. Request/response traffic between nodes, such as remote scans that serve SQL
queries, could stall for tens of milliseconds per exchange.

### Impact on Users

- No configuration changes are needed.
- Latency of node-to-node requests, ingress requests, and admin API calls can drop, mainly on
  Linux.
