# Release Notes for Issue #3306: Native mTLS for fabric inter-node communication

## New Feature

### What Changed

The fabric port (5122) can now be secured with TLS/mTLS at the application layer via a
new optional `[networking.tls]` configuration section:

```toml
[networking.tls]
mode = "strict"                # or "optional" for rolling upgrades
cert-file = "/certs/node.crt"
key-file = "/certs/node.key"
ca-files = ["/certs/ca.crt"]
require-client-auth = true     # default: mTLS enabled
refresh-interval = "1h"        # hot-reload certs from disk

# Authorization: required when require-client-auth is true.
# Use ["*"] for CA-only trust, or identity patterns for subject checking.
allowed-subject-names = ["spiffe://svc.example.com/restate/*"]

# Optional: separate client certs for outbound (inherits from above if omitted)
[networking.tls.client]
cert-file = "/certs/client.crt"
key-file = "/certs/client.key"
root-ca-files = ["/certs/client-ca.crt"]
```

- **Encryption + authentication (mTLS)**: TLS termination at the fabric listener and
  TLS client identity on all outbound fabric connections, with periodic certificate
  hot-reload from disk.
- **Authorization**: after mTLS chain validation, both sides verify the peer
  certificate's Subject CN and SANs (DNS and URI, including SPIFFE IDs) against
  `allowed-subject-names` glob patterns. This prevents unauthorized services holding a
  certificate from a shared CA from connecting.
- **Rolling upgrades**: `mode = "optional"` accepts both plaintext and TLS on the same
  port, allowing a zero-downtime migration: deploy all nodes with `optional` + certs,
  verify, then switch to `strict`.
- TLS-enabled nodes advertise `https://` fabric addresses; peers use the scheme to
  decide the connection type.

### Why This Matters

Previously the fabric port had no transport security and users were expected to secure
it via the network layer (e.g. Kubernetes NetworkPolicy), which many production
environments cannot use. This brings Restate to parity with other distributed systems
(etcd, CockroachDB, Consul) that offer built-in inter-node TLS.

### Impact on Users

- **Existing deployments**: no change — without `[networking.tls]`, behavior is
  identical to today (plaintext).
- **New deployments**: opt in via `[networking.tls]`.
- **Fail-safe validation**: `allowed-subject-names` must be non-empty when
  `require-client-auth = true`; the node refuses to start otherwise. Use `["*"]` to
  explicitly opt into CA-only trust (chain validation without identity checking).
- **restatectl**: in `strict` mode, plaintext connections to port 5122 are rejected;
  `restatectl` needs TLS flags or `optional` mode until the internal/external port
  split (#3583) lands.

### Migration Guidance

Rolling enablement on a live cluster:

1. Deploy all nodes with `mode = "optional"` and certificates — nodes advertise
   `https://` and accept both plaintext and TLS.
2. Verify all inter-node connections use TLS.
3. Switch to `mode = "strict"` — plaintext is rejected.

Note: nodes registered in the nodes configuration keep their advertised address until
they re-register (restart). Enable TLS on all nodes before switching any node to
`strict`.

### Related Issues

- Issue #3306: Support mTLS for cross-node communication
- Issue #3583: Split internal/external gRPC services on port 5122 (related follow-up)
