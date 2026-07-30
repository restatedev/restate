# Release Notes for Issue #3306: Native mTLS for fabric inter-node communication

## New Feature

### What Changed

The fabric port (5122) can now be secured with TLS/mTLS at the application layer via a
new optional `[tls]` configuration section:

```toml
[tls]
mode = "require"               # off | allow | prefer | require
cert-file = "/certs/node.crt"
key-file = "/certs/node.key"
ca-files = ["/certs/ca.crt"]
require-client-auth = true     # enable mTLS
refresh-interval = "1h"        # hot-reload certs from disk

# Authorization: required when require-client-auth is true.
# Use ["*"] for CA-only trust, or identity patterns for subject checking.
allowed-subject-names = ["spiffe://svc.example.com/restate/*"]

# Optional: separate client certs for outbound (inherits from above if omitted)
[tls.client]
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
- **restatectl support**: `--tls-ca` (env: `RESTATECTL_TLS_CA`) verifies TLS-secured
  fabric ports and is sufficient on its own for clusters without client authentication;
  add `--tls-cert`/`--tls-key` when the cluster sets `require-client-auth = true`.
- **Rolling enablement**: different enforcement modes for gradual rollout:
  - `off` (default) — TLS disabled; the section may be staged on disk without effect
  - `allow` — certs loaded, TLS and plaintext accepted, nodes still advertise `http://`
  - `prefer` — TLS and plaintext accepted, nodes advertises `https://` so peers dial TLS
  - `require` — only TLS accepted, advertises `https://`

### Why This Matters

Previously the fabric port had no transport security and users were expected to secure
it via the network layer (e.g. Kubernetes NetworkPolicy), which many production
environments cannot use. This feature adds native support for TLS in restate.

### Impact on Users

- **Existing deployments**: no change — without `[tls]`, behavior is
  identical to today (plaintext).
- **New deployments**: opt in via `[tls]`.

### Migration Guidance

Rolling enablement on a live cluster, one mode step at a time (each step is applied
to every node — with a restart — before moving to the next):

1. `mode = "allow"` + certificates on all nodes — certs are loaded and TLS is
   accepted, but nodes still advertise `http://`, so peers that have not restarted
   into the TLS config yet can still connect everywhere.
2. `mode = "prefer"` on all nodes — nodes re-register with `https://` addresses and
   peers dial them with TLS. Plaintext is still accepted.
3. Verify all inter-node connections use TLS, then `mode = "require"` — plaintext is
   rejected.

Rollback is the same sequence in reverse. Nodes keep their registered advertised
address until they re-register (restart), which is why `allow` must be fully rolled
out before any node moves to `prefer`.

### Related Issues

- Issue #3306: Support mTLS for cross-node communication
