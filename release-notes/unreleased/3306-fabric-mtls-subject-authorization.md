# Release Notes for Issue #3306: Native mTLS for fabric inter-node communication

## New Feature

### What Changed

The fabric port (5122) can now be secured with TLS/mTLS at the application layer via a
new optional `[networking.tls]` configuration section:

```toml
[networking.tls]
mode = "require"               # off | allow | prefer | require
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
- **Rolling enablement**: the mode decouples certificate distribution from advertising
  TLS from requiring it (modeled after MongoDB's `tlsMode`):
  - `off` — TLS disabled; the section may be staged on disk without effect
  - `allow` — certs loaded, TLS and plaintext accepted, still advertises `http://`
  - `prefer` — TLS and plaintext accepted, advertises `https://` so peers dial TLS
  - `require` (default) — only TLS accepted, advertises `https://`
- Nodes in `prefer`/`require` mode advertise `https://` fabric addresses; peers use
  the scheme to decide the connection type.

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
- **restatectl**: in `require` mode, plaintext connections to port 5122 are rejected.
  `restatectl` can present a client certificate via the `RESTATECTL_TLS_CA_FILE`,
  `RESTATECTL_TLS_CERT_FILE`, and `RESTATECTL_TLS_KEY_FILE` environment variables
  (or the corresponding `--tls-*` flags) until the internal/external port split
  (#3583) lands.

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
- Issue #3583: Split internal/external gRPC services on port 5122 (related follow-up)
