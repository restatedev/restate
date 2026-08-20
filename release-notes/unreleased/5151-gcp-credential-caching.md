# Release Notes for Issue #5151: Cache GCP ID-token credentials instead of tokens

## Bug Fix

### What Changed

GCP ID-token minting for HTTP deployments (Cloud Run, Cloud Functions, and similar Google-fronted
endpoints) now caches the underlying credential objects, not just the minted token strings. Every
distinct `(impersonation target, audience)` identity shares one outer ID-token credential, built
once and reused process-wide, instead of a new credential per mint attempt or per hourly token
refresh. Impersonated identities additionally share a single process-wide ambient source
credential — the identity used to authenticate the impersonation call itself — rather than each
impersonated identity building its own copy. Credential construction runs as a TaskCenter task on
its process-lifetime default runtime, so a credential's background refresh task keeps running even
if the partition or invoker that first requested it is later torn down — and, as a TaskCenter task,
that refresh work now shows up in SIGUSR2 tokio task dumps and TaskCenter's task metrics, where it
was previously invisible on an unmanaged runtime of its own.

### Why This Matters

The `google-cloud-auth` library starts a background token-refresh task every time a credential is
built. Restate previously discarded credentials right after minting a token, so each mint
attempt — or, in the worst case, every retried attempt during a GCP outage — could strand a
refresh task that never exits. Under sustained failures (e.g. the metadata server or IAM
Credentials API being unreachable) this could accumulate background tasks and blocking work
without bound.

### Impact on Users

- No configuration changes are required. GCP-authenticated HTTP deployments behave the same from
  the caller's perspective.
- The admin/discovery path (deployment registration) now caches credentials the same way the
  worker/invoker path always did. A re-registration that changes the audience or impersonation
  target uses a different cache key, so this introduces no staleness risk.
- Cached credentials proactively refresh while in use, which is a small amount of background mint
  traffic that did not exist before, in exchange for steady-state mint calls never waiting on
  network I/O. A credential idle for more than about an hour (no deployments minting against it)
  stops refreshing and is evicted; a credential that a deployment actively uses stays cached
  indefinitely.
- The shared ambient source credential is never idle-evicted (it has no single deployment to go
  idle with). If its background refresh task hits a permanent error and gives up — for example, a
  revoked service-account key — Restate notices the next time an impersonated mint fails and
  rebuilds it automatically; a misconfigured *impersonation target* on an otherwise healthy source
  never triggers a rebuild.
- Restate no longer parses the minted token or rejects one for having too little validity left.
  That check was a carry-over from the old token-string cache, where a minted string could be
  handed out up to an hour after being read; the registry's read path never serves an
  already-expired token (it blocks on the credential's own refresh instead), so the only remaining
  exposure is a token expiring within the single network round trip to the deployment, which Cloud
  Run itself validates on arrival.

### Known Limitation

If a credential's refresh task is genuinely stuck mid-fetch (for example, blocked in a DNS
resolution that never returns) when its cache entry is evicted, that task keeps running until the
stuck fetch resolves rather than being cancelled immediately. This is bounded by the number of
distinct GCP identities Restate has minted for — one ambient source actor for the whole process,
plus at most one outer actor per `(impersonation target, audience)` key — not by the number of mint
attempts, so it cannot grow unbounded the way the original issue's task-per-attempt leak could. A
future `google-cloud-auth` update is expected to close this gap by tearing down a refresh task as
soon as its credential is evicted, regardless of what the task is doing at the time.

### Related Issues

- Issue #5151: GCP ID-token mint failures can exhaust process threads
