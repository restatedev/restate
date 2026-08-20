# Release Notes for Issue #5151: Cache GCP ID-token credentials instead of tokens

## Bug Fix

### What Changed

GCP ID-token minting for HTTP deployments (Cloud Run, Cloud Functions, and similar Google-fronted
endpoints) now caches the underlying credential objects, not just the minted token strings, in a
process-wide registry keyed by `(impersonation target, audience)`. Impersonated identities share a
single ambient source credential — the identity used to authenticate the impersonation call itself
— instead of each building their own.

### Why This Matters

The `google-cloud-auth` library starts a background refresh task every time a credential is built.
Restate previously discarded credentials right after minting a token, so each mint attempt — or,
during a GCP outage, every retried attempt — could strand a refresh task that never exits.

### Impact on Users

No configuration changes are required. Cached credentials proactively refresh while in use, which
is a small amount of background mint traffic that did not exist before, in exchange for
steady-state mint calls never waiting on network I/O.

### Known Limitation

A refresh task that is already mid-fetch when its cache entry is evicted is not cancelled
immediately; it keeps running until the fetch resolves.

### Related Issues

- Issue #5151: GCP ID-token mint failures can exhaust process threads
