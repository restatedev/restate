# Release Notes: Cloud Tunnel Client Requires a Signed Target Claim

## Behavioral Change

### What Changed

The cloud tunnel client now requires every JWT used for request identity to carry a `target` claim, and it validates that claim against the request's method, path, and query. Previously, the `target` claim was optional: tokens without it were accepted if the `aud` (audience) claim was a suffix of the request path.

The `target` claim is now a mandatory, non-optional field of the token claims, and the `aud`-suffix fallback check has been removed. Since `target` fully binds the method, path, and query, the separate audience suffix validation is no longer needed.

### Why This Matters

A current restate-server signs a `target` for every proxied call, so the client can now rely on it to fully authorize each request. The `aud`-suffix fallback only existed to tolerate tokens from servers that predate the `target` claim; such tokens expire within minutes.

### Impact on Users

- Tokens issued without a `target` claim are now rejected with a JWT validation error
  (`Could not validate JWT: ...`), because `target` is a mandatory field the JWT must carry,
  instead of being allowed via the `aud` fallback.
- No configuration change is required. Deployments using a current restate-server are unaffected because those servers always sign a `target`.

### Migration Guidance

None required for current deployments. If you run a restate-server version that predates the `target` claim, upgrade it so tokens continue to be accepted.
