# Release Notes: Retry policy `on_status_code`

## New Feature

### What Changed
Service/handler retry policies can now define `onStatusCode` rules in the deployment manifest.
Each rule maps a status code returned by the handler to an action — `PAUSE` or `KILL`. When an
invocation fails with a status code matching a rule, that action is applied immediately, overriding
the usual retry behavior. The first matching rule wins; if no rule matches, the usual retry policy
applies. Rules can be set at the service level and overridden at the handler level.

The rules only apply to status codes the handler/SDK explicitly returns — internal Restate errors
(e.g. journal mismatch, protocol violation) keep their normal retry path.

This required bumping the service discovery protocol to **V5**
(`application/vnd.restate.endpointmanifest.v5+json`).

### Why This Matters
Some handler failures are not worth retrying (e.g. a permanent business error). `onStatusCode` lets
users short-circuit the retry loop and immediately pause (for manual intervention) or kill the
invocation based on the status code, instead of waiting for `max_attempts` to be exhausted.

### Impact on Users
- **Existing deployments:** No change. The field is optional and defaults to no rules; existing
  retry behavior is preserved. Deployments discovered with older protocol versions continue to work.
- **New deployments:** SDKs that support discovery protocol V5 can emit `onStatusCode` rules. Older
  SDKs negotiate down to V4 and simply don't send the field.

### Migration Guidance
None required. To use the feature, upgrade to an SDK that supports it and configure
`onStatusCode` on your service/handler retry policy.

### Related Issues
- Retry policy `on_status_code` (pause/kill on specific handler status codes)
