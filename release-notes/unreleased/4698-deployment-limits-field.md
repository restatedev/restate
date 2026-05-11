# Release Notes for Issue #4698: Endpoint manifest `limits` field

## New Feature

### What Changed

The endpoint manifest schema accepts a new optional top-level `limits` object:

```json
{
  "limits": {
    "invocations": 1024
  }
}
```

`limits.invocations` declares the maximum number of concurrent invocations per
node for the deployment. A value of `0` means unlimited (equivalent to omitting
the field).

The value is plumbed through discovery into the deployment metadata and
preserved across deployment updates, but Restate does **not yet enforce** the
limit at runtime in this release. Enforcement will land in a follow-up.

### Why This Matters

This gives SDK authors a forward-compatible place to declare a per-deployment
concurrency budget that the runtime can begin to honor in a later release.

### Impact on Users

- **Existing manifests**: no action required. The field is optional.
- **SDK authors**: may begin emitting `limits.invocations` in the discovery
  response. Restate accepts and stores the value but does not act on it in
  this release.
- **Operators**: no behavioral change; existing deployments continue to run
  with no concurrency cap (other than the invoker concurrency limit)

### Related Issues

- #4698 — this issue
