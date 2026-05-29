# Release Notes: `restatectl provision --disable-feature`

## New Feature

### What Changed

`restatectl provision` now enables all available cluster-wide features by
default at bootstrap time. Operators can opt out of specific features with the
new `--disable-feature` flag. The resulting feature set is persisted in
`NodesConfiguration` and cannot be changed after provisioning.

```bash
# Provision with all default features enabled (default behavior)
restatectl provision

# Explicitly opt out of a feature
restatectl provision --disable-feature controlled-idempotent-sharding
# or comma-separated / repeated
restatectl provision --disable-feature controlled-idempotent-sharding,other-feature
```

The dry-run preview lists the features that will be enabled, as well as any
features explicitly disabled via the flag. The enabled-feature list is computed
on the server and echoed back in the response, so older `restatectl` builds
will continue to provision new default features added in future server
releases — the operator only needs to express what they want *off*.

### Impact on Users

- Existing deployments: no change. Clusters that were provisioned before this
  release continue to behave exactly as before — their feature set is frozen
  in `NodesConfiguration` at the version they were provisioned with.
- New deployments: all default features are enabled out of the box. Pass
  `--disable-feature` only if you need to opt out of a specific feature.

### Migration Guidance

None — the flag is optional. Use it only if you need to disable a feature that
is on by default.

The current set of features enabled by default:

- `controlled-idempotent-sharding` — changes how idempotent invocations
  on unscoped services map to partition keys. Without the feature, the
  partition key is derived by hashing the idempotency key directly, spreading
  invocations across the full partition-key space. With the feature enabled,
  each unscoped service has a bounded, deterministic set of 255 partition-key
  buckets, and an idempotent invocation is routed to one of those buckets
  based on its idempotency key.

  This is a one-way decision: once a cluster has accepted idempotent
  invocations under a given sharding scheme, changing the scheme would
  re-shard those invocations onto different partitions and break
  deduplication. That is why the feature set is persisted in
  `NodesConfiguration` and cannot be toggled after provisioning.
