# Release Notes: `restatectl provision --features`

## New Feature

### What Changed

`restatectl provision` now accepts a `--features` flag to opt the cluster into
cluster-wide feature flags at bootstrap time. The selected features are
persisted in `NodesConfiguration` and cannot be changed after provisioning.

```bash
restatectl provision --features unscoped-idempotent-service-bucketing
# or comma-separated / repeated
restatectl provision --features unscoped-idempotent-service-bucketing,other-feature
```

The dry-run preview lists the features that will be enabled.

### Impact on Users

- Existing deployments: no change. Features default to an empty set; clusters
  that were provisioned before this release continue to behave exactly as
  before.
- New deployments: opt in via `--features` if a feature is needed at provision
  time.

### Migration Guidance

None — the flag is optional.

The initial set of selectable features is intentionally small:

- `unscoped-idempotent-service-bucketing` — changes how idempotent invocations
  on unscoped services map to partition keys. Without the feature, the
  partition key is derived by hashing the idempotency key directly, spreading
  invocations across the full partition-key space. With the feature enabled,
  each unscoped service has a bounded, deterministic set of 255 partition-key
  buckets, and an idempotent invocation is routed to one of those buckets
  based on its idempotency key.

  This is a one-way decision: once a cluster has accepted idempotent
  invocations under a given bucketing scheme, changing the scheme would
  re-shard those invocations onto different partitions and break
  deduplication. That is why the flag is persisted in `NodesConfiguration`
  and cannot be toggled after provisioning.
