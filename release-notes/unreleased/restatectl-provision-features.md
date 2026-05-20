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

- `unscoped-idempotent-service-bucketing` — currently a placeholder; not yet consumed by
  the server. Listed so that operators can opt in during provisioning ahead of
  the feature landing.
