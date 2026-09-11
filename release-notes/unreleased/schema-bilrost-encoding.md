# Release Notes: Bilrost encoding for schema metadata

## New Feature

### What Changed

The schema metadata object — the registry of deployments, services, subscriptions and Kafka
clusters — can now be stored as zstd-compressed bilrost instead of flexbuffers. This is an
opt-in experimental feature:

```toml
[common]
experimental-enable-schema-bilrost-encoding = true
```

or via environment variable:

```
RESTATE_experimental_enable_schema_bilrost_encoding=true
```

Changing this option at runtime has no effect; it is only picked up on restart. Restate reads both
encodings regardless of how the option is set, so the stored schema is rewritten in the new
encoding the next time it changes (a deployment registration, a subscription change, etc.).

This option is planned to become the default in v1.9.0.

This release also drops support for reading the legacy (v1) schema format and the one-time
migration that rewrote it. v1.7.x performed that migration automatically on start-up, so clusters
upgrading from v1.7.x are unaffected; clusters on a release older than v1.7.0 must upgrade through
v1.7.x before v1.8.0.

### Why This Matters

Schema metadata is read by every node and grows with the number of registered deployments,
services and handlers. Bilrost plus zstd produces a substantially smaller payload than
flexbuffers, which reduces metadata-store footprint and the cost of distributing the schema
across the cluster.

### Impact on Users

- Default behaviour is unchanged. Without the option set, schemas are still written as
  flexbuffers.
- `restatectl metadata get` and `restatectl metadata patch` can only read flexbuffers-encoded
  values. Once the schema has been written with the new encoding, those two commands can no longer
  read or patch it.
- Restate versions older than v1.8.0 do not understand the new encoding and will fail to read a
  schema that has been rewritten with it.
- Upgrading directly from a pre-v1.7.0 release to v1.8.0 fails, because v1.8.0 no longer reads the
  v1 schema format.

### Migration Guidance

If you are on a release older than v1.7.0, upgrade through v1.7.x first: start the cluster on the
latest v1.7.x release and let it come up healthy — that is when the schema is migrated — then
upgrade to v1.8.0.

Enable the new encoding only after every node in the cluster runs v1.8.0 or later:

1. Complete the rollout of v1.8.0 to all nodes.
2. Set `experimental-enable-schema-bilrost-encoding = true` and restart the nodes.

Rolling back is not automatic. To downgrade to a release older than v1.8.0, first unset the option
and restart, then trigger a schema change (for example by registering a deployment) so the schema
is rewritten as flexbuffers, and only then downgrade.
