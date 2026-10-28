# Release Notes for PR #5675: Configurable schema compression level

## New Feature

### What Changed

A new option, `schema-compression-level`, sets the Zstandard (zstd) compression level Restate uses
for schema data. It always applies to schema updates that nodes send to each other. It applies to
stored schemas only when `experimental-enable-schema-bilrost-encoding` is enabled.

### Why This Matters

Clusters with many deployments and services can have large schemas. A higher level makes schema
data smaller, at the cost of more CPU every time the schema changes (for example, when you register
a deployment). A lower or negative level makes schema changes cheaper but the data larger.

### Impact on Users

- The default is `0`, which uses zstd's default level (currently `3`), the same level Restate
  used before. Nothing changes unless you set the option.
- Accepted values are `-22` to `19`. Values outside that range are clamped to it.
- Nodes read schema data written at any level, so nodes can use different values, and you can
  change the value during a rolling upgrade. Changing it does not rewrite data already stored.

### Migration Guidance

No action is needed. To change the level:

```toml
schema-compression-level = 9
```

or through the environment:

```sh
RESTATE_SCHEMA_COMPRESSION_LEVEL=9
```

### Related Issues

- PR #5675: Make schema zstd compression level configurable
