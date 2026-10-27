# Release Notes: Store handler JSON schemas separately from the schema registry

## New Feature

### What Changed
Restate keeps the registered services, including the input/output JSON schemas of every handler,
in memory on every node. Services with large JSON schemas (e.g. generated from big request types)
can make this considerably larger than needed, which costs memory and slows down propagating schema
changes between nodes.

With the new experimental option `experimental-enable-json-schemas-split`, the JSON schemas are
stored once per deployment in the metadata store and loaded only by the Admin API when they are
requested. Nothing changes in the Admin API, the CLI, or the generated OpenAPI documents.

Deployments registered after enabling the option are stored this way right away. Existing
deployments are moved over when a node with the admin role starts with the option enabled.

### Impact on Users
- Disabled by default; existing and new deployments behave as before.
- When enabled, the memory used for registered services shrinks on every node, and schema changes
  propagate faster.
- The option is planned to become the default in v1.9.0.

### Migration Guidance
Only enable the option once all nodes run v1.8.2 or newer:

```toml
[common]
experimental-enable-json-schemas-split = true
```

You can roll back to v1.8.2 at any time. If you roll back to an older version after enabling the
option, the Admin API no longer shows the JSON schemas and OpenAPI documents of the deployments that
were moved, until you re-register those deployments with `--force`.
