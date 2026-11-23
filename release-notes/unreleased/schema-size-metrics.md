# Metric for the size of the schema registry

## New Feature

Every node now reports the uncompressed size of the schema it last loaded as
`restate_schema_size_bytes`.

The time a node needs to load a new schema grows with this size. Every registered deployment adds
to it until the deployment is removed. Use this metric to spot a schema that is growing large, for
example because deployments are registered repeatedly and never removed. No configuration changes
are required.
