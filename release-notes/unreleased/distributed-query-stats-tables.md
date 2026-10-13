# Release Notes: Statistics tables in query engine v2

## Bug Fix

### What Changed

Query engine v2 now exposes `sys_service_stats`, `sys_deployment_stats`, and
`sys_virtual_object_stats`, with the same columns and count semantics as query
engine v1.

### Impact on Users

Queries against these tables no longer fail with missing-table errors when using
v2. Service and deployment statistics retain their cluster-wide aggregation.

### Migration Guidance

No query changes are required. Existing requirements for enabling statistics
collection still apply.
