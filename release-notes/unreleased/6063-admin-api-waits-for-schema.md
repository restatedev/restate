# Release Notes for Issue #6063: Admin API waits for the schema on start-up

## Bug Fix / Behavioral Change

### What Changed
A restarting node no longer answers admin API requests before it has loaded the current schema
(the registered deployments and services). Until then, the admin API, including `/health`, accepts
connections but does not answer them, and SQL queries through `restatectl sql` wait and then answer
`Unavailable` if the node shuts down first.

### Why This Matters
Before this change, a node answered for a short time after start-up (tens to hundreds of
milliseconds, longer with a large schema) as if no deployments were registered. For example,
`SELECT * FROM sys_deployment` returned no rows and `GET /deployments` returned an empty list.
Tools that act on these answers, such as a Kubernetes operator that removes deployments it no
longer sees, could remove deployments that were still in use.

### Impact on Users
- A node's admin API becomes reachable a little later on start-up: once the node has read the
  schema version from the metadata store and loaded that version.
- If the metadata store is unreachable when a node starts, the admin API, and readiness probes
  that use `/health`, stay unanswered until it is reachable again.
- Nothing changes for nodes without the admin role.

### Related Issues
- Issue #6063: Admin API answers `/query` with an empty `sys_deployment` while a restarting node is
  still loading its schema
