# Statistics tables in the default query engine

## New Feature

### What Changed

The default (V1) query engine can now serve `sys_service_stats`,
`sys_deployment_stats` and `sys_virtual_object_stats`, with the same columns
and sums as the experimental V2 engine. They show current entry counts per
service, deployment and virtual object, by stage and status, including
completed entries until they're deleted. Service and deployment counts are
summed over all partitions.

Both engines show the tables only with `experimental-enable-indexes` on the
nodes that run queries. This is off by default. Their queries fail until the
statistics are complete on every partition.

### Why This Matters

You can query statistics without switching to the V2 engine.

### Impact on Users

- The tables appear only with `experimental-enable-indexes` on the nodes that
  run queries. New query sessions see a change; sessions already open keep
  their tables.
- The statistics are built by the same option on workers. Queries succeed once
  they're complete on every partition (status `complete` in
  `restate.cluster.registry`).
- A query fails if any partition it reads has missing or incomplete
  statistics, before it returns any row. It never returns a partial result,
  also with a `LIMIT`.
- Query the tables only after upgrading all nodes. An older node can't serve
  them, and queries that need it fail.
