# Statistics tables in the default query engine

## New Feature

### What Changed

The default (V1) query engine can now serve `sys_service_stats`,
`sys_deployment_stats` and `sys_virtual_object_stats`, with the same columns
and sums as the experimental V2 engine. They show current entry counts per
service, deployment and virtual object, by stage and status, including
completed entries until they're deleted. Service and deployment counts are
summed over all partitions.

The tables are off by default. Enable them with
`experimental-enable-query-stats-tables` on the nodes that run queries. New
query sessions see them; sessions already open keep their tables.

### Why This Matters

You can query statistics without switching to the V2 engine.

### Impact on Users

- The statistics must exist first: enable
  `experimental-enable-track-status-gauges` on workers and wait until they're
  Ready on every partition (see `restate.cluster.indexes`). Enabling the tables
  doesn't start that, or wait for it.
- A query fails if any partition it reads has missing or incomplete
  statistics, instead of returning a partial total.
- Enable the tables only after upgrading all nodes. An older node can't serve
  them, and queries that need it fail.
