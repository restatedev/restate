# Release Notes: Cluster SQL tables use the `cluster` schema

## Behavioral Change / Breaking Change

### What Changed

Cluster-operations SQL tables and views now live in `restate.cluster` instead of
`restate.public`. Cluster SQL sessions use `restate.cluster` as their default
namespace.

### Impact on Users

- Unqualified queries such as `SELECT * FROM partitions` continue to work,
  including when sent by older `restatectl` binaries.
- Explicitly qualified queries using `public.partitions` or
  `restate.public.partitions` must use `cluster.partitions` or
  `restate.cluster.partitions` instead. The same change applies to other cluster
  tables and views, including `logs_tail_segments`.
- Cluster SQL metadata listings report `cluster` as the schema name.
- The Admin HTTP `/query` endpoint continues to use `restate.public` for user
  tables. This change does not expose cluster tables through that endpoint.

### Migration Guidance

Update SQL scripts and metadata filters that explicitly refer to the `public`
schema for cluster tables. For example:

```sql
-- Previously
SELECT * FROM restate.public.partitions;

-- Now
SELECT * FROM restate.cluster.partitions;

-- Unqualified form, supported before and after this change
SELECT * FROM partitions;
```

No client upgrade is required for queries that use unqualified table names.

During a rolling upgrade, use unqualified names until all query-serving nodes
have been upgraded. Older nodes recognize the `public` namespace; upgraded
nodes recognize `cluster`.
