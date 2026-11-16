# Release Notes: Experimental distributed query engine (V2)

## New Feature

### What Changed
Restate ships a second, experimental SQL query engine, V2, next to the
default one, V1. V2 plans a query on the node that receives it and runs the
parts that read partition data on the nodes that own those partitions,
instead of pulling every row to the receiving node first.

V2 is off by default. When it's enabled, admin `/query` requests choose the
engine with a header:

- `X-Restate-Query-Engine: v1` or `v2` selects the engine for one request;
- the response carries the same header with the engine that served the query,
  and a `Server-Timing` header with the planning time;
- `restate sql --engine v2` sends the header from the CLI and reports the
  engine with the row count.

New options, all off by default:

- `experimental-enable-query-engine-v2` starts V2's query task service and
  allows selecting it;
- `experimental-enable-query-engine-v2-default` makes V2 the engine for
  requests without the header, and enables V2 on the node too;
- `experimental-enable-query-operator-tables` lets requests with
  `X-Restate-Query-Level: operator` query the cluster tables, except
  `config`, in both engines;
- `experimental-enable-query-index-tables` shows the index tables in V2, such
  as `_idx_entry_by_queue`, so you can inspect them with SQL.

### Why This Matters
V2 is for comparing the two engines on staging clusters: the same query, sent
once with `v1` and once with `v2`, should return the same rows.

### Impact on Users
- Queries keep running on V1 unless you enable
  `experimental-enable-query-engine-v2` (or its `-default` variant). The
  operator-level cluster tables
  have their own option, `experimental-enable-query-operator-tables`, which
  also works with V1 alone.
- V2 uses indexes on its own, with or without
  `experimental-enable-query-index-tables`: for example, `sys_vqueues` filtered
  by a queue reads only that queue's entries.
- V2 doesn't serve every table yet. It has no `sys_inbox`, and in the
  operator-level `cluster` schema it serves only `nodes`, `partition_state`,
  `bifrost_read_streams`, `indexes` and `migration_jobs`. Query the others
  with `v1`.
- **V2 doesn't bound the bytes of a batch yet.** A query over large rows, such
  as big journal entries or state values, can fail when a batch exceeds
  `networking.message-size-limit` (32 MiB by default). Bounding this is
  planned work.
- Both engines share the node's query limits: one memory pool of
  `admin.query-engine.memory-size`, and one `/query` rate limit
  (`admin.query-engine.rate-limiting`).
- `restate sql --engine v2` fails when the server doesn't confirm that V2
  served the query, for example a server older than v1.8.2, which ignores the
  selection.
- V2 needs every participating node on v1.8.2 or newer. A node that runs an
  older version rejects V2's query tasks: the query fails, and the nodes'
  connection stays up.

### Migration Guidance
To try V2 on a cluster:

1. Upgrade every node to v1.8.2 with V2 disabled.
2. Set `experimental-enable-query-engine-v2 = true` on every node and restart
   the nodes.
3. Send queries with `X-Restate-Query-Engine: v2`, or run
   `restate sql --engine v2 "..."`.

When upgrading later, disable V2 first, upgrade every node, then enable it
again.

### Examples

```shell
restate sql --engine v2 "SELECT status, COUNT(*) FROM sys_vqueue_entry_status GROUP BY status"
```

```shell
curl -s "$ADMIN/query" -H 'X-Restate-Query-Engine: v2' \
  -H 'Content-Type: application/json' -H 'Accept: application/json' \
  -d '{"query": "SELECT COUNT(*) FROM sys_invocation_status"}'
```
