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
  engine with the row count;
- `restatectl sql --engine v2` does the same for the operator-level `cluster`
  tables, and reports the engine too.

New options, all off by default:

- `experimental-enable-query-engine-v2` starts V2's query task service and
  allows selecting it;
- `experimental-enable-query-engine-v2-default` makes V2 the engine for
  queries that don't select one, and enables V2 on the node too. That covers
  the admin API, `restate sql` and `restatectl sql` without `--engine`, the
  other `restate` commands, and the `restatectl` commands that read the
  cluster's tables, such as `status`, `partitions list` and `logs list` with
  `--preview`. Those commands show a warning when a node couldn't answer;
- `experimental-enable-query-operator-tables` lets requests with
  `X-Restate-Query-Level: operator` query the cluster tables, except
  `config`, in both engines;
- `admin.query-engine.batch-compression` compresses the rows V2 sends from
  one node to another with `lz4` or `zstd`. Use it when the network between
  your nodes is slow or billed by volume. Nodes with different settings work
  together. It has no effect while `networking.disable-compression` is
  `false`, because the connections compress everything already.

### Why This Matters
V2 is for comparing the two engines on staging clusters: the same query, sent
once with `v1` and once with `v2`, should return the same rows.

### Impact on Users
- Queries keep running on V1 unless you enable
  `experimental-enable-query-engine-v2` (or its `-default` variant). The
  operator-level cluster tables
  have their own option, `experimental-enable-query-operator-tables`, which
  also works with V1 alone.
- V2 uses the scheduler's queue stages on its own, with no option to set: for
  example, `sys_vqueues` filtered by a queue reads only that queue's entries.
  The entry indexes need `experimental-enable-indexes`. Indexes aren't tables
  you can query; `EXPLAIN` shows which one a query reads.
- A query that names specific queues, such as
  `SELECT * FROM sys_vqueue_meta WHERE id IN (…)`, makes V2 read those queues'
  metadata directly instead of scanning the metadata stored between them.
  The same goes for `sys_locks` when a query names both the scope and the
  lock name, such as `WHERE scope IS NULL AND lock_name IN (…)` for unscoped
  locks. Naming only the lock name, or more than 10,000 scope and lock name
  combinations, still scans.
- V2 doesn't serve every table yet. It has no `sys_inbox`, and in the
  operator-level `cluster` schema it serves only `nodes`, `partitions`,
  `partition_replica_set`, `partition_state`, `logs`, `logs_tail_segments`,
  `loglet_workers`, `bifrost_read_streams`, `registry` and `migration_jobs`.
  Query the others with `v1`.
- Both engines serve `config`, each node's running configuration with
  secret-looking values redacted, to `restatectl sql` only. The admin `/query`
  API never shows it, not even at the operator level, and
  `disable-config-sql-table` hides it in both engines.
- **V2's `partition_state` asks the workers.** V1 shows the cluster
  controller's view, so it's empty on nodes that don't run the controller. V2
  asks every worker for its partition processors' state and works on every
  node. It has a row for every replica the node expects, even when no answer
  came back, and a new last column, `report`, says what each row is:
  - `reported`: the worker's answer;
  - `node_unavailable`: no complete answer from that node. It wasn't asked
    (gossip sees it as dead, or it isn't a worker), or it failed, or it didn't
    answer within `networking.http2-keep-alive-timeout` (3 seconds by
    default);
  - `not_reported`: the worker answered, but had nothing for this partition;
  - `no_known_replicas`: the node knows no replica of the partition.

  Only `reported` rows have processor values, such as `effective_mode` or
  `applied_log_lsn`; the others have NULL there. To compare the
  engines, select `WHERE report = 'reported'` in V2. `restatectl sql` prints a
  warning for each node without a complete answer.
- **V2 doesn't bound the bytes of a batch yet.** A query over large rows, such
  as big journal entries or state values, can fail when a batch exceeds
  `networking.message-size-limit` (32 MiB by default). Bounding this is
  planned work.
- Both engines share the node's query limits: one memory pool of
  `admin.query-engine.memory-size`, and one `/query` rate limit
  (`admin.query-engine.rate-limiting`).
- `restate sql --engine v2` and `restatectl sql --engine v2` fail when the
  server doesn't confirm that V2 served the query, for example a server older
  than v1.8.2, which ignores the selection.
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
