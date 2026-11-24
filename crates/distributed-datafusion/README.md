# V2 query engine

The experimental distributed query engine (V2). It shares
`restate-storage-query-api` with the existing engine in
`restate-storage-query-datafusion`.
`DataFusionEnv::with_distributed_execution` opts into owner-bound source stages
executed by `datafusion-distributed` over Restate RPC. Register
`DistributedQueryServer` on each participating owner.

The planner reads a table through a complete index when a query restricts the
indexed columns. For example, `sys_vqueues` filtered by queue reads the
scheduler's queue stages, the `_idx_entry_by_queue` index. This needs no option.
The entry indexes, such as invocations by creation time, are built and read only
with `experimental-enable-indexes`. Indexes aren't tables in SQL: `EXPLAIN` shows
which index a query reads.

Distributed query tasks require network protocol V7 or later. The network fabric
negotiates compatibility; the coordinator rejects an unsupported node before
sending tasks, and workers reject tasks arriving on older connections. Installation
also checks the output schema. Rejection fails the query and leaves the connection
between the nodes open. Keep the engine off during upgrades, and turn it on after
all participating nodes have been upgraded.

For staging A/B requests, start participating nodes with
`experimental-enable-query-engine-v2 = true`, then select the engine on
`POST /query` with `X-Restate-Query-Engine: v1` or `v2` (or
`restate sql --engine v2`). Set `experimental-enable-query-engine-v2-default =
true` on admin nodes to use V2 for requests without the header; it enables V2 on
the node too, and an explicit header still wins. Both engines share the node's
query memory pool and `/query` rate limit. Responses name the engine that served the query in the same
header, and the planning time in `Server-Timing`. Use `EXPLAIN VERBOSE` and
`EXPLAIN ANALYZE VERBOSE` to inspect plans and execution.

V2 doesn't bound the bytes of a batch yet: a query over large rows can fail
when a batch exceeds `networking.message-size-limit`.

Storage reads run on the query IO pool. Node setup (`restate-node`,
`query.rs`) passes V1's pool to `register_worker_partition_scanners` and
`register_cluster_scanners`, so both engines share one pool per node.

Run the crate's baseline tests with:

```sh
cargo nextest run -p restate-distributed-datafusion --all-features
```

The differential correctness gate compares the candidate against independent
fixture and broad primary-scan references:

```sh
cargo nextest run -p restate-distributed-datafusion --all-features query_correctness
cargo nextest run -p restate-distributed-datafusion --all-features distributed::
```

The [sparse MIN/MAX regression comparison](src/query_correctness/sparse_min_max.rs)
runs the same sparse `sys_vqueue_entry_status` data through v1 and distributed v2, with aggregate
dynamic-filter pushdown enabled and disabled. It is ignored by default while
#5214 is unresolved; run it explicitly with:

```sh
cargo nextest run -p restate-distributed-datafusion --all-features issue_5214 \
  --run-ignored ignored-only --success-output immediate
```
