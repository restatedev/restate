# Distributed DataFusion prototype

Experimental query-engine crate copied from `restate-storage-query-datafusion` at
`08e715ceb960` (`[Datafusion][6/N]`). It shares `restate-storage-query-api` with the
existing engine. `DataFusionEnv::with_distributed_execution` opts into owner-bound
source stages executed by `datafusion-distributed` over Restate RPC. Register
`DistributedQueryServer` on each participating owner. Partition and node sources
share predicate domains, source descriptors, and task transport. Partition work
covers every selected owner even when query parallelism is smaller than owner count.
Storage-placement options are validated by both the locator and worker binding.

Distributed query tasks require network protocol V5 and query task protocol v1
on every participating node. Installation checks the task version and output schema.

For staging A/B requests, start participating nodes with
`experimental-enable-query-engine-v2 = true`, then select the engine on
`POST /query` using `X-Restate-Query-Engine: v1` or `v2` (distributed).
Also set `experimental-enable-query-engine-v2-default = true` on admin nodes to
default headerless requests to the distributed engine; explicit headers override it.
Query responses identify the selected engine and include planning duration in
`Server-Timing`. Use `EXPLAIN VERBOSE` and `EXPLAIN ANALYZE VERBOSE` to inspect plans
and execution.

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
runs the same persisted invocation data through v1 and distributed v2, with aggregate
dynamic-filter pushdown enabled and disabled. It is ignored by default while
#5214 is unresolved; run it explicitly with:

```sh
cargo nextest run -p restate-distributed-datafusion --all-features issue_5214 \
  --run-ignored ignored-only --success-output immediate
```
