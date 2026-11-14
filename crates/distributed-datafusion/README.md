# Distributed DataFusion prototype

Experimental query engine. It shares `restate-storage-query-api` with the
existing engine in `restate-storage-query-datafusion`.
`DataFusionEnv::with_distributed_execution` opts into owner-bound source stages
executed by `datafusion-distributed` over Restate RPC. Register
`DistributedQueryServer` on each participating owner.

Distributed query tasks require network protocol V7 or later. The network fabric
negotiates compatibility; the coordinator rejects an unsupported node before
sending tasks, and workers reject tasks arriving on older connections. Installation
also checks the output schema. Rejection fails the query and leaves the connection
between the nodes open. Once the engine can be selected, keep it off during
upgrades from older custom builds, and turn it on after all participating nodes
have been upgraded.

Nothing selects this engine yet: nodes and the admin `POST /query` endpoint run
the existing engine. Wiring the engine into node setup, and the options that
select it, come in a later change; until then, it runs in this crate's tests
only. Use `EXPLAIN VERBOSE` and `EXPLAIN ANALYZE VERBOSE` to inspect plans and
execution.

Storage reads run on the query IO pool that the existing engine uses too. Pass
it to `register_partition_scanners` or `register_worker_partition_scanners`.

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
