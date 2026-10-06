# Distributed DataFusion prototype

Experimental query-engine crate copied from `restate-storage-query-datafusion` at
`08e715ceb960` (`[Datafusion][6/N]`). It shares `restate-storage-query-api` with the
existing engine. `DataFusionEnv::with_distributed_execution` opts into owner-bound
storage stages executed by `datafusion-distributed` over Restate RPC. Register
`DistributedQueryServer` on the selected storage owner. This milestone supports
one owner, including several output lanes from one installed task.

Distributed query tasks require network protocol V5 and query task protocol v1
on every participating node. Installation checks the task version and output schema.

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
