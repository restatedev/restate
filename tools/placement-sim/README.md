# Placement Simulator

This is a small deterministic simulator for Restate partition and replicated-loglet placement.
It models homogeneous all-in-one clusters with:

- partition replication factor 2
- replicated-loglet nodeset size 3
- one partition log per partition
- the log sequencer colocated with the partition leader

It compares current per-id HRW placement against load-aware top-N candidate placement for:

- partition replicas
- partition leaders
- replicated-loglet nodeset members

Run:

```shell
cargo run -p placement-sim
```

CSV for spreadsheet analysis:

```shell
cargo run -p placement-sim -- --csv 2> placement.csv
```

This is a model for comparing placement policies. The production selector lives in
`crates/types/src/replication/load_balanced_selector.rs`; keep the simulator's hashing and
top-N behavior aligned with that implementation when changing either side.
