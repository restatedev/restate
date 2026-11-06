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

The movement report separately counts partition transitions whose old and new replica sets are
fully disjoint. The combined strategy retains one alive current replica when needed.
The model assumes alive replicas are warm; production instead prefers replicas reported active.
It does not model replay lag, pending reconfigurations, placement freezes, or asynchronous health
reports. Replica-set overlap alone is not evidence of uninterrupted service.

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
The nodeset model compares placement plans, not the production watchdog's incremental
reconfiguration and load-range acceptance check. Its results do not establish convergence
under concurrent loglet reconfiguration.
The `repair-restore` scenarios describe the original from-scratch planner, not the
current incremental partition rebalancer. Production now retains fair placements
and accepts only imbalance-reducing partition moves; its scheduler tests exercise
that behavior directly, including partial completion of pending changes.
