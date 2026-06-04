# Release Notes for Issue #4879: Restore ingestion pipelining

## New Feature / Breaking Change

### What Changed
Cross-batch pipelining for the internal ingestion path (Kafka ingress and shuffle forwarding
records to partition leaders) has been restored. Previously, the conservative fix for #4810
limited each partition session to a single unacknowledged batch in flight, capping throughput at
roughly `batch_size / RTT`. A session can now keep multiple batches in flight while still
guaranteeing that a producer's records reach the partition log in the order they were produced.

In-order delivery is preserved by pinning every in-flight request to the leader epoch the client
last observed (`target_leader_epoch`) and processing replies strictly head-first: a leadership
change rejects the entire in-flight pipeline atomically (with the new `NotLeaderWithEpoch`
response), and the session replays the rejected batches in order against the new epoch.

This required a new network protocol version, `V4`. Clients differentiate `V4` partition
processors (pipelining) from `V3` ones (sequential fallback, one batch at a time).

### Why This Matters
Ingestion throughput is no longer bounded by round-trip latency, substantially improving Kafka
ingress and cross-partition shuffle throughput, without reintroducing the out-of-order append /
record-loss problem from #4810.

### Impact on Users
- New deployments: pipelining is used automatically once all nodes speak protocol `V4`.
- Existing deployments: take effect after upgrade; no configuration changes required.
- During a rolling upgrade, a `V1.7` partition leader **rejects** ingestion requests coming from
  pre-`v1.7` ingestion clients (only if the experimental ingestion feature was enabled) which do 
  not send a leader epoch. These requests are **rejected, not lost**: the affected clients keep 
  retrying (with back-pressure) and resume successfully once they are upgraded. 
  This is intentional, to avoid data loss from clients that cannot participate in the 
  ordering protocol.

### Migration Guidance
- No configuration changes are required.
- To minimize the rejection window during a rolling upgrade, upgrade nodes so that ingestion
  clients (ingress/worker nodes) reach `v1.7` together with or shortly after the partition
  processors they talk to. Ingestion from not-yet-upgraded clients to upgraded leaders is paused
  (retried) until those clients are upgraded.

### Related Issues
- Issue #4879: Restore ingestion pipelining
- Issue #4810: Record loss caused by out-of-order commits (the regression this builds on)
