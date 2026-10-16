# Local disaster-recovery proof

The executable owner is `multinode_s3_disaster_recovery_matrix` in `recover.rs`.
This is deliberately best-effort salvage, not a distributed consistent checkpoint.

## Matrix

| Source metadata after disaster | Original identity | New identity |
| --- | --- | --- |
| Object-store namespace survives | Read metadata, restore schema, rebuild operational metadata elsewhere | Same data/schema, different name and fingerprint |
| All built-in Raft replicas lost | Infer topology from snapshots, rebuild empty registry, re-register deployments | Same salvage with different name and fingerprint |

Every source is a real three-node cluster with replicated logs and six partitions.
Four independent counter keys per partition receive concurrent acknowledged writes
through different ingress nodes. Two generations of snapshots are taken. Later
snapshots are interleaved with writes, so there is intentionally no shared cut.
One source node is crash-restarted between generations to change its node
generation and allow possible leadership failover before the selected snapshots.
The test does not assert a particular leader moved.
The independently recorded expected values are the values acknowledged before
each partition's selected snapshot, not values computed from the recovery report.
Afterward more acknowledged writes are made which must NOT reappear on restore.

## Disaster and recovery checks

1. Stop all source nodes and drop their temporary storage directories, including
   every partition replica, log server and built-in metadata replica. Only MinIO
   and the mock deployment endpoint survive. Assert the old cluster directory is
   gone. The external metadata namespace, when used, remains byte-for-byte intact.
2. Invoke the actual recovery executable with fresh output and metadata namespaces.
   The two identity modes reuse exactly the same immutable source snapshots.
3. Before starting any recovered server, assert there is no log-server directory.
   After startup, inspect actual metadata: one new log segment per partition,
   starting at the imported applied position plus one. This is empty log HISTORY,
   not resetting LSNs.
4. Start the unchanged server. Assert its actual metadata identity matches the
   chosen mode. Query all recovered state before any re-registration. In the lost
   metadata cases assert an empty registry, then re-register the endpoint.
5. Check every counter against the independent expected cut; perform concurrent
   new writes, abruptly restart the recovered node, and check persistence and
   another write. Source snapshots and surviving metadata must be unchanged.

## Limits and follow-on stress

The recovery tool intentionally prepares one seed node, not a fully replicated
destination. The multinode fixture exercises loss of replicated source state;
it does not claim destination high availability. A separate expansion proof
should configure a NEW snapshot repository on the recovered cluster, publish
snapshots there, add two workers/log servers, increase replication through normal
cluster operations, and remove the seed. Do not assume an empty-history log can
reconstruct the salvaged prefix on another worker without a recovered snapshot.

This test does not establish cross-partition invariants, exactly-once external
effects, in-flight deployment-ID compatibility, or consistency with a database.
Quiescent backup and consistent cluster snapshots remain later work.

## Running

Use a normal `cargo build -p restate-server`, install MinIO (or set `MINIO_BIN`),
then run the ignored test explicitly with `cargo nextest run -p restate-doctor
--test recover --run-ignored all -E 'test(multinode_s3_disaster_recovery_matrix)'`.
It creates its own local bucket and credentials; no cloud account is involved.
