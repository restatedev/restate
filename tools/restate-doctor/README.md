# Emergency snapshot recovery

`restate-doctor recover` prepares a **fresh single-node cluster** from existing
partition snapshots. It does not require a cluster backup manifest or a running
source cluster, and it adds no server APIs. Start the ordinary `restate-server`
with the generated configuration once preparation has finished.

This is salvage, **not point-in-time recovery**. A successful recovery means the
server can open the recovered state and accept new writes; it does not mean
invocations or external systems are mutually consistent.

## Before starting

- Keep the original nodes stopped or fenced off, especially when preserving the
  original cluster identity. Do not run the original and recovered cluster together.
  **Never start a node with an original data directory against the recovered
  cluster.** Preserving identity is not a server-enforced fence against old disks:
  original membership and log identifiers may still collide. Added nodes must
  use empty directories, even when replacing the original infrastructure.
- Use a new output directory and, for external metadata, a **new isolated
  destination**. The tool refuses existing directories and populated destinations.
- Use a tool/server build compatible with the snapshots. Import checks the
  storage format and the minimum server version persisted in each partition.
- Isolate deployment endpoints and external ingress/connectors until you are ready
  for recovered work to run.
  Restored invocations can replay effects, become stuck, or refer to missing work
  on other partitions. Missing partitions are irretrievably empty in this recovery.
- Use read-only credentials for the source snapshots and source metadata. The
  recovery tool never creates snapshots, retention pins, or writes to either source.

## Recover without source metadata

First inspect the plan. If the selected snapshots cover the complete keyspace,
their partition IDs, ranges, and log IDs supply the topology:

```sh
restate-doctor recover s3://my-bucket/snapshots \
  --output /recovery/attempt-1 \
  --controlled-idempotent-sharding false \
  --dry-run
```

The idempotency-sharding setting must match the **original** cluster. It is
required without source nodes metadata: using today's default could route
idempotent requests to different partitions and bypass recovered deduplication.

To execute, replace `--dry-run` with `--accept-data-loss`. Preparation writes:

- `recovery.json`: selected immutable snapshot metadata, source/target identities,
  topology, empty partitions, actual imported log positions, and completion status.
- `restate.toml`: startup configuration, written **only after successful preparation**.
- `data/recovery/`: partition stores and, by default, built-in metadata storage.

Review the report, then start:

```sh
restate-server --config-file /recovery/attempt-1/restate.toml
```

Check the server's environment overrides before startup: they can override the
generated configuration, including the metadata and snapshot destinations. In
particular, unset `RESTATE_NODE_NAME`: changing the seed node's name can conflict
with its pre-registered node ID and prevent startup. With
Unix listeners, choose a short output path to fit the platform's socket path limit.

The initial cluster has one worker/log node with replication factor one. Its logs
have no original history: they start immediately above each imported partition's
actual applied position, not at LSN 1. The generated configuration also clears a
custom log-server WAL directory, so a copied config cannot reuse the source WAL mount.

Automatic snapshots are disabled so the recovered cluster cannot overwrite the
recovery source. Before expanding or moving partitions off the recovered node,
configure a **separate snapshot destination and snapshot every recovered partition**.
Fresh workers cannot reconstruct the salvaged prefix from empty-history logs alone.
Then use normal cluster operations to expand and increase replication.

## Missing snapshots and explicit selections

The default is the newest discovered immutable metadata record per partition,
ordered by its minimum applied log position, timestamp, and snapshot ID. Discovery
does not depend on `latest.json`. It is not an atomic read of the bucket; source
retention can still delete selected SSTs before they are downloaded. Such a
failure aborts preparation rather than silently changing the selection.

To recover missing partitions empty, supply the original topology. For a standard
equally sized layout this can be a partition count:

```sh
restate-doctor recover s3://my-bucket/snapshots \
  --output /recovery/attempt-2 \
  --num-partitions 64 --allow-missing-partitions \
  --controlled-idempotent-sharding false --accept-data-loss
```

Every available snapshot must agree with that topology. An under-counted partition
count, overlapping ranges, gaps, conflicting log IDs, or mixed cluster identities
are rejected. For a custom topology use `--partition-table original-table.json`,
or read the table from surviving source metadata.

An incomplete/corrupt newest snapshot is **not** silently replaced by another
generation. Choose an older snapshot explicitly, or discard the partition:

```sh
# Paths are relative to the snapshot repository URL.
--snapshot '3=3/00000000000001234567_<snapshot-id>/metadata.json'

# Requires known topology; this partition becomes empty even if snapshots exist.
--skip-partition 3
```

Repeat `--snapshot` for several partitions. `--skip-partition` accepts a
comma-separated list. A selected partition cannot also be skipped.

## Deployment registrations are optional

Without a schema option the recovered registry is empty. State is accessible
through the query API, and deployments can be re-registered for new work.
However, in-flight invocations can be pinned to old deployment IDs. Re-registering
the same URL does **not** guarantee those IDs are restored or that old work resumes.

If source metadata is lost, a selected partition snapshot may still contain a
cached deployment registry:

```sh
--schema-from-partition 0
```

This chooses one cached schema as the authority; it does not merge divergent
registries or establish consistency with other partitions. The tool normalizes
every imported schema cache to that authority so future registrations work.

If source metadata survives, provide its normal connection configuration:

```sh
--source-metadata-config original-metadata-connection.toml --restore-schema
```

The tool reads nodes configuration, topology, optional schema, and limiter rules.
It rebuilds membership, log chains, and leadership rather than copying obsolete
operational metadata. Limiter rules are recovered when source metadata exists;
otherwise they are reset, including partition-local cached copies.

## External metadata and cluster identity

Use `--config-file target.toml` to select the destination backend and server
settings. Source and target configurations are separate. For example:

```toml
[metadata-client]
type = "object-store"
path = "s3://my-bucket/new-recovery-metadata-prefix"
```

The existing clients support object-store, DynamoDB, and etcd destinations.
DynamoDB must use a new table/key prefix; etcd must use a dedicated empty store
(the existing client does not expose a namespace prefix). Built-in metadata is
initialized locally; configured remote built-in metadata addresses are rejected.
This tool does **not** migrate or overwrite an existing metadata backend.

By default recovery generates a new fingerprint and a name ending in `-recovered`.
To take over the original logical identity after infrastructure loss:

```sh
--preserve-cluster-id --original-cluster-stopped
```

This preserves both name and fingerprint, but still creates fresh membership/log
metadata. It does not require reusing the original metadata keyspace. If neither
snapshots nor surviving metadata record the fingerprint, preservation is refused.

Standard AWS credentials are supported. For an S3-compatible endpoint or explicit
options, supply `--object-store-config options.json` using the existing
`ObjectStoreOptions` fields (`aws-region`, `aws-endpoint-url`, `aws-allow-http`, etc.).
The source options are not copied into the generated server configuration.

## Failure handling and validation

Failure leaves the new directory available for inspection and the report marked
incomplete; there is no startup configuration. There is intentionally no retry-in-place
or rollback of a partially provisioned external keyspace. Use another fresh directory
and fresh metadata destination for the next attempt. No original data is deleted.
Downloads have no overall wall-clock deadline; large recoveries can take hours.
Individual object-store requests still use the client's retry policy.

The ordinary integration test exercises lost metadata, deliberately conservative
snapshot log positions, misaligned snapshots, new registrations/writes, restart,
identity preservation, an empty partition, dry-run, and source immutability:

```sh
# Enable the repository's EXISTING test-only file snapshot fixture on the server.
cargo build -p restate-server --features restate-node/test-util
cargo nextest run -p restate-doctor --test recover \
  -E 'test(recovery_without_metadata)'
```

The separate S3/external-metadata test uses a normal server build and a locally
spawned MinIO instance. Install `minio` (or set `MINIO_BIN`); it requires no existing
bucket or cloud credentials:

```sh
cargo build -p restate-server
cargo nextest run -p restate-doctor --test recover --run-ignored all \
  -E 'test(recovery_from_s3)'
```

For the three-node disaster simulation and all four metadata/identity combinations,
see [the local test design](tests/recovery-multinode-design.md). It deletes the
source fixture's entire storage after staggered snapshots, verifies all recovered
counter values, performs concurrent writes, and crash-restarts each recovery:

```sh
cargo nextest run -p restate-doctor --test recover --run-ignored all \
  -E 'test(multinode_s3_disaster_recovery_matrix)'
```
