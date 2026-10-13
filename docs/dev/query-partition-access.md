# Query-facing partition access

`PartitionQueryAccess` in `crates/worker-api/src/partition_query.rs` is the native, object-safe
boundary for reading partition-owned live state. It is independent of SQL, Arrow, and the
processor-manager command channel. `ProcessorsManagerHandle::query_access` supplies the live
implementation; offline tools and tests supply other implementations.

## Contract

- Each request names a `PartitionId` and a requested `KeyRange`.
- The live backend resolves that exact partition's registration and intersects the range with
  the registered partition range. An empty intersection issues no downstream request.
- Rows are owned, restricted to that range, and ordered by partition key.
- Missing readers, closed channels, and `NotLeader` responses are errors, not empty results.
- Epochs protect replacement registrations from stale guard drops. A changed registration while
  awaiting a response fails the query before that response's rows are published.
- Dropping the stream drops pending response receivers. No per-query detached worker task is
  created by the native capability; existing leader tasks continue to own request processing.

`PartitionLeaderHandlesRegistry` currently adapts finite snapshots returned by the existing
invoker and leader-query channels into fallible streams. This is not incremental streaming
inside those components, nor does it freeze live state for the lifetime of a query.

## Registration and execution

Worker-role initialization calls `register_partition_scanners` for persisted sources and
`register_live_scanners` with the worker's query-access capability. Catalog construction only
exposes SQL providers and views. Both local plans and incoming scan RPCs find the same registered
local adapters. The routing factory has no local-registration side effects.

Local registration and distributed scanner construction use the same `QueryEngineTable` marker.
Its identity is globally unique across SQL namespaces and remains the wire lookup key. Provider
construction belongs to the table implementation; SQL naming belongs to catalog assembly.
`DataFusionEnv` holds the shared provider inventory, while each session gets independent catalog
containers containing selected provider `Arc`s. Missing inventory entries are omitted at catalog
construction, but failures of an exposed live source still propagate as query errors.

The three live adapters (`sys_invocation_state`, `sys_scheduler`, `sys_user_limits`) use
`LivePartitionScanner` for Arrow batching, limits, cancellation, and error propagation. They
do not access `PartitionStoreManager` to determine partition ranges.

## Offline execution

`OfflinePartitionQueryAccess` has a fixed partition universe and returns empty live-data streams
for known partitions. Unknown partitions still fail. The snapshot tool registers this capability
alongside its restored-store scanners; no fake processor-manager command channel is required.
The snapshot tool uses the same `UserTables` registration as the server, without adding the
optional `MetadataTables` group. All partition-backed tables are available, including
`sys_vqueue_entry_status`. The three live tables are visible but empty offline; the
`sys_invocation` view preserves persisted invocations with null live-state columns.
`sys_service`, `sys_deployment`, and `sys_rules` are absent without cluster metadata.

## Database lifecycle follow-up

Persisted scanners still use the existing `PartitionStoreManager` adapter. This change does not
provide database fencing, draining, or leased RocksDB read views. Their future acquisition must
go through processor-owned admission rather than exposing freely cloneable database handles.
That ownership must cover local and remote scans, iterators, and point reads, allowing a processor
to fence new access and drain existing users before replacing its database with a snapshot.

Live-source registration checks must not be mistaken for that future database-lifetime guarantee.
