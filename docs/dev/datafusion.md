# DataFusion Query Engine

This document describes how the SQL query engine works in Restate, covering the architecture, the path of a query through the system, optimizations, and future improvement opportunities.

## Overview

Restate integrates [Apache DataFusion](https://datafusion.apache.org/) to provide SQL querying capabilities over internal state. The query engine is exposed via the `/query` endpoint on the admin service and is used by:

- **CLI**: The `restate sql` command for interactive queries
- **Web UI**: For introspecting invocations, state, and other system data

The query engine enables users to inspect invocation status, state, journals, and other internal data using standard SQL.

## Architecture

### Two-Tier Distributed Scanning

The query engine uses a two-tier architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Admin Node                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    QueryContext                           │  │
│  │  - DataFusion SessionContext                              │  │
│  │  - SQL parsing, planning, optimization                    │  │
│  │  - Accumulations, joins, aggregations                     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                    ┌─────────┴─────────┐                        │
│                    │                   │                        │
│              Local Scan          Remote Scan                    │
│           (colocated PP)       (via network)                    │
└────────────────────┼───────────────────┼────────────────────────┘
                     │                   │
                     ▼                   ▼
          ┌──────────────────┐   ┌──────────────────┐
          │  Worker Node A   │   │  Worker Node B   │
          │  (Partition 1)   │   │  (Partition 2)   │
          │                  │   │                  │
          │  RocksDB scan    │   │  RocksDB scan    │
          │  + deserialize   │   │  + deserialize   │
          │  on IO threads   │   │  on IO threads   │
          └──────────────────┘   └──────────────────┘
```

**Key principle**: The query engine (accumulations, joins, operators) always runs on the admin node. Workers are "dumb" - they only scan data from RocksDB and apply post-scan filters.

### Key Components

| Component | Location | Description |
|-----------|----------|-------------|
| `QueryContext` | `storage-query-datafusion/src/context.rs` | Central orchestrator wrapping DataFusion's `SessionContext` |
| `PartitionedTableProvider` | `storage-query-datafusion/src/table_providers.rs` | DataFusion `TableProvider` for partitioned tables |
| `RemoteScannerManager` | `storage-query-datafusion/src/remote_query_scanner_manager.rs` | Routes scans to local or remote partitions |
| `RemotePartitionsScanner` | `storage-query-datafusion/src/remote_query_scanner_manager.rs` | Decides local vs remote execution per partition |
| `LocalPartitionsScanner` | `storage-query-datafusion/src/partition_store_scanner.rs` | Scans local RocksDB partitions |
| `ScanLocalPartition` | `storage-query-datafusion/src/partition_store_scanner.rs` | Trait defining how each table scans its data |
| `ScannerTask` | `storage-query-datafusion/src/scanner_task.rs` | Server-side task handling remote scan requests |

### Tables

**Partitioned tables** (data distributed across partitions by partition key):
- `sys_invocation_status` - Invocation metadata and status
- `sys_invocation_state` - Runtime invocation state (retry info, in-flight status)
- `keyed_service_status` - Virtual object lock status
- `state` - User state key-value pairs
- `journal` - Journal entries
- `journal_events` - Decoded journal events
- `inbox` - Pending invocations in inbox
- `idempotency` - Idempotency key mappings
- `promise` - Promise completions

**Non-partitioned tables** (cluster-wide metadata):
- `deployment` - Registered deployments
- `service` - Registered services
- `node` - Cluster nodes
- `partition` - Partition metadata
- `partition_replica_set` - Partition replica distribution
- `log` - Bifrost logs
- `partition_state` - Partition processor states

**Views**:
- `sys_invocation` - Joins `sys_invocation_status` and `sys_invocation_state` with computed status

## Query Path: `invocation_status` Example

Let's trace a query like:
```sql
SELECT * FROM sys_invocation_status WHERE id = 'inv_1abc...'
```

### 1. SQL Parsing and Planning

The `QueryContext::execute` method handles SQL execution:

```rust
let statement = state.sql_to_statement(sql, &Dialect::PostgreSQL)?;
let plan = state.statement_to_plan(statement).await?;
let df = self.datafusion_context.execute_logical_plan(plan).await?;
df.execute_stream().await
```

### 2. Partition Key Extraction

Before scanning, we extract partition keys from the WHERE clause to enable partition pruning. The `FirstMatchingPartitionKeyExtractor` in `partition_filter.rs` tries multiple extraction strategies:

For `sys_invocation_status`, we can extract partition keys from:
- `partition_key` column directly
- `target_service_key` column (hashed to partition key)
- `id` column (invocation ID contains partition key)

If partition keys are extracted, we only scan those specific partitions rather than all partitions.

### 3. Physical Partition to Logical Partition Mapping

Physical partitions (Restate partitions) are mapped to logical partitions (DataFusion execution partitions) by `physical_partitions_to_logical` in `table_providers.rs`. If fewer partitions than `target_partitions`, they map 1:1. Otherwise, physical partitions are round-robined across logical partitions.

If multiple ids are provided via `IN` lists, each partition key becomes its own "physical partition" with a single-key range, enabling efficient multi-point-reads.

### 4. Partition Location Decision

For each partition, `RemotePartitionsScanner::scan_partition` decides whether to scan locally or remotely:

```rust
match self.manager.get_partition_target_node(partition_id)? {
    PartitionLocation::Local => {
        // Use LocalPartitionsScanner directly
        scanner.scan_partition(...)
    }
    PartitionLocation::Remote { node_id } => {
        // Use remote_scan_as_datafusion_stream
        remote_scan_as_datafusion_stream(...)
    }
}
```

### 5a. Local Scan Path

When the partition is local, `LocalPartitionsScanner` coordinates the scan:

1. Get `PartitionStore` from `PartitionStoreManager`
2. Call the table's `ScanLocalPartition::for_each_row` implementation (e.g., `for_each_invocation_status_lazy`)
3. Deserialize rows, build Arrow record batches
4. Send batches back via channel to DataFusion

The scan and deserialization happen on background storage threads to avoid blocking the tokio runtime (see [IO Thread Execution](#io-thread-execution) below).

### 5b. Remote Scan Path

When the partition is remote, `remote_scan_as_datafusion_stream` manages the RPC protocol:

1. **Open scanner**:
   - Establish connection to target node
   - Send `RemoteQueryScannerOpen` RPC with table name, schema, range, predicate, batch size
   - Receive `ScannerId` back

2. **Stream batches**:
   - Client sends `RemoteQueryScannerNext` with optional updated predicate
   - Server's `ScannerTask`:
     - Pulls batch from local scanner stream
     - Applies post-scan filter (predicate evaluation)
     - Encodes batch to Arrow IPC format
     - Sends back via RPC
   - Client decodes batch and yields to DataFusion

3. **Cleanup**:
   - Scanner auto-closes on drop (sends `RemoteQueryScannerClose`)
   - Server-side scanner has 60-second inactivity timeout
   - Monitors peer health - disposes scanner if peer dies

### 6. Filter Application

Filters are applied at multiple points:

1. **Partition pruning** (before scan): Extract partition keys from predicates to avoid scanning irrelevant partitions
2. **Post-scan filtering** (remote scanner only): Applied by `ScannerTask` after deserialization but before sending over network
3. **DataFusion FilterExec** (if needed): Applied by DataFusion after receiving batches

## Optimizations

### Partition Pruning

Queries with `id = '...'`, `target_service_key = '...'`, or `partition_key = N` predicates skip irrelevant partitions entirely.

**Multi-point reads** with `IN` lists create single-key ranges per partition key:
```sql
SELECT * FROM sys_invocation_status WHERE id IN ('inv_1...', 'inv_2...', 'inv_3...')
```

This translates to individual RocksDB point lookups rather than scans, which is extremely efficient.

### Efficient Two-Query Pattern

For UI/CLI listing scenarios:

1. First query: Get IDs with minimal columns (timestamps, status)
   ```sql
   SELECT id, modified_at FROM sys_invocation_status ORDER BY modified_at DESC LIMIT 100
   ```

2. Second query: Get full details for selected IDs
   ```sql
   SELECT * FROM sys_invocation_status WHERE id IN ('inv_1...', 'inv_2...', ...)
   ```

The second query uses multi-point reads - each ID resolves to a single partition key, and we seek directly to that key in RocksDB. No scanning of unneeded rows.

This pattern trades consistency (data can change between queries) for performance, avoiding expensive materialization of all columns for rows that don't meet the filter.

### Dynamic Filter Pushdown

DataFusion's dynamic filter pushdown feature tightens predicates during execution. This is powerful for top-k queries:

```sql
SELECT * FROM sys_invocation_status ORDER BY modified_at DESC LIMIT 10
```

As the top-k operator accumulates results, it updates the minimum `modified_at` threshold. This updated predicate propagates to the remote scanner (via `next_predicate` in the RPC protocol), which applies it to subsequent batches.

**Result**: After the first few batches, very few rows pass the filter and need to be sent over the network.

### IO Thread Execution

RocksDB iteration and protobuf deserialization happen on background storage threads, not the tokio runtime. This prevents blocking async tasks.

The mechanism works as follows:
1. Each table implements the `ScanLocalPartition` trait, defining a `for_each_row` method
2. These implementations call iterator methods on `PartitionStore` (e.g., `for_each_invocation_status_lazy`)
3. Under the hood, these use `iterator_for_each` which offloads execution to a background storage task via `run_background_iterator` in the `restate-rocksdb` crate
4. Batches are sent back to tokio-land via channels, with scheduling overhead only at batch boundaries

## Current Limitations and Future Improvements

### Main Bottleneck: `invocation_status` Deserialization

The `sys_invocation_status` table is the primary performance bottleneck. This table is critical because:
- It powers `restate inv ls` and the UI invocations page
- It may contain millions of rows in production systems, although almost all are in 'completed' status which is often skipped.
- Unlike most other tables, we regularly need full scans across all partitions instead of looking up a particular ID or key

Other tables don't have the same problem:
- **Journal**: Always queried for a single invocation ID, so only a handful of rows
- **State**: Values aren't proto-encoded; the state value is simply raw bytes in RocksDB passed directly to the DF batch

For `invocation_status`, we've implemented lazy deserialization via the `v1::lazy` module in `restate-types/src/protobuf_types.rs`. This defers deserialization of nested fields and UTF-8 validation until the field is actually accessed. Most other tables still use standard prost deserialization since their access patterns don't require optimization.

Even with lazy deserialization, significant bottlenecks remain:

1. **Must read entire top-level message**: Even to access one field, we parse all top-level varints, as this is the only way to see what fields are present. Only nested messages are kept serialized until accessed.

2. **No filter-before-deserialize**: All filtering happens after full row materialization. We cannot skip rows based on status or timestamp without first deserializing.

3. **Expensive fields**: Some fields require additional work:
   - Nested proto messages that must be fully deserialized when accessed, and if the filters use them, then this happens on every row. For example, the `target_service_name` comes from a nested message with 4 fields.
   - Formatting - what we put in the row isn't always what comes out of the scan. For example, invocation IDs are always formatted to a string, an expensive base62 operation which happens on every row.

### Potential Improvements

**Near-term (with current proto format)**:

- Read specific fields before full deserialization if they appear early in the proto stream
- Deliberately write filterable fields (status, timestamps) early in the proto encoding
- Add separate metadata that's quick to read for filtering decisions

**Longer-term (new serialization format)**:

A format like FlatBuffers would allow:
- Reading individual fields, even deeply nested ones, with near-zero CPU cost
- Filtering before materializing the full row
- Significant reduction in deserialization overhead

**Challenge**: DataFusion filters are opaque functions that operate on complete Arrow batches. Supporting pre-deserialization filtering would require parsing DataFusion expressions to extract simple comparisons (status equality, timestamp comparisons) and checking them individually before full materialization.

### Lack of Indices

The partition stores have no secondary indices or pre-computed aggregates. Many common queries require full scans when they could be answered directly from maintained state:

- **Counts by status**: `SELECT status, COUNT(*) FROM sys_invocation_status GROUP BY status` requires scanning all rows, but we could maintain per-status counters
- **Total state size**: Calculating total state bytes requires scanning all state entries
- **Non-completed invocations**: Listing or counting active invocations requires scanning all invocations. If the RocksDB key space separated invocations by status, we could efficiently scan only non-completed ones

### Aggregation Optimization

Currently, aggregations like:
```sql
SELECT status, COUNT(*) FROM sys_invocation_status GROUP BY status
```

Send all matching rows back to the admin node, even though counting could happen per-partition.

**Future optimization**: Partition the execution plan by node, send partial plans to workers, aggregate locally, then combine results. This would require:
- Encoding per-node execution plans to protobuf
- Distributing plans over the network
- Executing locally and returning partial results
- Cross-partition accumulation on the coordinator

This is significantly more complex than the current architecture (which only pushes filters, not operators).

### Remote Scanner Bottleneck

The remote scanner pulls one batch at a time with no pipelining. For queries that don't benefit from dynamic filter pushdown, this can be slow.

Potential improvements:
- Larger batch sizes (we have 128 which is pretty small, but this was set partly to keep memory usage down)
- Coalesce batches in the server, split them up again in the client
