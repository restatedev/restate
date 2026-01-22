# Release Notes: Automatic Partition Store Memory Management

## Breaking Change / New Feature

### What Changed

The partition store now includes an **automatic memory manager** that dynamically balances RocksDB memory budget across all active partition stores. 
The system monitors memory usage every 5 seconds and automatically redistributes memory when partitions are opened or closed.
This ensures that the system is using the available memory more efficiently.

**Removed configuration options:**

| Removed Option | Reason |
|----------------|--------|
| `worker.storage.num-partitions-to-share-memory-budget` | No longer needed; partition count is now determined automatically |
| `common.rocksdb-write-stall-threshold` | Write stalls disabled by default to prevent indefinite hangs |
| `common.rocksdb-enable-stall-on-memory-limit` | Write stalls disabled by default |

**Other changes:**
- Compression algorithm changed from LZ4 to ZSTD for all partition stores

### Why This Matters

- **Simplified configuration**: No need to manually estimate the number of partitions when configuring memory budgets
- **Dynamic balancing**: Memory is automatically redistributed as partitions are opened/closed during normal operation or rebalancing
- **Better stability**: Disabling write stalls prevents indefinite hangs under memory pressure
- **Container awareness**: On Linux, the system automatically detects container memory limits (cgroup v1/v2) and warns if RocksDB memory is misconfigured

### Impact on Users

- **Existing deployments**: If you configured `num-partitions-to-share-memory-budget`, remove it from your configuration as it is no longer needed
- **Compression**: Existing data will continue to be readable; new data will use ZSTD compression

### Migration Guidance

**Remove deprecated options** from your configuration:
   ```toml
   # Remove these if present:
   # [worker.storage]
   # num-partitions-to-share-memory-budget = ...
   
   # [common]
   # rocksdb-write-stall-threshold = ...
   # rocksdb-enable-stall-on-memory-limit = ...
   ```

### Related Issues

- [#3804](https://github.com/restatedev/restate/pull/3804): Automatic partition store memory manager
