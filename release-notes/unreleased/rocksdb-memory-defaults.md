# Release Notes: RocksDB memory defaults changed

## Behavioral Change

### What Changed
The default RocksDB memory configuration has been changed:
- **`rocksdb-total-memory-size`**: Changed from **6GiB** to **2GiB**
- **`rocksdb-total-memtables-ratio`**: Changed from **0.5 (50%)** to **0.85 (85%)**

This means the default block cache size is now approximately 300MiB (15% of 2GiB) compared to the previous 3GiB (50% of 6GiB).

### Why This Matters
These changes address two goals:

1. **Reduced memory confusion**: The previous 6GiB default caused Restate's memory footprint to appear to grow continuously as the block cache filled up, leading to user confusion about memory "ballooning." The lower default provides more predictable memory behavior out of the box.

2. **Performance-optimized allocation**: Memtables have a much larger impact on write performance than block cache does on read performance in typical workloads. By allocating 85% of RocksDB memory to memtables, the new defaults prioritize the more performance-critical component.

Thanks to the variety of performance improvements in v1.6, Restate now delivers better performance with less memory than before.

### Impact on Users
- **New deployments**: Will use the new memory-efficient defaults automatically
- **Existing deployments**: Will adopt new defaults on upgrade, resulting in lower memory usage
- **Custom configurations**: Any explicitly configured `rocksdb-total-memory-size` or `rocksdb-total-memtables-ratio` values will continue to work as before

### Migration Guidance
The new defaults should work well for most workloads. However, if you have a read-heavy workload that benefits from a larger block cache, or if you were relying on the previous memory allocation, you can restore the old behavior:

```toml
[common]
rocksdb-total-memory-size = "6GiB"
rocksdb-total-memtables-ratio = 0.5
```

For systems with more available memory that want to maximize performance, consider tuning these values based on your specific workload characteristics and available RAM.
