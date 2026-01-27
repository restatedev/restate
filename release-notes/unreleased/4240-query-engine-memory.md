# Release Notes: Query Engine Memory Budget Reduced

## Behavioral Change

### What Changed

The default memory budget for the SQL query engine has been reduced from **4 GiB** to **1 GiB**.

### Why This Matters

The previous 4 GiB default was required due to inefficient queries in earlier versions. Internal optimizations, particularly streaming of underlying iterators, have significantly reduced memory requirements. This change lowers the default memory footprint of Restate.

### Impact on Users

- **Lower memory usage**: Default Restate deployments will use less memory

### Migration Guidance

If you experience slower query performance, you can restore the previous behavior by configuring a larger memory budget:

```toml
[query-engine]
memory-size = "4GiB"
```

Or via environment variable:

```bash
RESTATE_QUERY_ENGINE__MEMORY_SIZE=4GiB
```

### Related Issues

- [#4240](https://github.com/restatedev/restate/pull/4240): Change default memory budget of query engine to 1 GiB
