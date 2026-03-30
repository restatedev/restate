# Release Notes: `restatectl storage compact` command

## New Feature

### What Changed

A new `restatectl storage compact` command has been added to trigger manual RocksDB
compaction on cluster nodes. This allows operators to reclaim disk space, reduce read
amplification, or force compaction after bulk deletions without restarting nodes.

### Why This Matters

RocksDB compactions run automatically in the background, but there are situations where
operators may want to trigger them explicitly — for example, after a large number of
invocations have been cleaned up, or as part of routine maintenance to ensure optimal
storage performance.

### Usage

```bash
# Compact all databases on all nodes
restatectl storage compact

# Compact specific database types
restatectl storage compact -d partition-store
restatectl storage compact -d log-server,metadata-server

# Target specific nodes
restatectl storage compact -n N1,N2

# Combine filters
restatectl storage compact -d partition-store -n N1
```

**Available database types:** `partition-store`, `log-server`, `metadata-server`, `local-loglet`
