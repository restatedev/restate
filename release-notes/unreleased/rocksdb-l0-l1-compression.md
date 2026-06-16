# Release Notes: Configure RocksDB L0/L1 Compression

## New Feature

### What Changed
Restate now supports selecting the RocksDB compression algorithm for L0 and L1 SST files with `rocksdb-l0-l1-compression`.

Supported values are `zstd`, `lz4`, `snappy`, and `none`. Higher RocksDB levels continue to use ZSTD.

### Why This Matters
L0 and L1 files are short-lived and frequently compacted. Operators can now test lighter-weight compression algorithms such as LZ4 or Snappy to reduce CPU overhead while still preserving some I/O reduction.

### Impact on Users
- Existing deployments keep the current ZSTD default.
- Existing `rocksdb-disable-l0-l1-compression = true` settings continue to disable L0/L1 compression.
- Existing `rocksdb-disable-l0-l1-compression` settings take precedence if both L0/L1 compression options are set in the same config block.
- New deployments can opt into a lighter compression algorithm.

### Migration Guidance
To test LZ4 for L0/L1 SST files:

```toml
rocksdb-l0-l1-compression = "lz4"
```

To keep the previous explicit disable behavior:

```toml
rocksdb-l0-l1-compression = "none"
```
