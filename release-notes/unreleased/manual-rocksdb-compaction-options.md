# Release Notes: Control Manual RocksDB Compaction

## New Feature

### What Changed

`restatectl storage compact` now lets operators control bottommost-level compaction with
`--bottommost-level-compaction`. It preserves RocksDB's default behavior, which only rewrites the
bottommost level when a compaction filter is configured. Use `force-optimized` to rewrite existing
bottommost files after large deletions while avoiding files created during the same compaction.

The `--recalculate-level` flag asks RocksDB to move compacted files to the minimum level capable of
holding the resulting data. This is useful when compaction substantially reduces the data size.

The command now waits up to 30 minutes by default, configurable with `--timeout`. Compactions
accepted by a node continue in the background if the client times out. Any database, node, or
request failure now causes a non-zero exit status after all available results have been printed.

### Usage

```bash
restatectl storage compact -d partition-store
restatectl storage compact --bottommost-level-compaction force-optimized
restatectl storage compact --recalculate-level --timeout 1h
```

### Operational Note

The current RocksDB Rust binding does not expose the native status returned by manual compaction.
A successful result therefore confirms that the compaction task completed, but cannot report a
native RocksDB compaction error.
