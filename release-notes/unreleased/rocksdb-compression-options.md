# Release Notes: Configurable RocksDB compression per level

## New Feature

### What Changed

A new option, `rocksdb-compression`, sets the compression of RocksDB's SST files (`none`, `lz4` or
`zstd`) for the partition stores, the log-server, the metadata server and the local loglet. It takes
up to three values, for L0 (the files written by flushes), the levels in between, and the
bottommost level (the last one that holds data, which holds most of it), as a list or separated by
commas. The last value applies to the remaining levels:

| Value | L0 | Levels in between | Bottommost level |
|---|---|---|---|
| `"none,lz4,zstd"` (default) | none | Lz4 | Zstd |
| `"lz4"` | Lz4 | Lz4 | Lz4 |
| `"lz4,none"` | Lz4 | none | none |

In TOML, a list works too: `rocksdb-compression = ["none", "lz4", "zstd"]`. The option can be set
once for all databases or in each database's section (`worker.storage`, `log-server`,
`metadata-server`, `bifrost.local`), where it replaces the value set for all databases.
`--dump-config` shows the value each database uses.

### Why This Matters

Zstd writes smaller files than Lz4 but costs more CPU to compress and decompress. Compaction soon
rewrites the files that flushes write to L0, so compressing them mostly costs CPU. The bottommost
level keeps data longest, so Zstd saves the most space there.

### Impact on Users

- The default changes to `none,lz4,zstd` for every database:
  - Flushes no longer compress L0 files, which used Lz4 or Zstd depending on the database. Flushes
    use less CPU but write more bytes, and L0 takes more disk space until compaction rewrites it.
  - The levels in between use Lz4 where they used Zstd, which can increase disk usage a little,
    mostly for data that compaction will rewrite anyway. With universal compaction, the newer sorted
    runs, which can hold up to about half of a database before a full merge, now use Lz4 too: the
    increase is larger, and full merges come sooner.
  - The bottommost level keeps Zstd.
- Changes take effect when a node restarts and apply to files written from then on. Existing files
  keep their compression until compaction rewrites them.
- `rocksdb-disable-l0-l1-compression` is deprecated and will be removed in v1.9.0. `true` now turns
  off compression in L0 only, on top of the `rocksdb-compression` the database uses, and the level
  below L0 follows the levels in between. `rocksdb-compression` set in the same section takes
  precedence, and `false` has no effect. A warning is printed when it is `true`.

### Migration Guidance

No action is needed. If you set `rocksdb-disable-l0-l1-compression = true`, remove it: L0 is no
longer compressed by default.

To compress flushes with Lz4 as before:

```toml
rocksdb-compression = "lz4,lz4,zstd"
```

To use Zstd on every level:

```toml
rocksdb-compression = "zstd"
```

or through the environment, for all databases or only the partition stores:

```sh
RESTATE_ROCKSDB_COMPRESSION=zstd
RESTATE_WORKER__STORAGE__ROCKSDB_COMPRESSION=zstd
```
