# Release Notes: Configurable RocksDB compression per level

## New Feature

### What Changed

Three new options set the compression of RocksDB's files (`lz4`, `zstd` or `none`) for the
partition stores, the log-server, the metadata server and the local loglet:

| Option | Levels | Default when unset |
|---|---|---|
| `rocksdb-compression` | every level without an override | Lz4 |
| `rocksdb-l0-compression` | L0, written by flushes | `rocksdb-compression`, else Lz4 |
| `rocksdb-bottommost-compression` | the last level that holds data, which holds most of it | `rocksdb-compression`, else Zstd |

Setting `rocksdb-compression` alone applies it to every level. The options can be set once for all
databases or in each database's section (`worker.storage`, `log-server`, `metadata-server`,
`bifrost.local`).

### Why This Matters

Zstd writes smaller files than Lz4 but costs more CPU to compress and decompress. Compressing the
levels that are rewritten often with Lz4, and the bottommost level, which keeps data longest, with
Zstd, saves CPU where data is short-lived and disk space where it stays.

### Impact on Users

- The default layout changes slightly. Partition stores and the log-server keep Lz4 for L0 and the
  level below it and Zstd at the bottom, but the levels in between now use Lz4 instead of Zstd. The
  metadata server and the local loglet now use Lz4 instead of Zstd above the bottommost level. This
  can increase disk usage a little, mostly for data that compaction will rewrite anyway. With
  universal compaction, the newer sorted runs, which can hold up to about half of a database before
  a full merge, now use Lz4 too: the increase is larger, and full merges come sooner.
- Changes take effect when a node restarts and apply to files written from then on. Existing files
  keep their compression until compaction rewrites them.
- `rocksdb-disable-l0-l1-compression` is deprecated. `true` now means
  `rocksdb-l0-compression = "none"`: flushes stay uncompressed, but the level below L0 now follows
  `rocksdb-compression`. In a database's section, `false` still overrides a `true` set for all
  databases. A warning is printed when it is set to `true`.

### Migration Guidance

No action is needed. If you set `rocksdb-disable-l0-l1-compression = true`, replace it with:

```toml
rocksdb-l0-compression = "none"
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
