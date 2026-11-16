# Release Notes: Universal compaction for partition stores and the log-server

## New Feature

### What Changed

A new option, `rocksdb-compaction`, selects how RocksDB compacts the partition stores
(`worker.storage.rocksdb-compaction`) and the log-server (`log-server.rocksdb-compaction`). Its
`style` is `level`, the current behavior and the default, or `universal`. Universal compaction
also accepts the following settings:

| Setting | Default | Meaning |
|---|---|---|
| `sorted-runs-compaction-trigger` | `8` | Number of sorted runs that starts a compaction. Used instead of `rocksdb-l0-num-compaction-trigger`. |
| `max-size-amplification-percent` | `100` | Merge everything into one sorted run once the newer runs reach this percentage of the oldest. |

### Why This Matters

Leveled compaction rewrites every value several times on each level as data moves down. Universal
compaction rewrites values far less often, so it can use much less disk bandwidth and CPU, and keep
up with higher write rates on the same disks. This helps most for nodes that store a lot of data
and are limited by disk bandwidth.

The trade-off is disk space. With universal compaction, newer data accumulates next to the oldest
data until it reaches `max-size-amplification-percent` of its size. Then everything is merged
into one sorted run, which rewrites the whole database in one go. With the default of 100, a
database grows to up to about twice its live data before such a merge. The merge writes a new copy
of the live data before it deletes the old files, so the database briefly takes up to about three
times its live data. Each partition store is merged on its own, but partition stores that grow at
the same pace merge at about the same time.

### Impact on Users

- Nothing changes unless you set the option. The default remains leveled compaction.
- Changes take effect when a node restarts.
- Switching an existing node to `universal` needs no migration. Switching back to `level` also
  works, but compaction then rewrites the data through the levels again, which takes time and
  disk bandwidth.
- Nodes with different compaction styles can share partition snapshots in both directions.
- On the log-server with `rocksdb-enable-blob-separation`, universal compaction doesn't force
  blob garbage collection, so space of trimmed records in blob files comes back later than with
  leveled compaction: once compactions rewrite the files that point to them. A warning is logged
  when both are enabled.
- Make sure nodes have enough free disk space before enabling universal compaction: with the
  default settings, plan for peaks of up to about three times the live data, compared to about
  1.1 times with leveled compaction.
- Only the oldest sorted run uses `rocksdb-bottommost-compression` (Zstd by default). Newer runs use
  `rocksdb-compression` (Lz4 by default), so they take more space and full merges come sooner.
  Setting `rocksdb-compression = "zstd"` and `rocksdb-l0-compression = "lz4"` keeps them smaller,
  at a higher CPU cost per compaction.

### Migration Guidance

No action is needed. To use universal compaction for the partition stores:

```toml
[worker.storage.rocksdb-compaction]
style = "universal"
```

and for the log-server:

```toml
[log-server.rocksdb-compaction]
style = "universal"
```

or through the environment:

```sh
RESTATE_WORKER__STORAGE__ROCKSDB_COMPACTION__STYLE=universal
RESTATE_LOG_SERVER__ROCKSDB_COMPACTION__STYLE=universal
```
