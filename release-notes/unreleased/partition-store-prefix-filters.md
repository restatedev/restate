# Release Notes: More selective partition-store filters

## Behavioral Change

### What Changed

Partition stores now build their filters per invocation and per queue instead of per partition
key. Two new options in `worker.storage` let you shrink the filters and extend them to the
largest (last) storage level:

- `rocksdb-disable-whole-key-filtering`: build filters from key prefixes only. Defaults to
  `false`.
- `rocksdb-enable-l6-filters`: also build filters for the largest level. Defaults to `false`.

Restate v1.9 is planned to enable both by default and to remove
`rocksdb-disable-whole-key-filtering`.

### Why This Matters

Reading an invocation's journal or a queue's contents now skips more files that cannot contain
the requested data.

With both options enabled, lookups of data that does not exist, such as invocation ids that were
never created or are already purged, usually no longer read from disk. The filters also take far
less memory than whole-key filters on every level would.

### Impact on Users

- No configuration changes are needed. Point lookups keep their current performance and filter
  memory stays about the same.
- `rocksdb-enable-l6-filters` alone increases filter memory considerably. Combine it with
  `rocksdb-disable-whole-key-filtering` to keep filters small.
- Setting `rocksdb-disable-whole-key-filtering` right after upgrading makes lookups of missing
  data slower, and it stays that way until background compaction has rewritten the files
  written before the upgrade.

### Migration Guidance

No action is required. To opt into the planned v1.9 defaults, wait until the node has run this
version for a while, then set:

```toml
[worker.storage]
rocksdb-disable-whole-key-filtering = true
rocksdb-enable-l6-filters = true
```

These options are applied when a partition store is opened, so a change takes effect after a node
restart and applies to newly written files.
