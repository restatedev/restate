# New option to limit concurrent automatic snapshots

## New Feature

### What Changed

A new option, `automatic-snapshot-concurrency-limit` in the `[worker.snapshots]` section, sets how
many snapshots can be in progress on a node before it stops starting automatic ones. A snapshot is
in progress from exporting the partition store until it's uploaded to the snapshot destination. It
defaults to `4`, the limit Restate has used so far.

Snapshots created with `restatectl snapshots create` count toward the limit while they run, but
are never held back by it.

Exports are short and limited separately by `export-concurrency-limit`, so in practice the option
limits how many snapshots are uploaded at the same time. Each one keeps its export on local disk
until its upload finishes.

### Why This Matters

A node with many partitions can fall behind its snapshot schedule with only four snapshots at a
time. A lower limit reduces the disk, network, and object store load that snapshots add.

### Impact on Users

- Nothing changes unless you set the option.
- The value must be a positive integer. `0` is rejected when the configuration is loaded. To turn
  automatic snapshots off, set `snapshot-interval = "0"` instead.
- Changes to the configuration file take effect without a restart.
- `export-concurrency-limit` still limits how many partition stores are exported at the same time,
  for automatic and requested snapshots alike. Uploads don't count against it.

### Migration Guidance

No action is needed. To change the limit:

```toml
[worker.snapshots]
automatic-snapshot-concurrency-limit = 4
```
