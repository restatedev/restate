# Release Notes: `restatectl partition drop-store`

## New Feature

### What Changed

A new command deletes a single node's local copy of a partition:

```shell
restatectl partition drop-store <PARTITION_ID> --node N3
```

The request goes through the cluster controller (`ClusterCtrlSvc.DropPartitionStore`) to the
targeted node's partition processor manager, which deletes the partition's RocksDB column
family. Once dropped, the node restarts the processor and re-bootstraps from the latest
snapshot, or from the log if no snapshot repository is configured.

The node **rejects** the request while it is running a partition processor for that partition,
unless it has already given up on it (reported as `Broken (ahead-of-log)` in
`restatectl partition list`). Passing `--force` overrides that: the node stops the running
processor, waits for it to terminate, and only then deletes the store. `restatectl` prints the
node's reported state and asks for confirmation before sending either variant.

### Why This Matters

A partition store that is ahead of its log is sealed and can never be used again. Until now the
only way to clear it was to stop the node and delete files by hand. This makes the recovery a
single, guarded command.

### Impact on Users

- **Destructive.** All of the targeted node's data for that partition is deleted. Other replicas
  are untouched. Dropping the store of the last remaining replica loses the partition's data
  unless a snapshot exists.
- Requires the targeted node to be alive.
- Confirmation is skipped with `--yes` or when `CI` is set, as with other `restatectl` commands.

### Migration Guidance

None; this is a new command.
