# Release Notes: Drop a node's copy of a partition with `restatectl`

## New Feature

### What Changed

The new `restatectl partition drop-store` command deletes one node's local copy of a partition,
while the node keeps running its other partitions:

```shell
# Drop node N3's copy of partition 7
restatectl partition drop-store 7 --node N3

# Also stop the partition on N3 first, if N3 is still running it
restatectl partition drop-store 7 --node N3 --force
```

Only the node you name is affected; other replicas keep their data. After the drop, the node
rebuilds its copy of the partition if it is still one of its replicas: from the latest snapshot
when one is available, otherwise by replaying the partition's log.

By default, the node only accepts the request if it isn't running the partition, or if it has
given up on it, which `restatectl partition list` shows as `Broken (ahead-of-log)`. To drop the
copy of a partition the node is still running, add `--force`: the node stops the partition
first, then deletes its data.

Before sending the request, `restatectl` shows how the node reports the partition, warns you if
`--force` targets a partition that doesn't look broken, and asks for confirmation.

### Why This Matters

When a node's copy of a partition is ahead of the partition's log, the log has lost data and the
node won't run that partition again. Until now, the only fix was to stop the node and delete the
partition's files by hand. Now it's one command, and the node stays up.

### Impact on Users

- **This deletes data.** Everything the node stores for the partition is removed. If you drop the
  last remaining copy and there's no snapshot, the partition's data is lost.
- Without a snapshot, the node can only rebuild the partition if its log hasn't been trimmed.
- The targeted node must be running.
- The drop waits until reads of the partition that are already running on that node, such as SQL
  queries, have finished. Long-running queries can delay it.
- `--yes` skips the confirmation prompt. It is also skipped when the `CI` environment variable is
  set.
- If the connection fails after the request was sent, `restatectl` doesn't retry it, because the
  drop might have happened anyway. Check `restatectl partition list` before running the command
  again.

### Migration Guidance

None. This is a new command.
