# Release Notes: Drop a node's copy of a partition with `restatectl`

## New Feature

### What Changed

The new `restatectl partition drop-store` command deletes one node's local copy of a partition,
while the node keeps running its other partitions:

```shell
# Drop node N3's copy of partition 7
restatectl partition drop-store 7 --node N3 --reason "store sealed after log data loss"

# Also stop the partition on N3 first, if N3 is still running it
restatectl partition drop-store 7 --node N3 --force --reason "rebuild from snapshot"
```

Only the node you name is affected; other replicas keep their data. After the drop, the node
rebuilds its copy of the partition if it is still one of its replicas: from the latest snapshot
when one is available, otherwise by replaying the partition's log.

By default, the node only accepts the request in two cases:

- It has given up on the partition, which `restatectl partition list` shows as
  `Broken (ahead-of-log)`.
- It is no longer part of the partition's replica set, and keeps a copy that nothing uses anymore.

The node checks this against a replica set that is at least as recent as the one `restatectl`
saw. If it hasn't learned about that one yet, it waits a few seconds, then refuses. To drop the
copy of a replica anyway, add `--force`: the node stops the partition first if it is running it,
then deletes its data.

`--reason` is required. The admin and the node log it next to the request, so you can tell later
why a store was dropped. After the drop, `restatectl` reports how much disk space the deleted
data used.

Before sending the request, `restatectl` shows how the node reports the partition, warns you if
`--force` targets a partition that doesn't look broken, and asks for confirmation.

To see what would happen without deleting anything, add `--dry-run`. The node makes the same
checks and reports whether it would drop its copy and how much disk space that copy uses. The
admin and the node log dry runs with a `[DRY RUN]` prefix.

#### Removing stale partition stores

When a partition moves to other nodes, the nodes it left keep their copy of it on disk. The new
`restatectl partition drop-stale-stores` command removes these copies across the cluster:

```shell
# Drop every alive worker node's copies of partitions it isn't a replica of
restatectl partition drop-stale-stores --reason "reclaim disk space after rebalancing"

# Limit it to some nodes or partitions
restatectl partition drop-stale-stores --node N1,N3 0-15 --reason "free up disk space on N1 and N3"

# Only report which copies would be dropped, and how much disk space they use
restatectl partition drop-stale-stores --dry-run --reason "check before cleaning up"
```

`restatectl` looks up each partition's replica set, shows how many partitions it will check on
each node (those the node isn't a replica of), and asks for confirmation. It then sends one drop
request at a time, reports every store it drops with its size, and ends with a table of how many
stores it dropped on each node, how much disk space they used, and the total across all nodes.
After each dropped store it waits a second before the next request, to spread out the deletions;
set `--delay-between-drops` to change that, for example `--delay-between-drops 0s`. Nodes that
don't have a copy of a partition skip the request. The same checks as for `drop-store` without
`--force` apply, so a node keeps its copy if it still runs the partition or has learned that it
is a replica again. Nodes that aren't alive are skipped, and shown as `DEAD`. If a request's
outcome is unknown, for example because the connection broke, the command stops; running it
again continues where it left off.

### Why This Matters

When a node's copy of a partition is ahead of the partition's log, the log has lost data and the
node won't run that partition again. Until now, the only fix was to stop the node and delete the
partition's files by hand. Now it's one command, and the node stays up.

### Impact on Users

- **This deletes data.** Everything the node stores for the partition is removed. If you drop the
  last remaining copy and there's no snapshot, the partition's data is lost.
- Without a snapshot, the node can only rebuild the partition if its log hasn't been trimmed.
- After its stale copy is dropped, a node that becomes a replica of the partition again rebuilds
  it from a snapshot or the log, instead of catching up from its old copy.
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
