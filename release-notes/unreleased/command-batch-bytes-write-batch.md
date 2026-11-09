# `max-command-batch-bytes` limits the partition store write batch

## Behavioral Change

`worker.max-command-batch-bytes` now limits the size of the write batch a partition processor
commits to its partition store for each batch of commands it applies, instead of the size of the
commands themselves. Applying a command usually writes more than its own size, so with the same
setting batches are smaller than before, most noticeably with large commands. The default stays
1 MiB, and no configuration changes are required.

The new histogram `restate_partition_write_batch_size_bytes` shows the size of the write batches
that partition processors commit, labeled by `leader` (`1` for leaders, `0` for followers).
