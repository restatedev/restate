# `max-command-batch-bytes` limits the partition store write batch

## Bug Fix / Behavioral Change

`worker.max-command-batch-bytes` now limits the size of the write batch a partition processor
commits to its partition store for each batch of commands it applies, on every replica of a
partition.

Before, it limited the size of the commands themselves, and the replica on the node that appends
the partition's records, usually its leader, underestimated the size of most of them. With large
records, that replica's batches were limited only by `worker.max-command-batch-size` (32 records
by default), and a single RocksDB write batch could grow to hundreds of MiB. A write batch larger
than the partition's memtables stalls writes to the partition store until they're flushed.

Applying a command usually writes more than its own size, so with the same setting, batches are
smaller than before, most noticeably with large commands. The default stays 1 MiB, and no
configuration changes are required.

The new histogram `restate_partition_write_batch_size_bytes` shows the size of the write batches
that partition processors commit, labeled by `leader` (`1` for leaders, `0` for followers).
