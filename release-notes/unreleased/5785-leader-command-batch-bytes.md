# Leaders apply `max-command-batch-bytes`

## Bug Fix

`worker.max-command-batch-bytes` now limits the batches of every replica of a partition. The
replica on the node that appends the partition's records, usually its leader, underestimated the
size of most records it applied. With large records, its batches were limited only by
`worker.max-command-batch-size` (32 records by default), and a single RocksDB write batch could
grow to hundreds of MiB. A write batch larger than the partition's memtables stalls writes to the
partition store until they're flushed. Leaders now apply more, smaller batches when records are
large. No configuration changes are required.
