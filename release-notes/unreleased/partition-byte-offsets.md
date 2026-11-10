# Byte offsets of partitions (experimental)

## New Feature

Partitions can track how much data they have applied, in bytes, next to their applied LSN. Byte
offsets tell how much data lies between two positions of a partition, for example how far one
replica is behind another, which LSNs alone don't.

Like the applied LSN, byte offsets are positions: they count from `byte_offsets_base_lsn`, the log
position at which the partition turned them on.

- `log_byte_offset` is how far a partition has applied its log, in bytes as the log stores its
  records. Offsets with the same `byte_offsets_base_lsn` can be compared between replicas to
  estimate how far apart they are. Replicas at the same applied LSN normally have the same
  offset, but it can differ slightly, for example while they run different versions.
- `partition_store_byte_offset` is how much data a replica has written to its partition store,
  before compression. It's local to each replica, since replicas write their partition stores
  differently, and a replica restored from a snapshot continues from the snapshot's.

The `partition_state` table shows them in the columns `byte_offsets_base_lsn`, `log_byte_offset`
and `partition_store_byte_offset`, which are NULL while a partition doesn't track byte offsets.
Partition snapshots record them, as of the snapshot's applied LSN, in the same fields of their
`metadata.json`. A snapshot's `log_byte_offset` can be compared with a replica's when their
`byte_offsets_base_lsn` is the same, for example to estimate how much of the log lies between
the snapshot and the replica.

To turn byte offsets on, upgrade all nodes to v1.8.2 or newer, then enable:

```toml
experimental-enable-byte-offsets = true
```

A partition turns them on the next time a leader is elected on a node that has the option, for
example when its current leader's node restarts with it.

- Once a partition has turned byte offsets on, they stay on, even if the option is removed.
- Once enabled, you **cannot** roll back to a Restate-server version older than v1.8.2.
