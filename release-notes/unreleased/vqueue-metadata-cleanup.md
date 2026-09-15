# Release Notes: Automatically clean up obsolete VQueue metadata

## Bug Fix

### What Changed

Restate now deletes a VQueue's metadata when an update leaves the queue unpaused and fully empty,
including its finished-entry stage. Empty paused queues are retained so their pause state is
preserved.

Starting with Restate v1.7.10, a one-time local storage migration can also remove obsolete VQueue
metadata accumulated by earlier versions. The migration is enabled with
`experimental-enable-vqueue-obsolete-cleanup` in v1.7.10 and v1.8, and is enabled unconditionally
from v1.9. The migration scans the partition's VQueue metadata in bounded, low-priority chunks and
records a local storage feature when it completes.

### Why This Matters

Workloads that create many short-lived VQueues could previously retain an unbounded number of empty
metadata records. Automatically removing them limits partition-store growth and avoids increasingly
expensive VQueue metadata scans.

### Impact on Users

- Fully empty, unpaused queues disappear from `sys_vqueue_meta` and the VQueue CLI.
- Historical timestamps and exponential moving averages stored in deleted metadata are discarded.
  If the same VQueue is created again, these statistics restart from a new metadata record.
- Empty paused queues remain visible and keep their pause state.
- The first start after enabling the migration may take longer while each local partition-store
  replica performs its cleanup scan. Deleting a large backlog can temporarily increase RocksDB
  compaction and disk I/O.
- A partition store that completed this cleanup requires Restate v1.7.10 or newer.

### Migration Guidance

On v1.7.10 and v1.8, enable the migration on each node after upgrading the cluster:

```toml
[common]
experimental-enable-vqueue-obsolete-cleanup = true
```

Restate v1.9 enables the migration without configuration. For clusters with a large metadata
backlog, roll the setting or upgrade through replicas gradually and allow cleanup and subsequent
RocksDB compaction to settle before restarting additional replicas.
