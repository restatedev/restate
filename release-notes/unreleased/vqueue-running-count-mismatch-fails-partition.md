# Release Notes: A vqueue running-count mismatch fails one partition, not the node

## Bug Fix

### What Changed
When a partition processor becomes leader (or resumes a paused virtual queue) and a virtual
queue's stored running-entry count disagrees with the entries actually in its running stage,
the partition processor now stops with an error naming the vqueue and is restarted with backoff.
Previously the scheduler panicked (`vqueue has no running items but its metadata says it has N`),
which shut the whole node down after the 60 s graceful-shutdown timeout and failed over every
partition that node was leading.

### Impact on Users
- Only the affected partition processor on that node restarts; the node and its other partitions
  keep running, and another replica can take over leadership of the affected partition.
- If the mismatch is in that replica's stored data, its processor keeps failing on every attempt
  to lead the partition. The error log names the vqueue; report it so the inconsistency can be
  investigated.
