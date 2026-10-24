# Best-effort recovery from existing partition snapshots

## New feature

`restate-doctor recover` can prepare a fresh single-node cluster from existing
partition snapshots without a backup manifest or a surviving source cluster.
Recovery can start with an empty deployment registry, optionally recover a registry
from source metadata or a snapshot, and preserve or replace the cluster identity.

This is emergency recovery, not a consistent point-in-time restore. Operators must
explicitly accept data loss and isolate the original cluster. Missing partitions
can be initialized empty when their original topology is supplied. Re-registering
deployments does not necessarily let invocations pinned to old deployment IDs resume.

The source remains read-only. Recovery requires a new output directory and, when
using external metadata, a new isolated destination. See the
[recovery guide](../../tools/restate-doctor/README.md) for preparation, startup, and
limitations. Existing server behavior and snapshot formats are unchanged.
