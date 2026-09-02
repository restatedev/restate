# Release Notes: New `PurgeVQueueMeta` WAL command

## New Feature (forward-compatibility groundwork)

### What Changed

A new partition-processor WAL command, `PurgeVQueueMeta`, has been introduced. It carries a
list of vqueue ids (all on the same partition key) and deletes their vqueue metadata records
from the partition store and the in-memory cache. Deletion is re-validated at apply time:
only vqueues that are still obsolete (fully empty in all stages, including `Finished`) and
not paused are purged; all other ids are skipped.

No component proposes this command in this release; it is inert.

### Why This Matters

Vqueue metadata records currently live forever, even after a vqueue drains and is never used
again. A future release will introduce a background vacuum that proposes `PurgeVQueueMeta`
commands to reclaim this space. Shipping the apply-side handling now ensures that, by the
time the vacuum is enabled, the minimum supported server version can already process the
command.

### Impact on Users

None in this release. Note for mixed-version clusters in future releases: servers older than
this version cannot apply `PurgeVQueueMeta` commands, which is why proposing them is deferred
until this version is the minimum supported version.

### Migration Guidance

No action required.
