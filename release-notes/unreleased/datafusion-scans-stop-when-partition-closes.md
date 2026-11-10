# DataFusion scans stop when a partition closes

## Behavioral Change

### What Changed

Queries through SQL/DataFusion, including `restatectl sql` and the introspection tables, now stop
when the partition they are reading closes or moves to another node. Previously, an in-progress
query could keep a closing partition store open until the scan finished.

### Why This Matters

Partition transfers and shutdowns no longer wait for long-running or abandoned introspection
queries, and a slow or abandoned query client can no longer delay a partition close. The scan is
also cancelled while it is waiting to hand results to its client, so it doesn't linger after the
partition is gone.

### Impact on Users

- A query that spans a partition that is transferred or shut down while the query runs may now
  fail with an error saying the scan was interrupted or the partition is unavailable.
- Retry the whole query and discard any partial results; the data is available from the
  partition's new location.

### Migration Guidance

No action is needed.
