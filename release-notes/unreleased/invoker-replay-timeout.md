# Invocation attempts fail if the deployment doesn't receive the state and journal in time

## Bug Fix / Behavioral Change

When Restate starts or resumes an invocation, it sends the invocation's state and journal to the
service deployment. This step is now limited to 10 minutes. If it takes longer, the attempt fails
with error `RT0010` and is retried according to the invocation's retry policy.

Before, this step had no time limit. If a deployment accepted the request but stopped reading the
request body, an invocation with more state and journal than
`worker.invoker.per-invocation-memory-limit` stayed in this step forever. While it did, the
partition store on that node couldn't clean up overwritten and deleted data, so its disk usage
kept growing and reads got slower.

The limit is independent of the inactivity and abort timeouts, including the ones configured per
service or handler. Sending the state and journal doesn't run handler code, so handlers that take
long to replay aren't affected.

When Restate gives up on an invocation attempt before the request body was sent, it now resets
the HTTP/2 stream to the deployment. Before, the stream could stay open, and the unsent part of the
request kept using invoker memory until the connection was closed.

Two new metrics show whether something holds back the cleanup of a RocksDB database:

- `restate_rocksdb_num_snapshots`: the number of open snapshots.
- `restate_rocksdb_oldest_snapshot_time_seconds`: the Unix timestamp at which the oldest open
  snapshot was taken, `0` if there is none.
