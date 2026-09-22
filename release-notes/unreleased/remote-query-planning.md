# Plan remote storage queries against fixed partition owners

## Behavioral Change

### What Changed

Distributed storage-query physical plans now record local and remote partition placement
explicitly. If partition ownership changes after planning, the query fails instead of
silently rerouting part of the already planned query. Remote execution uses a generic,
versioned physical-plan fragment foundation with explicit worker acceptance and local
fallback. Stable filter/computed-projection chains and eligible partial aggregates execute
at partition owners. Partial aggregates can include safe `FILTER (WHERE ...)` clauses and
any serializable, order-insensitive aggregate whose accumulator state DataFusion can
merge. They send computed rows or accumulator state instead of raw input rows to the
query coordinator. Remote query cancellation also interrupts a scanner while it is
actively producing the next batch.

### Why This Matters

One physical query now uses a single, internally consistent partition-placement
decision. The generic fragment boundary allows additional safe pushdowns to be layered
without adding feature-specific transport paths. A capacity-one remote stream bounds
read-ahead and stops promptly when the query is cancelled. Partial
aggregation can substantially reduce network traffic and coordinator work for
low-cardinality grouped aggregates. `EXPLAIN ANALYZE` reports whether workers accepted or
declined requested fragments.

### Impact on Users

Queries keep the same results and require no configuration changes. Volatile or otherwise
unsafe row-wise expressions and unsupported aggregate shapes continue to execute on the
coordinator. A new querying node normally requires the worker to acknowledge validation
of its planned partition owner. During a rolling upgrade, it accepts the legacy
acknowledgement only from an exact node generation positively identified as running a
v1.7 binary; unknown, older-than-v1.7, and v1.8-or-newer peer versions remain strict. A
worker can decline an incompatible fragment before execution, in which case the querying
node executes the same fragment locally over the raw pull stream. A query that races with
partition movement can likewise return a transient error and should be retried.

### Migration Guidance

No migration is required. Clients that already retry transient query failures need no
changes.
