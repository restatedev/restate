# Release Notes: SQL queries over unavailable partitions

## Behavioral Change

### What Changed

Queries over the partition-backed introspection tables (`sys_invocation`,
`sys_invocation_status`, `sys_journal`, `state`, `sys_inbox`, …) used to fail outright when a
single Restate partition could not be scanned — whether because it routed nowhere:

```
$ restate sql 'SELECT MAX(completion_retention) FROM sys_invocation_status'
External error: node lookup for partition 22 failed
```

or because its store was closed on the node serving it, which is what a disabled partition
looks like (after `restatectl partition drop-store`, while a partition is being transferred,
or whenever its processor has stopped):

```
$ restate sql 'select * from sys_invocation_status'
External error: partition 14 doesn't exist on this node, this is benign if the
partition is being transferred out of/into this node.
```

Such partitions are now **skipped**, and the query returns results from the remaining ones
together with a report of what was skipped:

```
$ restate sql 'SELECT MAX(completion_retention) FROM sys_invocation_status'
 MAX(sys_invocation_status.completion_retention)
-------------------------------------------------
 1d 12h

WARNING (partial results): rows are missing, so aggregates are lower bounds and a lookup may find nothing for a record that exists.
  partition 22: partition 22 is currently unavailable: no partition store is open on this node
```

The rule is that a partition is skipped when its scan **produced no rows**. A partition that
fails *after* it has already returned rows still fails the query, because an arbitrary
truncated subset of one partition is a result no warning can describe honestly.

The report is delivered as the `x-restate-query-warnings` response header (a JSON array of
`{"origin", "message"}`) and, for `Accept: application/json`, under the body's `warnings`
key. Every skipped scan is also logged on the node that served the query.

`SET` statements are no longer accepted by the query endpoints.

### Why This Matters

A broken or disabled partition made every introspection query over these tables unusable —
including the queries an operator most needs while diagnosing that very partition. There was
no way to work around it from SQL either: no predicate can exclude a partition, because the
only predicate-based narrowing is an inclusive key set, and any predicate that narrows the
scan also filters out the rows it was meant to keep.

### Impact on Users

- **Results can now be silently incomplete unless you check the warnings.** With a partition
  skipped:
  - aggregates (`count`, `sum`, `max`, …) cover only the partitions that were scanned, so
    they are lower bounds rather than answers;
  - a lookup by id or key can return nothing for a record that exists;
  - a query that stops early (`LIMIT` without `ORDER BY`) may never attempt every partition,
    so an *empty* warning list does not by itself prove the result is complete;
  - `sys_invocation` joins two independently scanned tables, so a partition can be skipped
    by one side and scanned by the other; affected rows fall back to default column values
    (for example `status` reads as `ready`) rather than carrying a marker.
- `restate sql` prints the warnings to **stderr**, so `--json`/`--jsonl` output stays
  machine-parseable, and the exit code stays 0.
- `restate invocations describe` no longer reports `Invocation ... not found!` when the
  partition that would hold it could not be searched; it fails with an explicit
  "cannot tell whether it exists" error instead. `restate invocations list` prints a warning
  when its listing and count are understated for the same reason.
- Anything that reads the admin `/query` endpoint directly — dashboards, Grafana, scripts —
  will start receiving partial results where it previously received an error. Check
  `x-restate-query-warnings` (or the `warnings` key) before acting on a result, and do not
  feed a result that carries warnings into billing, alerting, or capacity decisions.
- `SET` statements are rejected. They previously mutated the shared query session for every
  subsequent query on that node, so this closes a cross-query side effect.
- `restatectl sql` is unaffected: its query context exposes cluster-scoped tables only.
- The `x-restate-query-warnings` **header** can only report partitions already known to be
  unavailable when the query was planned, because response headers precede the body. That
  covers a partition that routes nowhere and one whose store is closed on this node — the
  usual cases. A partition that fails while the query is already streaming (a *remote* node's
  store closing, a node dying mid-query) is reported in the JSON body's `warnings` key and in
  the serving node's log, but cannot appear in the header. Prefer
  `Accept: application/json` if you need the report to be complete in all cases.

### Migration Guidance

No configuration change is required. If a consumer needs all-or-nothing semantics, have it
treat a non-empty `x-restate-query-warnings` header (or `warnings` body key) as a failure:

```shell
curl -sD - -X POST localhost:9070/query \
  -H 'content-type: application/json' -H 'accept: application/json' \
  -d '{"query":"SELECT COUNT(*) FROM sys_invocation_status"}'
```

If a partition shows up as unavailable, `restatectl partitions list` shows its replica-set,
leader and processor state. It becomes scannable again once it both routes somewhere — it has
a leader, or any alive node in its replica-set — and has a running processor holding its store
open on the node that serves the scan.
