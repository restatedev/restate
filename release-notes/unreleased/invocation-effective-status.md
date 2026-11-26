# Invocations show their stage and effective status; `status` and `completion_result` are deprecated

## New Feature / Behavioral Change / Deprecation

### What Changed

`sys_invocation_status` and `sys_invocation` have three new columns that tell what an invocation is
doing now, with the same values `sys_vqueue_entry_status` shows for the invocation's entry:

| Column             | Values                                                                                                                                  |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `stage`            | `inbox`, `running`, `suspended`, `paused`, `finished`                                                                                   |
| `effective_status` | in the inbox: `new`, `scheduled`, `ready`, `backing-off`, `yielded`; then `running`, `suspended`, `paused`; finished: `succeeded`, `failed`, `killed`, `cancelled` |
| `completed`        | `true` once the invocation finished                                                                                                     |

```sql
SELECT id, stage, effective_status, completed FROM sys_invocation WHERE target_service_name = 'Greeter';
```

```text
id            stage      effective_status  completed
inv_1a…       running    running           false
inv_1b…       inbox      backing-off       false
inv_1c…       finished   killed            true
```

The `status` and `completion_result` columns of both tables are **deprecated**: use
`effective_status`, `stage` and `completed` instead. They will be removed in a future release.
Until then they keep their names but are now derived from the effective status, in both tables
the same way:

| `effective_status`                 | `sys_invocation_status.status` | `sys_invocation.status` | `completion_result` |
|------------------------------------|--------------------------------|-------------------------|---------------------|
| `new`                              | `inboxed`                      | `pending`               | NULL                |
| `scheduled`                        | `scheduled`                    | `scheduled`             | NULL                |
| `ready`, `yielded`                 | `invoked`                      | `ready`                 | NULL                |
| `backing-off`                      | `invoked`                      | `backing-off`           | NULL                |
| `running`                          | `invoked`                      | `running`               | NULL                |
| `suspended`                        | `suspended`                    | `suspended`             | NULL                |
| `paused`                           | `paused`                       | `paused`                | NULL                |
| `succeeded`                        | `completed`                    | `completed`             | `success`           |
| `failed`, `killed`, `cancelled`    | `completed`                    | `completed`             | `failure`           |

`completion_failure` still shows the failure of a completed invocation.

### Why This Matters

Until now, `sys_invocation.status` mixed the stored invocation status with what the invoker was
doing at that moment, so it could disagree with the invocation's entry: for example, an invocation
waiting in the inbox to retry after a failure could show `ready`. The new columns and the derived
`status` come from one source, the invocation's entry, so `sys_invocation`, `sys_invocation_status`
and `sys_vqueue_entry_status` always agree.

### Impact on Users

- Queries that select columns by name are unaffected. The three columns come last in both tables;
  tools that read `SELECT *` results by position need to account for them.
- `sys_invocation.status` no longer depends on the invoker's live state: `running` means the
  invocation is in the running stage, and `backing-off` that it waits in the inbox to retry.
- Where the server finds no entry for an invocation, or can't recognize its stage, `stage` and
  `effective_status` show NULL, `status` and `completion_result` show the stored values as before,
  and `completed` follows `status`.
- Queries that show or filter on `status`, `completion_result` or the new columns also read each
  invocation's entry. Lookups and paged lists are unaffected; scans over all invocations take
  about 15% longer.
- **During a rolling upgrade**, until every node runs this version:
  - queries on `sys_invocation` that show `status`, including `SELECT *`, fail for partitions led by
    nodes on an older version. This includes `restate invocations` commands and the UI's invocation
    pages. `status` now needs a column older nodes don't have;
  - queries that use `stage`, `effective_status` or `completed` fail the same way;
  - `sys_invocation_status.status` and `completion_result` keep working, but partitions led by older
    nodes show the stored values instead of the derived ones.

  Upgrade every node before relying on these queries.

### Migration Guidance

Replace conditions on `status` with conditions on the new columns:

| Instead of                                 | Use                                                            |
|--------------------------------------------|----------------------------------------------------------------|
| `sys_invocation.status = 'pending'`        | `effective_status = 'new'`                                     |
| `sys_invocation.status = 'ready'`          | `effective_status IN ('ready', 'yielded')`                     |
| `sys_invocation_status.status = 'inboxed'` | `effective_status = 'new'`                                     |
| `sys_invocation_status.status = 'invoked'` | `effective_status IN ('ready', 'yielded', 'backing-off', 'running')` |
| `status = 'completed'`                     | `completed`, or `stage = 'finished'`                           |
| any other `status = '<name>'`              | `effective_status = '<name>'`                                  |
| `completion_result = 'success'`            | `effective_status = 'succeeded'`                               |
| `completion_result = 'failure'`            | `effective_status IN ('failed', 'killed', 'cancelled')`        |

```sql
-- Before
SELECT id FROM sys_invocation WHERE status = 'backing-off';
-- Now
SELECT id FROM sys_invocation WHERE effective_status = 'backing-off';
```
