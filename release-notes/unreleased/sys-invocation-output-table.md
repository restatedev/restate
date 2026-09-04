# Release Notes: `sys_invocation_output` SQL table

## New Feature

### What Changed

Invocation output payloads now live in a dedicated partition-store table instead of being embedded
in the invocation status record. A new SQL table, `sys_invocation_output`, exposes them:

| Column | Type | Description |
| --- | --- | --- |
| `partition_key` | `UInt64` | Internal partitioning column. |
| `id` | `LargeUtf8` | Invocation ID. |
| `result` | `LargeUtf8` | Either `success` or `failure`. |
| `output` | `LargeBinary` | Uninterpreted invocation output. NULL if `result = 'failure'`. |
| `output_utf8` | `LargeUtf8` | The output as a string, when it is valid UTF-8 (the case for JSON, the SDK default). |
| `failure_code` | `UInt32` | Error code. NULL if `result = 'success'`. |
| `failure_json` | `LargeUtf8` | Error serialized as JSON. NULL if `result = 'success'`. |

The table is available both from a running cluster (`restate sql`) and from the offline snapshot
inspector (`restate-doctor snapshot`).

### Why This Matters

`sys_invocation_status` still reports *whether* an invocation completed successfully through its
`completion_result` / `completion_failure` / `completion_failure_code` columns, but it no longer
carries the response payload. `sys_invocation_output` is where the payload is now readable.

### Impact on Users

- Existing queries against `sys_invocation_status` keep working unchanged.
- Queries that need the response body should join `sys_invocation_output` on `id`.

### Migration Guidance

Read the payload from the new table:

```sql
SELECT s.id, s.target, o.result, o.output_utf8
FROM sys_invocation_status s
JOIN sys_invocation_output o ON s.id = o.id
WHERE s.status = 'completed';
```
