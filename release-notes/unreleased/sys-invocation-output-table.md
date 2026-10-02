# Release Notes: `sys_invocation_output` SQL table

## New Feature

### What Changed

A new experimental option stores invocation output payloads in a dedicated table instead of
embedding them in the invocation status record. When the option is enabled, a new SQL table,
`sys_invocation_output`, exposes them:

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

Large responses no longer bloat the invocation status record. `sys_invocation_status` still
reports *whether* an invocation completed successfully, and `sys_invocation_output` is where the
response payload is readable.

### Impact on Users

Without the option, nothing changes and `sys_invocation_output` stays empty.

With the option enabled, invocations that complete afterwards are reported differently in
`sys_invocation_status`:

- `completion_result` is `killed` for killed invocations (previously `failure`). Queries that
  filter on `completion_result = 'failure'` to find killed invocations must also match `'killed'`.
- `completion_failure` contains only the error code (e.g. `[500]`) and no longer includes the
  error message. Read the message from `sys_invocation_output.failure_json` instead.
- `completion_failure_code` is unchanged.

Invocations that completed before the option was enabled keep their previous representation.

Once enabled, the option cannot be turned off again: removing it from the configuration does not
switch already-upgraded partitions back.

### Migration Guidance

Enable the option on every node, after the whole cluster runs this version:

```toml
experimental-enable-write-output-table = true
```

Or via the environment variable `RESTATE_EXPERIMENTAL_ENABLE_WRITE_OUTPUT_TABLE=true`.

Read the payload from the new table:

```sql
SELECT s.id, s.target, o.result, o.output_utf8
FROM sys_invocation_status s
JOIN sys_invocation_output o ON s.id = o.id
WHERE s.status = 'completed';
```
