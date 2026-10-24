# Release Notes: `sys_blob` SQL table

## New Feature

### What Changed

A new experimental option stores invocation output payloads in a dedicated table instead of
embedding them in the invocation status record. When the option is enabled, a new SQL table,
`sys_blob`, exposes the stored blobs. Each row is one blob; for invocation outputs
(`kind = 'output'`) the payload is decoded into the result columns:

| Column | Type | Description |
| --- | --- | --- |
| `partition_key` | `UInt64` | Internal partitioning column. |
| `id` | `LargeUtf8` | Blob ID (`blob_1...`). Matches `sys_invocation_status.completion_blob_id`. |
| `kind` | `LargeUtf8` | Either `output`, `state` or `unknown`. |
| `ref_count` | `UInt32` | Number of references to the blob. `0` means it is marked for deletion. |
| `payload_length` | `UInt64` | Size of the blob payload in bytes. |
| `result` | `LargeUtf8` | Either `success` or `failure`. NULL if `kind != 'output'`. |
| `output` | `LargeBinary` | Uninterpreted invocation output. NULL if `result = 'failure'`. |
| `output_utf8` | `LargeUtf8` | The output as a string, when it is valid UTF-8 (the case for JSON, the SDK default). |
| `failure_code` | `UInt32` | Error code. NULL if `result = 'success'`. |
| `failure_json` | `LargeUtf8` | Error serialized as JSON. NULL if `result = 'success'`. |

The table is available both from a running cluster (`restate sql`) and from the offline snapshot
inspector (`restate-doctor snapshot`).

### Why This Matters

Large responses no longer bloat the invocation status record. `sys_invocation_status` still
reports *whether* an invocation completed successfully, and `sys_blob` is where the
response payload is readable.

### Impact on Users

Without the option, nothing changes and `sys_blob` stays empty.

With the option enabled, invocations that complete afterwards are reported differently in
`sys_invocation_status`:

- `completion_result` is `killed` for killed invocations (previously `failure`). Queries that
  filter on `completion_result = 'failure'` to find killed invocations must also match `'killed'`.
- `completion_failure` contains only the error code (e.g. `[500]`) and no longer includes the
  error message. Read the message from `sys_blob.failure_json` instead.
- `completion_failure_code` is unchanged.

In `sys_journal`, the last entry of a completed invocation is `Command: OutputMarker` instead of
`Command: Output`. The marker carries no payload; read the output from `sys_invocation_output`.
Killed and failed invocations also end with an `OutputMarker` entry. An invocation can't be
restarted from a journal prefix that includes the marker.

Invocations that completed before the option was enabled keep their previous representation.

Once enabled, the option cannot be turned off again: removing it from the configuration does not
switch already-upgraded partitions back.

### Migration Guidance

Enable the option on every node, after the whole cluster runs this version:

```toml
experimental-enable-write-blob-table = true
```

Or via the environment variable `RESTATE_EXPERIMENTAL_ENABLE_WRITE_BLOB_TABLE=true`.

Read the payload from the new table:

```sql
SELECT s.id, s.target, o.result, o.output_utf8
FROM sys_invocation_status s
JOIN sys_blob o ON s.completion_blob_id = o.id
WHERE s.status = 'completed';
```
