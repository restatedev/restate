# Release Notes: Store large invocation outputs separately (experimental)

## New Feature

### What Changed

With the experimental option storing large invocation outputs separately
(`experimental-enable-write-blob-table`), the invocation status record now refers to a large
invocation output stored that way, instead of embedding a copy of it. Small outputs stay
embedded.

### Why This Matters

Large responses no longer bloat the invocation status record, which is read far more often than
the output itself, and are stored once for both the journal and the invocation status.

### Impact on Users

Regardless of the option, restarting an invocation as new now rejects a journal prefix that
includes the invocation's output entry (`Command: Output`). Copying the output made the new
invocation complete right away with the same result. Pick a prefix that ends before the output.

Otherwise, nothing changes without the option.

With the option enabled, invocations that complete afterwards are reported differently in
`sys_invocation_status`:

- For outputs stored separately, `completion_failure` shows the error message truncated to its
  first 200 characters. The full message is in the invocation's last `sys_journal` entry
  (`Command: Output`), as long as the journal is retained.
- The new column `completion_failure_code` holds the error code of failed invocations.

In `sys_journal`, killed and failed invocations now also end with a `Command: Output` entry
carrying the failure, like completed invocations do. Invocations that are killed or fail before
reaching a deployment have no such entry.

Invocations that completed before the option was enabled keep their previous representation.

Once enabled, the option cannot be turned off again: removing it from the configuration does not
switch already-upgraded partitions back.

### Migration Guidance

Enable the option on every node, after the whole cluster runs this version:

```toml
experimental-enable-write-blob-table = true
```

Or via the environment variable `RESTATE_EXPERIMENTAL_ENABLE_WRITE_BLOB_TABLE=true`.

Read the full failure message of a failed invocation from its journal:

```sql
SELECT j.id, j.entry_json
FROM sys_journal j
JOIN sys_invocation_status s ON s.id = j.id
WHERE s.completion_result = 'failure' AND j.entry_type = 'Command: Output';
```
