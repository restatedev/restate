# Release Notes: Error Events Enabled by Default

## New Feature

### What Changed

Error events are now enabled by default. When invocations encounter transient failures, Restate automatically records detailed error information in the journal, accessible via the `sys_journal_events` SQL table.

### Why This Matters

- **Better observability**: See exactly why invocations are failing during retries
- **Debugging**: Access error codes, messages, stacktraces, and the related command that caused the failure

### Impact on Users

- **Improved observability in the invocation timeline UI**: Error events are now visible in the Restate UI invocation timeline
- **New SQL table**: `sys_journal_events` is now populated with error events
- **No configuration needed**: Error events are recorded automatically
- **Removed config**: The experimental flag `worker.invoker.experimental_features_propose_events` has been removed

### Usage

Query error events via SQL:
```sql
SELECT * FROM sys_journal_events WHERE id = '<invocation-id>'
```

**Table columns:**
- `id`: The invocation ID
- `after_journal_entry_index`: Journal index after which the event occurred
- `appended_at`: Timestamp when the event was recorded
- `event_type`: Either `TransientError` or `Paused`
- `event_json`: Full event details as JSON

**Event types:**

1. **TransientError**: Recorded when an invocation fails with a transient error and will be retried
   - Includes: error code, message, stacktrace (if available), related command info

2. **Paused**: Recorded when an invocation is paused (manually or due to max retries)
   - Includes: last failure information if the pause was due to errors

**Example event_json for TransientError:**
```json
{
  "error_code": 500,
  "error_message": "Connection refused",
  "error_stacktrace": "...",
  "restate_doc_error_code": "RT0001",
  "related_command_index": 3,
  "related_command_name": "Call",
  "related_command_type": "Call"
}
```

### Related Issues

- [#3301](https://github.com/restatedev/restate/pull/3301): Store transient errors in journal events
- [#3843](https://github.com/restatedev/restate/pull/3843): Enable error events
