# Release Notes for Issue #4425: KilledEvent in sys_journal_events

## Improvement

### What Changed

When an invocation is killed — either via the Admin API or after exhausting all retry attempts
with `onMaxAttempts: 'kill'` — a `Killed` journal event is now written to `sys_journal_events`.

Previously, kill-after-max-retries would surface as a plain failure with the raw service error,
making it impossible to distinguish from a genuine service error. The Admin API kill path also
wrote no journal event at all.

### New Behaviour

- **`onMaxAttempts: 'kill'`**: When retries are exhausted, a `Killed` event is written with the
  last transient error attached as `last_failure`. The final invocation result is now always
  `KILLED_INVOCATION_ERROR` (code `ABORTED`, message `"killed"`) instead of the raw service error.

- **Admin API kill**: A `Killed` event with no `last_failure` is written when an active
  (invoked, suspended, or paused) invocation is killed via the Admin API.

### Querying

```sql
-- Find all killed invocations and their last error
SELECT id, event_json
FROM sys_journal_events
WHERE event_type = 'Killed'
```

The `event_json` column contains a JSON object of the form:

```json
{ "ty": "Killed", "last_failure": { "error_code": 500, "error_message": "..." } }
```

`last_failure` is absent when the invocation was killed via the Admin API without any prior
retry failures.

### Impact on Users

- **Breaking**: The final error for kill-after-max-retries invocations changes from the raw
  service error to `KILLED_INVOCATION_ERROR`. Code that inspects the error payload to detect
  kills should migrate to querying `sys_journal_events` for `event_type = 'Killed'`.
- **UI**: The UI can now reliably display "killed after N retries" by reading the `Killed` event.
