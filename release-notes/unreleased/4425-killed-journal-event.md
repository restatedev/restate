# Release Notes for Issue #4425: KilledEvent in sys_journal_events

## Breaking Change

### What Changed

When an invocation is killed — either via the Admin API or after exhausting all retry
attempts with `onMaxAttempts: 'kill'` — a `Killed` journal event is now written to
`sys_journal_events`.

Previously, kill-after-max-retries surfaced as a plain failure carrying the raw service
error, making it impossible to distinguish from a genuine service error. The Admin API
kill path also wrote no journal event at all.

### Why This Matters

Without a `Killed` event, the UI and operators had no reliable way to tell whether an
invocation failed naturally or was deliberately killed. Querying "show me all killed
invocations" was impossible. This change makes kills a first-class observable event.

### New Behaviour

- **`onMaxAttempts: 'kill'`**: When retries are exhausted, a `Killed` event is written
  with the last transient error attached as `last_failure`. The final invocation result
  is now always `KILLED_INVOCATION_ERROR` (code `ABORTED`, message `"killed"`) instead
  of the raw service error.

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

`last_failure` is absent when the invocation was killed via the Admin API without any
prior retry failures.

### Impact on Users

- **Breaking**: The final error for `onMaxAttempts: 'kill'` invocations changes from the
  raw service error to `KILLED_INVOCATION_ERROR` (code `ABORTED`, message `"killed"`).
- **UI**: The UI can now reliably display "killed after N retries" by reading the `Killed`
  event.
- Existing deployments: No configuration change needed. The new event type is written
  automatically from the next invocation kill onwards.

### Migration Guidance

If you have code that inspects the invocation error payload to detect kills (e.g. matching
on a specific error message or code from your service), migrate it to query
`sys_journal_events` instead:

```sql
-- Before: fragile, depends on the raw service error
SELECT id FROM invocation WHERE last_failure_message = 'my-service-error'

-- After: reliable, event_type = 'Killed' is definitive
SELECT id, event_json
FROM sys_journal_events
WHERE event_type = 'Killed'
```

### Related Issues

- Issue #4425: Write KilledEvent to sys_journal_events on invocation kill
