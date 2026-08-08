# Wait for a replacement partition replica to become ready

## Bug Fix

### What Changed

Restate now waits for a newly added partition processor to catch up before completing a replica-set change.

### Why This Matters

During automatic replica replacement, a retained active member no longer makes the next configuration current while the replacement replays the log or restores a snapshot. This prevents the known-unready replacement from being selected for leadership.

### Impact on Users

- Rolling maintenance and node failures are less likely to select a replacement replica before it catches up.
- No configuration or migration is required.
