# Release Notes for Issue #4543: Remove handler removal check on deployment registration

## Behavioral Change

### What Changed
Restate no longer rejects deployment registrations that remove handlers from an existing service revision. Previously, registering a new deployment that removed handlers from a service would fail with error META0006 unless the `breaking` flag was set.

### Why
Removing handlers is a valid operation during service evolution and should not require forcing.

### Impact
- Deployments that remove handlers from existing services will now succeed without needing the `--breaking` flag.
- The `breaking` flag now only controls whether service type changes are allowed.
