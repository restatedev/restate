# Release Notes for Issue #4566: Fix awakeable signal loss during partition leadership transitions

## Bug Fix

### What Changed

Fixed a bug where awakeable resolutions and signal deliveries could be silently lost
during partition leadership transitions. When a leadership change occurred, the old
leader's self-proposed commands (e.g., `AppendSignal` for awakeable resolution) could
be committed to the Bifrost log after the new leader's `AnnounceLeader` entry. The
dedup mechanism then dropped these commands because entries from an earlier epoch are
considered outdated once an entry from a newer epoch has been stored.

Signal and invocation-response commands from ingress RPCs are now appended to Bifrost
without dedup information. Without dedup, these records are never filtered during
leadership transitions. The SDK handles potential duplicate signals gracefully.

### Impact on Users

- **Existing deployments**: Awakeable resolutions and signal deliveries that were
  previously accepted by the ingress but silently dropped during leadership transitions
  will now be reliably applied.
- **New deployments**: No special considerations.

### Migration Guidance

No action required.

### Related Issues

- Issue #4566
