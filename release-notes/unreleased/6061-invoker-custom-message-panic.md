# Invoker no longer panics on custom protocol messages

## Bug Fix

If a service deployment sent a protocol message with a custom or unassigned message type,
the invoker panicked and the partition processor restarted in a loop on the same invocation.
While this happened, the invocation could not be killed from the admin API.

Such an attempt now fails with error RT0012 ("received unexpected message") and is retried
under the service's retry policy, like other protocol violations. The partition processor
keeps running, and the invocation can be killed or paused as usual. No configuration changes
are required.

### Related Issues
- Issue #6061: Invoker panics when a deployment sends a custom / unassigned message type
