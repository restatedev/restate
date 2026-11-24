# Release Notes: Explicit recovery protocol (experimental)

## New Feature

### What Changed
Restate can run handlers on a second service protocol, the explicit recovery protocol. A handler
on it has no journal: every attempt runs it from its first line, and the handler finds out how far
an earlier attempt got by reading state. Besides the object's state, each invocation has its own
invocation state, and signals it receives by name.

The protocol is off by default. Enable it with the experimental option
`experimental-enable-explicit-recovery-protocol` (environment variable
`RESTATE_EXPERIMENTAL_ENABLE_EXPLICIT_RECOVERY_PROTOCOL=true`). It is checked when a deployment
is registered. Deployments declare which handlers use it in endpoint manifest version 6, which
needs HTTP/2 and the `BIDI_STREAM` protocol mode.

A new option, `worker.invoker.explicit-recovery-shutdown-timeout` (5 seconds by default), bounds
how long Restate waits for an SDK to answer a shutdown of an attempt on this protocol.

### Impact on Users
- Nothing changes while the option is off, and for handlers on the durable execution protocol.
- The protocol is experimental: its messages, and what it stores, may change in incompatible ways.
  Do not use it in production, and do not roll back to an older Restate version once invocations
  on it exist.
