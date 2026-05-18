# Record `SuspendedEvent` on invocation suspension

## New Feature

A new `SuspendedEvent` is appended to the journal events stream when an invocation transitions to `Suspended`.
This event is apppended only when using the `RESTATE_EXPERIMENTAL_FEATURE_ALLOW_PROTOCOL_V7=true`.
