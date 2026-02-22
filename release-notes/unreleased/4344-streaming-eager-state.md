# Release Notes for Issue #4344: Streaming Eager State (Protocol V7)

## New Feature

### What Changed
Added service protocol V7, which introduces streaming of eager state entries
to service endpoints. Instead of packing all state key-value pairs into a
single StartMessage (which could exceed the message size limit), the server
now streams state entries in batches via new `EagerStateEntryMessage` and
`EagerStateCompleteMessage` protocol messages.

### Why This Matters
Previously, services with large accumulated state (exceeding the ~32 MB
default message size limit) would fail to invoke because the state couldn't
fit in a single StartMessage. With V7, state of any total size can be
transferred, as long as each individual key-value pair fits within the
message size limit.

### Impact on Users
- This is an opt-in feature: V7 is only negotiated when both the server
  and SDK support it. Existing SDK versions continue to use V1-V6 without
  any change in behavior.
- SDKs must be updated to handle V7 to benefit from streaming state.
- No configuration changes required on the server side.

### Related Issues
- Issue #4344: State can exceed max message size when invoking services
