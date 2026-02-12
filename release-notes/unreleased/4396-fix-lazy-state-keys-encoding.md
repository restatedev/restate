# Release Notes for Issue #4396: Fix incorrect encoding of GetLazyStateKeys journal entries

## Bug Fix

### What Changed
Fixed incorrect protobuf encoding/decoding of `GetLazyStateKeys` journal entries. The server was incorrectly storing `GetLazyStateKeys` commands (triggered by `ctx.stateKeys()`) as `GetLazyState` commands (triggered by `ctx.get(key)`) in the journal.

### Symptoms
If you are using **lazy state** (`enableLazyState: true` in the deployment manifest) and your handler calls `ctx.stateKeys()`, you may see invocations stuck in a retry loop with the following journal mismatch error (error code `570`):

```
Found a mismatch between the code paths taken during the previous execution and the paths taken during this execution.
 - The previous execution ran and recorded the following: 'get state keys' (index '...')
 - The current execution attempts to perform the following: 'get state'
```

This error occurs when the invocation is replayed, for example after suspending and resuming in request-response mode.

**Note:** This issue only affects handlers that have lazy state enabled **and** call `ctx.stateKeys()`. Handlers using eager state or only calling `ctx.get(key)` are not affected. The Java SDK is also not affected.

### Impact on Users
- **New invocations** after upgrading: Work correctly with no issues.
- **Existing stuck invocations** created before the upgrade: These invocations have a corrupted journal entry and will **not** be automatically fixed by upgrading the server alone. See "Workaround for Existing Stuck Invocations" below.

### Workaround for Existing Stuck Invocations

Invocations that are already stuck due to this bug have a corrupted journal entry. After upgrading the Restate server, you have two options:

1. **Kill the affected invocations** using the CLI:
   ```bash
   restate invocations cancel --kill <INVOCATION_ID>
   ```
   Then re-invoke the handler. The new invocation will work correctly on the fixed server.

2. **Apply a client-side SDK fix** that tolerates the mismatched journal entry during replay. Please reach out to the Restate team for an SDK patch ([restatedev/sdk-shared-core#60](https://github.com/restatedev/sdk-shared-core/pull/60)) that allows these corrupted journals to replay successfully without needing to kill the invocation.

### Related Issues
- [restatedev/restate#4396](https://github.com/restatedev/restate/issues/4396): `Command: GetLazyState` is used for both state value and state keys retrieval
- [restatedev/restate#4397](https://github.com/restatedev/restate/pull/4397): Fix incorrect encoding/decoding get lazy state keys type
- [restatedev/sdk-shared-core#60](https://github.com/restatedev/sdk-shared-core/pull/60): SDK-side fix for replaying corrupted journals
- [restatedev/sdk-shared-core#61](https://github.com/restatedev/sdk-shared-core/issues/61): `GetStateKeys` replay fails with command type mismatch when lazy state is enabled
