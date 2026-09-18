# Release Notes: Eager-preload whitelist for lazy state

## New Feature

### What Changed

The service discovery manifest gained a per-key eager-preload whitelist on top of the existing
`enableLazyState` boolean. A new optional array is accepted at both the service and the handler
level:

- `eagerStateKeysWhitelist`: when `enableLazyState` is `true`, a whitelist of exact state keys to
  still preload eagerly into the invocation's START message.

`enableLazyState` continues to decide the default: with it `false`, all state is preloaded as
before and `alwaysEagerStateKeys` is irrelevant; with it `true`, nothing is preloaded except the
exact keys listed in `alwaysEagerStateKeys`. Handler-level lists override the service-level list.

This is exposed through a new service discovery protocol version, **V5**
(`application/vnd.restate.endpointmanifest.v5+json`).

The list is surfaced read-only on the Admin API service/handler metadata (`eagerStateKeysWhitelist`).

### Why This Matters

Previously eager vs lazy state was an all-or-nothing switch per service/handler. An object with a
large state that mostly wants lazy loading could not cheaply keep a few hot keys eager. The
whitelist allows preloading those specific keys without giving up the lazy default for the rest.

### Behavior and limits

- The invoker's eager state size limit (a memory safety cap, `worker.invoker` configuration) still
  always applies. `eagerStateKeysWhitelist` is therefore **best-effort**: an entry that does not fit
  the cap is not preloaded and is served lazily instead (the START message is marked partial).
- Under a lazy default, the whitelist is served via exact point lookups for just those keys, so
  the object's full state is not scanned. Entries are read on demand while building the START
  message, avoiding reads of the remaining keys once the eager state size limit is reached.
- Whitelisted entries use the same memory-budget handling as full eager state loading: reads
  wait for reclaimable memory when the reservation is feasible and report memory exhaustion
  otherwise, rather than silently omitting the remaining keys on a failed reservation.
- In case of a rollback to a previous restate version, the behavior of `eagerStateKeysWhitelist` will
  stop applying.

### Impact on Users

- Existing deployments and manifests are unaffected: with the new array absent, behavior is
  identical to before (`enableLazyState` alone still decides).
- To use the feature, the SDK must emit the new manifest field and negotiate discovery protocol
  V5. Until the SDK is updated, the field is simply not present.

### Related Issues

- Discovery manifest bump to V5; eager-preload whitelist for lazy state.
