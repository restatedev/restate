# Release Notes for Issue #4536: Remove the idempotency table

## Internal Cleanup

### What Changed
The legacy idempotency table has been removed from the partition store along with all related code (the `IdempotencyTable` storage trait, the `sys_idempotency` SQL table, the `IdempotencyMetadata` protobuf message and Rust struct, the `TableKind::Idempotency` variant, and the read/write fallbacks in the state machine and RPC handlers).

The `KeyKind::Idempotency` variant is preserved (marked `#[deprecated]`) so its `b"ip"` byte prefix stays reserved for any leftover on-disk entries.

### Why
Restate stopped writing to the idempotency table in v1.3 — invocation deduplication has since used the deterministic invocation id derived from the idempotency key. By the time v1.7 ships, any entries that were written before v1.3 will have aged well past their retention window, so the table can no longer satisfy any live request and its read/cleanup paths are dead weight.

### Impact
- No user-visible behavior change. Idempotent invocations continue to work via the deterministic invocation id introduced in v1.3.
- The `sys_idempotency` SQL table is no longer available through the storage query interface. Any tooling that selected from it must be updated.
- Storage data files written by v1.2 or older that still contain idempotency entries will not block startup (the byte prefix stays reserved), but those entries will no longer be consulted on duplicate detection.

### Migration Guidance
None required for users running v1.3 or newer.
