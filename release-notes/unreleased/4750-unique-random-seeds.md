# Release Notes: Deterministic random seeds for restart-from-prefix

## Bug Fix

### What Changed

Restart-from-prefix could replay copied prefix entries against a different
random seed than the original execution used. The seed that `ctx.rand()`
exposes to SDKs was derived lazily from the invocation id at invoke time, and
restart-from-prefix mints a new invocation id — so any handler that consumed
`ctx.rand()` inside the copied prefix could observe non-deterministic replay
or replay errors.

The fix persists a unique random seed on every new invocation, and carries 
that seed over when restart-from-prefix replays existing entries. This is a
backwards incompatible change which means that one cannot roll back to v1.6.x
once it is enabled.

### Why This Matters

Users who use (or plan to use) restart-from-prefix on services that call
`ctx.rand()` are exposed to the determinism bug on v1.7.0 unless they opt in.
Anyone considering restart-from-prefix should turn the flag on; everyone else
can safely wait for the default flip in v1.8.0.

### How to Opt In

In `restate.toml`:

```toml
[common]
experimental_enable_unique_random_seeds = true
```

Once this option has been applied, rollback to a Restate server version 
< v1.7.0 is no longer possible.

### Scope of the Fix

- **Invocations created before activation** keep generating the random seed
  based on the invocation id. There is no backfill, so restart-from-prefix on 
  these pre-existing invocations remains affected by the bug.
- **Invocations created after activation** get a persisted seed, so any
  subsequent restart-from-prefix on them is deterministic.
