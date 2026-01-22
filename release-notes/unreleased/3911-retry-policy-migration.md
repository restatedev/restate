# Release Notes: Invoker Retry Policy Configuration Migration

## Breaking Change

### What Changed

The deprecated `retry-policy` configuration option under `worker.invoker` has been removed. Users must migrate to the `default-retry-policy` option under the `invocation` section.

### Why This Matters

The new retry policy configuration provides:
- More intuitive configuration location under `invocation`
- New `on-max-attempts` behavior option (pause or kill)
- Per-service and per-handler retry policy overrides via SDK configuration

### Impact on Users

- **Existing configuration**: If you configured `worker.invoker.retry-policy`, Restate will ignore this option
- **Default behavior change**: Invocations now **pause** by default when max attempts are reached, instead of being killed as it happened with the old `retry-policy`

### Migration Guidance

**Before (no longer supported):**
```toml
[worker.invoker]
retry-policy = { type = "exponential", initial-interval = "50ms", factor = 2.0, max-attempts = 200, max-interval = "20s" }
```

**After:**
```toml
[invocation.default-retry-policy]
initial-interval = "50ms"
exponentiation-factor = 2.0
max-attempts = 200
max-interval = "20s"
on-max-attempts = "kill"  # Use "kill" to match previous behavior, or "pause" (new default)
```

**New defaults:**

| Parameter        | Old Default     | New Default |
|------------------|-----------------|-------------|
| `max-attempts`   | 200             | 70          |
| `max-interval`   | 20s             | 60s         |
| `on-max-attempts` | kill (implicit) | pause       |

**Per-service overrides**: Retry policies can also be customized per service or handler using the respective SDK APIs. See the [Restate documentation on retries](https://docs.restate.dev/services/configuration#retries) for more details.

### Related Issues

- [#3911](https://github.com/restatedev/restate/pull/3911): Remove old invoker retry policy
