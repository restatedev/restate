# Bifrost: Adaptive store timeouts for sequencer appends

## Behavioral Change / Deprecation

### What Changed

The sequencer now uses adaptive timeouts when sending store waves to log servers,
replacing the previous fixed-timeout + exponential-backoff retry policy.

The `rpc_timeout` configuration field under
`[bifrost.replicated-loglet]` has changed type from a fixed `Duration` to a
`BackoffInterval`, expressed as a compact duration-range string `"<min>..<max>"`:

```toml
[bifrost.replicated-loglet]
rpc_timeout = "250ms..15s"   # default
```

The old `sequencer_retry_policy` field is deprecated and will be ignored if
present. Remove it from your configuration.

### Why This Matters

Fixed timeouts are fragile: too low and transient slowdowns cause unnecessary
retries; too high and genuinely failed nodes stall progress for longer than
needed. The adaptive algorithm tracks per-node store latencies in a sliding
window and derives the wave timeout from the P99.99 of recent observations,
so the timeout automatically tightens when nodes are fast and relaxes when
they are slow — without any manual tuning.

### Impact on Users

- **Existing deployments**: Will automatically benefit from adaptive timeouts
  on upgrade. No action required unless you have customised `sequencer_retry_policy`.
- **Customised retry policy**: If you previously tuned `sequencer_retry_policy`,
  migrate to `rpc_timeout` (see Migration Guidance below). The new field
  covers both the per-wave timeout and the inter-wave backoff delay.
- **New deployments**: The default `"250ms..60s"` range applies.

### Migration Guidance

Remove `sequencer_retry_policy` from your configuration and, if needed, set
`rpc_timeout` to reflect your desired min/max bounds:

```toml
# Before (deprecated, will be ignored)
[bifrost.replicated-loglet]
sequencer_retry_policy = { type = "exponential", initial_delay = "250ms", max_delay = "5s" }

# After
[bifrost.replicated-loglet]
rpc_timeout = "250ms..15s"
```

The duration-range format accepts the same unit designators as the rest of
Restate's configuration (jiff-compatible: `ms`, `s`, `m`, `h`, `d`, and
verbose forms). Fractional values such as `0.5s` are also accepted.
