# Release Notes: Configurable HTTP/2 DATA frame budget

## New Feature

### What Changed

The HTTP/2 connection pool used by the invoker now exposes its connection-level DATA frame budget
as a configuration option, `http2-data-frame-budget`, alongside the existing HTTP/2 knobs under
`[worker.invoker]`. It was previously hardcoded to 25600 B.

```toml
[worker.invoker]
http2-data-frame-budget = "1 MiB"
```

Values below 25 KiB (25600 B) are raised to 25600 B, which is the underlying HTTP/2
implementation's own default.

### Why This Matters

HTTP/2 flow control bounds the number of payload bytes in flight, but not the number of frames
carrying them. A deployment that fragments its response into many tiny DATA frames causes
disproportionate memory usage on the client side. Small frames consume this budget, larger frames
replenish it, and exhausting the budget closes the connection with `ENHANCE_YOUR_CALM`.

Making the budget configurable lets operators raise it for deployments that legitimately stream
many small frames, or lower it to tighten the protection.

### Impact on Users

- Existing deployments: no behavioral change.
- New deployments: same default.
- Only applies to service endpoints served through the HTTP/2 connection pool
  (`http-version = "h2"`).

### Migration Guidance

None required. Set `http2-data-frame-budget` under `[worker.invoker]` only if you observe
connections being closed with `ENHANCE_YOUR_CALM` against a deployment that streams many small
DATA frames.
