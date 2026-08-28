# Detect stalled connections to the AWS Lambda API

## Behavioral Change

### What Changed

Restate now enables HTTP/2 keep-alive pings for AWS Lambda invocations. The existing
`worker.invoker.http2-keep-alive-interval` and
`worker.invoker.http2-keep-alive-timeout` options now also apply to Lambda. Their defaults remain
`40s` and `20s`.

### Why This Matters

Lambda invocations share long-lived HTTP/2 connections. Restate now detects a silently dropped
connection within the keep-alive interval plus its timeout instead of waiting for the kernel's TCP
retransmission timeout, which can take around 15 minutes.

### Impact on Users

- With the defaults, a silent Lambda connection fails within about one minute instead of remaining
  stuck for the kernel's retransmission period.
- Setting `worker.invoker.http2-keep-alive-interval` to `0` disables pings.
- Keep-alive pings are an HTTP/2 mechanism. The Lambda API negotiates HTTP/2; a connection that
  ends up on HTTP/1.1 (for example, an endpoint override pointing at a gateway that only speaks
  HTTP/1.1) keeps the previous behavior.
- AWS credential providers and STS continue using the SDK's HTTP client. Lambda continues honoring
  the SDK's proxy environment variables.

### Migration Guidance

No action required.
