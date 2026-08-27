# Detect stalled connections to the AWS Lambda API

## Behavioral Change

### What Changed

Restate now enables HTTP/2 keep-alive pings for AWS Lambda invocations. The existing
`worker.invoker.http2-keep-alive-interval` and
`worker.invoker.http2-keep-alive-timeout` options now apply to Lambda and HTTP deployments. Their
defaults remain `40s` and `20s`.

AWS credential providers and STS continue to use the AWS SDK's HTTP client.

### Why This Matters

Lambda invocations share long-lived HTTP/2 connections. A silently dropped connection previously
left its invocations stuck until the kernel exhausted its TCP retransmission budget, which can take
around 15 minutes. Restate now detects the failure within the keep-alive interval plus its timeout
and retries the affected invocations.

### Impact on Users

- Existing deployments require no configuration changes.
- With the defaults, a silent Lambda connection fails within about one minute instead of remaining
  stuck for the kernel's retransmission period.
- Setting `worker.invoker.http2-keep-alive-interval` to `0` disables pings, which restores the old
  behaviour for both HTTP deployments and Lambda.

### Migration Guidance

No action required.
