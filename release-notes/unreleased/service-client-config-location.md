# Release Notes: Move `service-client` options under `worker.invoker`

## Deprecation

### What Changed

The service-client configuration options — HTTP client tuning, AWS Lambda options, request
identity, and additional outbound headers — have moved from the TOML root to a dedicated
section under `worker.invoker`. The previous root-level location continues
to work but is deprecated and will be removed in Restate v1.8.

Affected keys (every key in this list previously lived at the TOML root):

- HTTP client: `http-keep-alive-options.interval`, `http-keep-alive-options.timeout`,
  `http-keep-alive-options.jitter`, `http-proxy`, `no-proxy`, `connect-timeout`,
  `initial-max-send-streams`, `streams-per-connection-limit`, `idle-connection-timeout`,
  `http2-initial-stream-window-size`, `http2-initial-connection-window-size`,
  `http2-max-frame-size`
- AWS Lambda: `aws-profile`, `aws-assume-role-external-id`, `request-compression-threshold`
- Identity / headers: `request-identity-private-key-pem-file`, `additional-request-headers`

### Why This Matters

These options govern the HTTP/Lambda client Restate uses to invoke service deployments —
the worker's invoker. Their presence at the TOML root was a side effect of how the original
config struct was flattened, and obscured their scope (the same keys at root read like they
could apply to other HTTP clients in the system). Grouping them under
`worker.invoker` makes the ownership explicit and aligns them with the
other `worker.invoker.*` settings.

### Migration Guidance

Move existing settings from the TOML root into the new section. For example:

```toml
# Before — at the TOML root
http-proxy = "http://proxy.internal:3128"
no-proxy = "localhost,127.0.0.1"
aws-profile = "restate-prod"
request-identity-private-key-pem-file = "/var/secrets/key.pem"

[http-keep-alive-options]
interval = "1m"
timeout = "30s"
```

becomes:

```toml
# After — under worker.invoker
[worker.invoker]
http-proxy = "http://proxy.internal:3128"
no-proxy = "localhost,127.0.0.1"
aws-profile = "restate-prod"
request-identity-private-key-pem-file = "/var/secrets/key.pem"

[worker.invoker.http-keep-alive-options]
interval = "1m"
timeout = "30s"
```

### Impact on Users

- **Existing configs continue to work**: any affected key at the TOML root is still read
  on startup and emits a deprecation warning telling you to move it.
- **When both locations set the same key**, the deprecated (root) value takes precedence
  and a warning is emitted. Remove the deprecated key once you've moved it to avoid the
  warning and surprises when the old location is removed.
- **Removal target**: the root-level keys will be removed in Restate v1.8. Migrate during
  the v1.7 window.
