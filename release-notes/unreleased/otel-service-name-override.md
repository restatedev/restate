# Release Notes: Allow overriding the services trace `service.name`

## Behavioral Change

### What Changed

The resource attributes attached to **services traces** (the traces emitted for your deployed
handlers, controlled by `tracing-services-endpoint` / `tracing-endpoint`) can now be overridden via
the standard OpenTelemetry environment variables:

- `OTEL_SERVICE_NAME` sets `service.name`.
- `OTEL_RESOURCE_ATTRIBUTES` (`key1=value1,key2=value2,...`) sets any resource attribute, including
  the `service.*` keys (`service.name`, `service.namespace`, `service.instance.id`,
  `service.version`).

Restate still supplies the same defaults as before (`service.name=Restate`,
`service.namespace=Restate`, `service.instance.id=<cluster>/<node>`, `service.version=<version>`),
but user-provided values now take precedence over them. When both are present, `OTEL_SERVICE_NAME`
wins over a `service.name` set in `OTEL_RESOURCE_ATTRIBUTES`, matching the OpenTelemetry
specification.

### Why This Matters

Previously the four `service.*` keys were hardcoded and applied *after* `OTEL_RESOURCE_ATTRIBUTES`,
so any user-supplied `service.name` was silently overwritten with `Restate`. This prevented
integrating Restate's services traces into observability pipelines that key off `service.name` for
routing, dashboards, or adoption metrics.

### Impact on Users

- **Deployments not setting these env vars**: No change — the defaults remain `service.name=Restate`
  and `service.namespace=Restate`.
- **Deployments setting `OTEL_SERVICE_NAME` or `service.*` in `OTEL_RESOURCE_ATTRIBUTES`**: These
  values are now honored for services traces instead of being overwritten.
- **Runtime traces** (`tracing-runtime-endpoint`) are unchanged: their `service.name` remains
  `<role>@<node-name>` so per-node runtime identity is preserved.
