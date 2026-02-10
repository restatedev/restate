# Release Notes: Support OTEL_RESOURCE_ATTRIBUTES environment variable

## New Feature

### What Changed
Restate now reads the `OTEL_RESOURCE_ATTRIBUTES` environment variable and merges
those attributes into the resource of all exported spans (both service and
runtime traces). This is the only additional `OTEL_*` variable now supported;
other OpenTelemetry environment variables (e.g. `OTEL_SERVICE_NAME`,
`OTEL_EXPORTER_OTLP_ENDPOINT`) remain ignored — use Restate's own configuration
for those.

### Why This Matters
In multi-instance or multi-cluster deployments, operators often need to attach
deployment-specific metadata (e.g. environment, region, pod name) to traces
without changing application configuration. The OpenTelemetry specification
defines `OTEL_RESOURCE_ATTRIBUTES` for exactly this purpose, but Restate was
previously not picking it up.

### Impact on Users
- **Existing deployments**: No change in behavior unless `OTEL_RESOURCE_ATTRIBUTES`
  is already set in the environment. If set, those attributes will now appear on
  exported spans.
- **New deployments**: Operators can set `OTEL_RESOURCE_ATTRIBUTES` to attach
  custom resource attributes to all traces.
- Restate's own resource attributes (`service.name`, `service.namespace`,
  `service.version`, `service.instance.id`) take precedence over
  environment-provided values with the same keys.

### Migration Guidance
No action required. To add custom attributes:

```bash
OTEL_RESOURCE_ATTRIBUTES="deployment.environment=staging,cloud.region=us-east-1" \
  restate-server --tracing-endpoint http://localhost:4317
```
