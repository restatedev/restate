# Release Notes: Configurable default and max for idempotency and workflow completion retention

## New Feature

### What Changed

`InvocationOptions` now exposes four new server-wide retention knobs alongside the existing
`default-journal-retention` / `max-journal-retention` pair:

- `default-idempotency-retention`
- `max-idempotency-retention`
- `default-workflow-completion-retention`
- `max-workflow-completion-retention`

The `default-*` options provide the fallback retention used when neither the service nor the
handler configures its own value via the SDK or the Admin API. The `max-*` options act as a
ceiling: when discovering a service deployment, or when serving service/handler metadata, any
configured retention higher than the configured max is clamped down to that max (and a
`SchemaInfo` message is attached, same as journal retention today).

Hardcoded 24-hour defaults that were previously baked into discovery (`DEFAULT_IDEMPOTENCY_RETENTION`,
`DEFAULT_WORKFLOW_COMPLETION_RETENTION`) are now expressed through the new config options.

### Why This Matters

Operators can now centrally control idempotency and workflow completion retention from
`restate.toml` — both the default applied to services that don't set a value, and a hard
ceiling that protects against misconfigured services or accidental retention explosions.
Previously, only journal retention had this treatment; idempotency and workflow retention
were stuck at a fixed 24-hour default with no max.

### Impact on Users

- **Defaults unchanged**: the new `default-*` options default to `24h`, matching the prior
  hardcoded behavior. Existing deployments will not see any retention difference on upgrade.
- **Storage semantics unchanged**: just like journal retention, the values you (or your SDK)
  configure are stored verbatim; clamping happens at read time. The `modify_service` Admin
  API path is unaffected.
- **New opt-in capability**: setting the `max-*` options will start clamping configured
  retentions when they exceed your limit.

### Configuration Example

```toml
default-idempotency-retention = "1h"
max-idempotency-retention = "7d"
default-workflow-completion-retention = "30d"
max-workflow-completion-retention = "90d"
```
