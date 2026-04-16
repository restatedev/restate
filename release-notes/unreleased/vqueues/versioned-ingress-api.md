# Release Notes: Versioned HTTP ingress API (/api/v1/)

## New Feature

### What Changed

A new versioned HTTP ingress API is available under the `/api/v1/` prefix. This provides a stable, evolvable API surface that supports scope and limit-key for vqueue-based concurrency control.

### New URL Patterns

```
# Non-scoped calls
POST /api/v1/call/{service}/{handler}
POST /api/v1/call/{service}/{key}/{handler}
POST /api/v1/send/{service}/{handler}
POST /api/v1/send/{service}/{key}/{handler}

# Scoped calls (for vqueue concurrency control)
POST /api/v1/scope/{scopeKey}/call/{service}/{handler}
POST /api/v1/scope/{scopeKey}/call/{service}/{key}/{handler}
POST /api/v1/scope/{scopeKey}/send/{service}/{handler}
POST /api/v1/scope/{scopeKey}/send/{service}/{key}/{handler}
```

### Limit Key

A limit key can be provided via header or query parameter:

```
# Via header
POST /api/v1/scope/myScope/call/MyService/myHandler
x-restate-limit-key: tenant1/user42

# Via query parameter (useful for webhook URL templates)
POST /api/v1/scope/myScope/call/MyService/myHandler?limit-key=tenant1/user42
```

If both are present, the header takes precedence.

### Impact on Users

- **Existing deployments**: No impact. The old unversioned paths (`/{service}/{handler}`, etc.) continue to work unchanged.
- **New usage**: Users who want scope-based partitioning or limit-key concurrency control should use the new `/api/v1/` paths.
- **Service name `api`**: A warning (RT0024) is now emitted when registering a service named `api`, as this will become a reserved keyword in a future version.
