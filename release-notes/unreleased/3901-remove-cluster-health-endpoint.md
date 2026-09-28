# Release Notes for Issue #3901: Remove the `/cluster-health` admin endpoint

## Breaking Change

### What Changed

The `/cluster-health` endpoint on the admin API (port 9070) has been removed. It was deprecated
in v1.6.0 and is no longer part of the admin API or its OpenAPI specification.

### Why This Matters

This endpoint was unintentionally exposed publicly and is not used by Restate internally. It did
not provide meaningful health information for external monitoring.

### Impact on Users

- Requests to `/cluster-health` now return `404 Not Found`.
- Liveness/readiness probes or scripts still calling `/cluster-health` will fail after upgrading.
- Deployments that already migrated away from the endpoint following the v1.6.0 deprecation are
  not affected.

### Migration Guidance

Replace any usage of `/cluster-health` with the health endpoints:

```bash
# Old (removed)
curl http://localhost:9070/cluster-health

# New
curl http://localhost:9070/health          # Admin API health
curl http://localhost:8080/restate/health  # Ingress health
```

To inspect the state of the cluster, use `restatectl status`.

### Related Issues

- [#3901](https://github.com/restatedev/restate/issues/3901): Remove the `/cluster-health` endpoint
- [#3898](https://github.com/restatedev/restate/issues/3898): Deprecate the `/cluster-health` endpoint (v1.6.0)
