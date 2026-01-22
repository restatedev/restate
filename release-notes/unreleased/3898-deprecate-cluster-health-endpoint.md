# Release Notes for Issue #3898: Deprecate /cluster-health Endpoint

## Deprecation

### What Changed

The `/cluster-health` endpoint on the admin API (port 9070) is now deprecated and will be removed in v1.7.0.

### Why This Matters

This endpoint was unintentionally exposed publicly and is no longer used by Restate internally. It does not provide meaningful health information for external monitoring purposes.

### Impact on Users

- **If you use `/cluster-health`**: Stop using this endpoint before upgrading to v1.7.0
- **Health checks**: Use the `/health` endpoint on the ingress port (8080) or admin port (9070) instead for liveness/readiness probes

### Migration Guidance

Replace any usage of `/cluster-health` with appropriate alternatives:

```bash
# Old (deprecated, will be removed in v1.7.0)
curl http://localhost:9070/cluster-health

# New alternatives for health checks
curl http://localhost:9070/health  # Admin API health
curl http://localhost:8080/restate/health  # Ingress health
```

### Related Issues

- [#3898](https://github.com/restatedev/restate/issues/3898): Deprecate cluster-health endpoint
