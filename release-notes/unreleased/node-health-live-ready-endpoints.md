# Liveness and readiness endpoints for health probes

## New Feature

### What Changed
Every node now serves two health endpoints on its node port (default `5122`):

- `GET /health/live` returns `200 OK` whenever the server process is running and able to answer
  requests. It still returns `200 OK` while the node is starting up or shutting down.
- `GET /health/ready` returns `200 OK` once all of the node's roles have started. While the node is
  starting up or shutting down, it returns `503 Service Unavailable`.

Both endpoints return the same JSON body as the existing `GET /health`, which is unchanged.

### Why This Matters
Until now, probes had to use the Admin API's `/health` endpoint on port `9070`. That endpoint only
exists on nodes that run the admin role, and it only tells you whether the Admin API is up. The new
endpoints work on every node, whatever roles it runs, and their result is carried in the HTTP
status code, which is all that Kubernetes probes look at.

### Impact on Users
- Nothing changes unless you start using the new endpoints.
- If you run Restate on Kubernetes, use `/health/ready` for readiness probes and `/health/live` for
  liveness and startup probes:

  ```yaml
  startupProbe:
    httpGet: { path: /health/live, port: 5122 }
  livenessProbe:
    httpGet: { path: /health/live, port: 5122 }
  readinessProbe:
    httpGet: { path: /health/ready, port: 5122 }
  ```

### Related Issues
- restatedev/restate-operator#240: RestateCluster pods have no liveness probe
