# Liveness and readiness endpoints for health probes

## New Feature

### What Changed
Every node now serves two health endpoints on its node port (default `5122`):

- `GET /health/live` returns `200 OK` whenever the server process is running and able to answer
  requests. It still returns `200 OK` while the node is starting up or shutting down.
- `GET /health/ready` returns `200 OK` once all of the node's roles have started. While the node is
  starting up or shutting down, it returns `503 Service Unavailable`. A node only becomes ready
  once the cluster has been provisioned, so on a fresh cluster `/health/ready` keeps returning
  `503` until you provision it.

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
    periodSeconds: 10
    failureThreshold: 30
  livenessProbe:
    httpGet: { path: /health/live, port: 5122 }
  readinessProbe:
    httpGet: { path: /health/ready, port: 5122 }
  ```

  The node port only starts answering after the node has opened its local storage, which can take
  a while on nodes with a lot of data. The startup probe above allows up to 5 minutes for this
  before Kubernetes restarts the pod; raise `failureThreshold` if your nodes need longer.
- Nodes must be able to reach each other before they become ready. If they discover each other
  through a headless Service, set `publishNotReadyAddresses: true` on it (the Restate Helm chart
  already does this). For the same reason, don't provision a fresh cluster through a Service that
  only routes to ready pods.

### Related Issues
- restatedev/restate-operator#240: RestateCluster pods have no liveness probe
