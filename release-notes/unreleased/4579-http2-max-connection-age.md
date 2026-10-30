# Release Notes for Issue #4579: Max connection age for HTTP deployments

## New Feature

### What Changed
Restate can now retire its HTTP/2 connections to service deployments once they reach a maximum age,
set with the new `http2-max-connection-age` option. A retired connection takes no new invocations.
Invocations already running on it finish normally, and the connection then closes. The next
invocation opens a fresh connection.

A second option, `http2-max-connection-age-jitter`, spreads the retirement time of each connection
by up to a fraction of the max age (default `0.1`, that is up to 10% later), so connections opened
together are not all replaced at the same moment.

### Why This Matters
Restate keeps its connections to a deployment open for a long time and sends many invocations over
each one. Behind an L4 load balancer, such as a Kubernetes `ClusterIP` service, every connection stays
on the pod it was first routed to. Pods added later, for example by a scale-out or a rolling
deployment, get no invocations until Restate happens to open a new connection, which under steady
load can take a very long time. With a max connection age, connections are replaced regularly and
spread over the pods that exist now.

### Impact on Users
- The option is unset by default, so existing and new deployments behave exactly as before.
- When set, values below 10 minutes are raised to 10 minutes, to avoid reconnecting too often.
- In-flight invocations are never interrupted when a connection is retired.

### Migration Guidance
None required. To turn it on, for example behind a Kubernetes `ClusterIP` service:

```toml
[worker.invoker]
http2-max-connection-age = "15m"
# optional, default 0.1 (up to +10%)
http2-max-connection-age-jitter = 0.1
```

### Related Issues
- Issue #4579: Follow up on connection pool and connection eviction
- PR #5644: connection-pool: retire connections past max_connection_age
