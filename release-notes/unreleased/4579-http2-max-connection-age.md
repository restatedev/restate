# Release Notes for Issue #4579: Max connection age for HTTP deployments

## New Feature / Behavioral Change

### What Changed
Restate now retires its HTTP/2 connections to service deployments once they reach a maximum age,
set with the new `http2-max-connection-age` option (default `5m`). A retired connection takes no new invocations.
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
- After upgrading, each connection to a deployment is replaced roughly every 5 minutes (plus up to
  10% jitter). Set `http2-max-connection-age = "0"` to keep the previous behavior of no age limit.
- Values between 0 and 30 seconds are raised to 30 seconds, to avoid reconnecting too often.
- In-flight invocations are never interrupted when a connection is retired.

### When to Use It
- **Behind an L4 load balancer whose backends change** (for example a Kubernetes `ClusterIP` service
  with autoscaling or rolling deployments): keep it enabled, so new pods start receiving invocations.
- **Not behind an L4 load balancer** (direct endpoints, an HTTP/2-aware L7 proxy, or a fixed set of
  backends): you can set it to `0`. Replacing connections gains you nothing here and only adds
  reconnects.

### Choosing a Value
A retired connection takes no new invocations, but it stays open until its longest in-flight
request to your deployment finishes. While that happens, its replacement is already serving new
invocations, so for a while both are open. What matters here is how long a single request to your
deployment stays open, that is how long your handlers run before they complete or suspend. It is
not the end-to-end duration of an invocation: an invocation that sleeps or waits on other calls
suspends and releases its request, even if the invocation itself lasts hours or days.

The longer your slowest requests stay open compared with the max age, the more connections are
open to your deployment at once:

| Max age compared with your slowest usual requests | Extra open connections (roughly) |
|---------------------------------------------------|----------------------------------|
| 10x longer                                        | about 10% more                   |
| about the same                                    | about twice as many              |
| much shorter                                      | grows with the gap               |

We recommend choosing a max age of at least 10x the longest time a single request to your
deployment usually stays open. The 5 minute default suits requests that stay open for up to about
30 seconds; raise it if yours commonly stay open longer. A shorter max age spreads load onto new
pods faster, at the cost of more open connections.

### Migration Guidance
None required for most deployments. To raise the max age, for example if your requests stay open
for several minutes:

```toml
[worker.invoker]
http2-max-connection-age = "30m"
# optional, default 0.1 (up to +10%)
http2-max-connection-age-jitter = 0.1
```

To disable it and keep connections open until they are idle or closed by the deployment:

```toml
[worker.invoker]
http2-max-connection-age = "0"
```

### Related Issues
- Issue #4579: Follow up on connection pool and connection eviction
- PR #5644: connection-pool: retire connections past max_connection_age
