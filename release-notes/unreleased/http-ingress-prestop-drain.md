# Graceful HTTP ingress drain control

## New Feature

### What Changed

`restatectl --single-address http://127.0.0.1:5122 nodes drain-http-ingress --current --json` can
now place the local running node's HTTP ingress role into an irreversible drain for the lifetime
of that process. `--current` resolves the local node's exact generation before draining, while
`--node <generational-node-id>` remains available for an explicitly targeted node. Draining closes
listeners, sends graceful shutdown notifications to existing connections, and reports the
outstanding request and connection counts. Existing requests remain active until normal node
shutdown applies its bounded final deadline.

### Why This Matters

Operators can stop new ingress traffic early in a planned node termination and wait for the HTTP
role to become quiescent before proceeding with the rest of the shutdown sequence.

### Impact on Users

There is no change to normal ingress behavior. A drained ingress role becomes active again only
after the node process restarts with a new generation.
