# Cluster availability metrics and `restatectl nodes check-safety`

## New Feature

### What Changed

The cluster controller now checks every minute whether all logs can still be
read and written, and whether all partitions still have a node to run on, given
which nodes are alive. It also checks which logs and partitions would lose
availability if one more failure domain failed. The results are exported as
metrics:

| Metric | Meaning |
|---|---|
| `restate_cluster_safety_write_unavailable_logs` | Logs that can't accept writes |
| `restate_cluster_safety_read_unavailable_logs` | Logs that can't be reliably read |
| `restate_cluster_safety_unavailable_partitions` | Partitions with no alive replica that can't be moved to other nodes |
| `restate_cluster_safety_write_at_risk_logs` | Logs that would stop accepting writes if one failure domain failed |
| `restate_cluster_safety_read_at_risk_logs` | Logs that could no longer be read if one failure domain failed |
| `restate_cluster_safety_at_risk_partitions` | Partitions that would become unavailable if one failure domain failed |
| `restate_cluster_safety_unchecked_logs` | Logs that can't be checked because they don't use the replicated loglet |
| `restate_cluster_safety_unchecked_partitions` | Partitions whose placement isn't known yet |
| `restate_cluster_safety_last_check_timestamp_seconds` | Unix timestamp of the last completed check |

To check whether it's safe to stop nodes, run:

```sh
restatectl nodes check-safety --assume-dead N1,N2
```

It lists the logs and partitions that would be unavailable or at risk, and the
failure domains that the cluster can't afford to lose. Without
`--assume-dead`, it checks the current state of the cluster. It exits with an
error when logs or partitions would be unavailable. With `--preview`, it shows a
verdict (safe, at risk or unsafe), each failure domain with the state of its
nodes and what losing it would make unavailable, and supports `--json`.

### Impact on Users

Alert on the `*_unavailable_*` metrics being above 0 to catch logs and partitions
that lost availability. An `*_at_risk_*` value above 0 means the cluster has no
headroom: losing one more failure domain takes these logs or partitions
offline. Failure domains follow your replication setting: with `{zone: 2}`
replication they are zones, with `{node: 2}` they are individual nodes (for
example, a node being restarted).

Partitions whose placement is frozen can't be moved to other nodes, so they
become unavailable once all of their replicas are down. Expect
`restate_cluster_safety_at_risk_partitions` to go up while placement is frozen.

Only the node currently acting as cluster controller leader reports these
metrics. Other nodes either don't report them or report 0, so use `max` across
nodes when building alerts and dashboards. The check runs every minute, so alert
if `time() - max(restate_cluster_safety_last_check_timestamp_seconds)` exceeds a
few minutes: the other values are stale, for example because no node is acting
as leader.
