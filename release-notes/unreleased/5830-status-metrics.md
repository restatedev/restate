# Release Notes for Issue #5830: Node and role status metrics

## New Feature

### What Changed
Restate now exports the status of the node and of each role as a gauge. Each gauge has one series for each status, given by the `status` label. The series for the current status has the value 1. All other series have the value 0.

| Gauge | `status` values |
|---|---|
| `restate_node_status` | `unknown`, `starting_up`, `alive`, `shutting_down` |
| `restate_worker_status` | `unknown`, `starting_up`, `ready` |
| `restate_admin_status` | `unknown`, `starting_up`, `ready` |
| `restate_log_server_status` | `unknown`, `starting_up`, `ready`, `failsafe`, `stopping` |
| `restate_metadata_server_status` | `unknown`, `starting_up`, `awaiting_provisioning`, `member`, `standby` |
| `restate_ingress_status` | `unknown`, `starting_up`, `ready` |

A node reports `restate_node_status` and the gauges of the roles that it runs.

### Why This Matters
Before this change, you could see these statuses only in the server log and in the `GET :5122/health` answer. Now you can alert on them.

The most important case is a log-server in `failsafe`. It stops accepting writes until an operator fixes the cause and restarts the node. To alert on it, use `restate_log_server_status{status="failsafe"} == 1`.

### Impact on Users
- No configuration is needed.
- Do not use the `failsafe` state as a Kubernetes liveness probe. The usual causes (a full disk, a read-only filesystem, corruption) stay after a restart. Automatic restarts can cause a crash loop that also stops the other roles of the node.

### Migration Guidance
None.

### Related Issues
- Issue #5830: Expose log-server status (failsafe) as a metric
