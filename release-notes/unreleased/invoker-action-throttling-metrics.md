# Release Notes: Metrics for invoker action throttling

## New Feature

### What Changed
When `worker.invoker.action-throttling` is configured, Restate now reports how often it delays
journal actions to stay within the configured rate:

- `restate_invoker_action_throttled_total`: number of actions that had to wait for the action
  throttling limit.
- `restate_invoker_action_throttling_wait_ms_total`: cumulative milliseconds actions spent
  waiting for the action throttling limit.

### Impact on Users
- Operators can now see whether, and how much, a node is throttling actions, for example to
  size the throttling limit or to correlate slow invocations with throttling.
- Nodes without action throttling configured report neither metric.

### Migration Guidance
None.
