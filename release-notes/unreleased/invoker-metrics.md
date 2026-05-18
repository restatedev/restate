# Invoker metrics

## Dropped metrics
The following metric has been dropped:
- `restate.invoker.available_slots`

## Added metrics
Two new counter metrics have been added as replacements:
- `restate.invoker.concurrency_slots.acquired` (counter) - cumulative per node
- `restate.invoker.concurrency_slots.released` (counter) - cumulative per node

These counters make it easy to derive:
- Rate of slot acquisition and release
- Node-level available slots, grouping by the node label exposed by your Prometheus setup, for example `node_name` with the built-in exporter:
  ```promql
  sum by (node_name) (restate_invoker_concurrency_limit)
    - (
      sum by (node_name) (restate_invoker_concurrency_slots_acquired_total)
      - sum by (node_name) (restate_invoker_concurrency_slots_released_total)
    )
  ```
  If your Prometheus setup exposes a `node_id` label, use `sum by (node_id)` instead.
  This aggregation also removes any remaining `invoker_id` label from `restate.invoker.concurrency_limit` in configurations where it is still present.

## Future breaking observability change
In Restate v1.8.0, the `invoker_id` label will be removed from `restate.invoker.concurrency_limit`, so this metric will always be reported at node scope. Update dashboards, alerts, and recording rules to stop grouping or filtering `restate.invoker.concurrency_limit` by `invoker_id`; group by the node label instead.
