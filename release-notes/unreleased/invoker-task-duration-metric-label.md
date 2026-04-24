# [Minor] Invoker task duration metric label change

## Behavioral change

### What changed
`restate.invoker.task_duration.seconds` is no longer emitted with a `partition_id` label.

### Impact on users
- Custom dashboards and alerts filtering this histogram by `partition_id` should be updated.

### Migration guidance
Remove `partition_id` selectors from `restate.invoker.task_duration.seconds` queries.
