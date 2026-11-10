# Release Notes: Invocation priorities

## New Feature

### What Changed
Invocations can now carry a priority: `low`, `normal` (the default), `high` or `live`. When invocations compete for the same capacity, such as a concurrency limit or the invoker capacity of a node, Restate runs the invocations with the highest priority first. Invocations with the same priority still run in the order they arrived.

Set the priority of a request with the `x-restate-priority` header or the `priority` query parameter. It works for scoped and unscoped calls and sends:

```shell
curl localhost:8080/restate/call/Checkout/process \
  -H 'x-restate-priority: live' \
  --json '{"orderId": "order-123"}'

curl "localhost:8080/restate/send/Reports/generate?priority=low" \
  --json '{"month": "2026-10"}'
```

Records sent through the ingestion API accept the same values in their `priority` field, either per record or in the stream defaults.

Calls and sends made from a handler inherit the priority of the invocation that makes them, so the work a `live` request triggers is also `live`.

### Why This Matters
Latency-sensitive work, such as requests a user is waiting on, no longer queues behind background work when capacity is exhausted, and background work can be marked `low` so it only uses capacity that nothing more urgent needs.

### Impact on Users
- Priorities are off by default. Until you enable them, the priority of a request is accepted and ignored.
- A priority never lets an invocation exceed a concurrency limit, it only decides which invocation runs next.
- Lower priority invocations wait as long as higher priority invocations keep the capacity they need busy.
- Calls to the same Virtual Object with different priorities don't run in arrival order. Calls with the same limit key and the same priority still do.
- The `sys_invocation_status`, `sys_invocation` and `sys_vqueue_meta` tables have a new `priority` column, and `restate vqueues list` and `restate vqueues describe` show it.
- `restate invocations cancel` and `restate invocations kill` accept `--priority` to act only on the invocations with that priority, for example to drop a low-priority backlog:

  ```shell
  restate invocations cancel Reports --priority low --limit 100000
  ```

### Migration Guidance
Upgrade all nodes to v1.8.2 or newer, then enable the option:

```toml
experimental-enable-invocation-priorities = true
```

- Enabling it is one-way: removing the option later doesn't turn priorities off again.
- Only invocations created after the option takes effect use their priority.
- Once enabled, you **cannot** roll back to a Restate-server version older than v1.8.2.
