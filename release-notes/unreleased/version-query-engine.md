# Query engine information in `/version`

## New Feature

### What Changed

The admin API's `/version` response has a new `query_engine` object:

```json
"query_engine": {
  "default_engine": "v1",
  "stats_tables": true
}
```

- `default_engine` is the engine that runs a query unless the request selects
  another: `v1` or `v2`.
- `stats_tables` says whether the statistics tables (`sys_service_stats`,
  `sys_deployment_stats` and `sys_virtual_object_stats`) answer queries:
  - `false` while the tables aren't shown: they appear only with
    `experimental-enable-indexes` on the nodes that run queries.
  - Otherwise `true`, also right after the admin node starts, until a check
    finds a partition whose statistics read would fail: its leader, which the
    read goes to, answered, and one of its status gauges isn't complete. It
    turns `true` again once no such partition is left.
  - Partitions without a known leader, and leaders that are down, don't answer,
    or are still opening the partition, don't count either way. So while a
    cluster starts, `stats_tables` doesn't flip to `false` just because
    partitions aren't open yet.

The admin node checks again every minute. The new option
`admin.query-engine.index-catalog-refresh-interval` changes that interval. If a
check fails as a whole, or doesn't finish within that interval, `stats_tables`
keeps its last value and the check is retried sooner.

### Why This Matters

The Web UI and other clients can read `query_engine.stats_tables` to decide
whether to show the statistics, instead of guessing from experimental feature
flags. Queries of the statistics tables fail until the statistics are complete,
so clients that check first avoid those errors.

### Impact on Users

- Nothing to do: the field is added to the existing response.
- Clients must treat a missing `query_engine` as "unknown": servers older than
  v1.8.2 don't send it.
