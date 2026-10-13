# Release Notes: Session-time SQL table visibility

## Behavioral Change

### What Changed

New query sessions expose `sys_service_stats`, `sys_deployment_stats`, and
`sys_virtual_object_stats` only when `common.experimental-enable-track-status-gauges`
is enabled. This applies to both query engines.

The secondary-index tables in query engine v1 are exposed only when
`common.experimental-enable-indexes-v1` is enabled.

The `restate.cluster.config` table is exposed only when
`common.disable-config-sql-table` is false. This is now decided when a session is
created, so enabling the table no longer requires restarting a node that started
with it disabled.

### Impact on Users

Disabled tables are absent from the session catalog, including table listings,
and queries referring to them fail with a missing-table error. Configuration
changes affect new sessions; existing sessions retain their original catalogs.

### Migration Guidance

Enable the corresponding option before creating a query session that needs these
tables. The existing limitations on statistics collection and index backfill still
apply.
