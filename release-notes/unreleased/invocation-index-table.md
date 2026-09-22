# Release Notes: Invocation secondary-index inspection table

## New Feature

### What Changed

The experimental SQL table `_idx_invocation_by_service` exposes persisted invocation
secondary-index entries. Its columns are `partition_id`, `service_name`, `stage`,
`transitioned_at`, `transitioned_at_hlc`, `invocation_id`, and `partition_key`.

Service, VQueue-stage, and invocation-ID predicates can filter native index scans.
Other predicates, including timestamp comparisons, are evaluated on the returned rows.
`transitioned_at` has millisecond precision; `transitioned_at_hlc` includes the internal
logical counter for ordering transitions within the same millisecond. Use `ORDER BY`
for ordered SQL results.

```sql
SELECT invocation_id, transitioned_at
FROM _idx_invocation_by_service
WHERE service_name = 'MyService' AND stage = 'inbox'
ORDER BY transitioned_at_hlc DESC
LIMIT 20;
```

### Why This Matters

The table allows inspection of the service/stage index without scanning invocation
primary records. It exposes retained VQueue invocations, including finished entries
until deletion, and excludes state mutations. `stage` is the VQueue stage rather than
the invocation status shown by `sys_invocation`.

### Migration Guidance

Index maintenance uses the existing experimental setting:

```toml
[common]
experimental-enable-indexes-v1 = true
```

Existing entries are not backfilled when index maintenance is enabled. This table
reports index contents, not an authoritative inventory of all invocations; it may be
empty or incomplete on stores where index maintenance was not enabled before entries
were created. No storage-format migration is introduced by this table.
