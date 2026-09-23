# Release Notes: Entry secondary-index inspection table

## New Feature

### What Changed

The experimental SQL table `_idx_entry_by_service` exposes persisted entry
secondary-index entries. Its columns are `partition_id`, `service_name`, `stage`,
`transitioned_at`, `canonical_id`, `entry_id`, and `partition_key`.

Service, VQueue-stage, canonical-ID, and millisecond timestamp predicates can filter
native index scans. `transitioned_at` uses Unix timestamps at millisecond precision;
timestamp predicates are translated to the corresponding internal clock ranges.
Other predicates are evaluated on the returned rows. Use `ORDER BY` for ordered SQL
results; transitions within the same millisecond have equal timestamps.

```sql
SELECT canonical_id, entry_id, transitioned_at
FROM _idx_entry_by_service
WHERE service_name = 'MyService' AND stage = 'inbox'
ORDER BY transitioned_at DESC
LIMIT 20;
```

### Why This Matters

The table allows inspection of the service/stage index without scanning entry
primary records. It exposes VQueue invocations and state mutations, including finished entries
until deletion. `stage` is the VQueue stage rather than
the invocation status shown by `sys_invocation`.

### Migration Guidance

Index maintenance uses the existing experimental setting:

```toml
[common]
experimental-enable-indexes-v1 = true
```

Existing entries are not backfilled when index maintenance is enabled. This table
reports index contents, not an authoritative inventory of all entries; it may be
empty or incomplete on stores where index maintenance was not enabled before entries
were created. No storage-format migration is introduced by this table.
