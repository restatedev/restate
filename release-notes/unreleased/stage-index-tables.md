# Stage-based entry index inspection tables

## New Feature

### What Changed

Two experimental SQL tables expose the persisted stage-based VQueue entry indexes:

- `_idx_entry_by_stage`: `partition_id`, `stage`, `transitioned_at`, `status`, `canonical_id`, `entry_id`, and `partition_key`.
- `_idx_entry_next_at_by_stage`: `partition_id`, `stage`, `next_at`, `status`, `canonical_id`, `entry_id`, and `partition_key`.

Both include invocations and state mutations. `canonical_id` identifies the entry incarnation; `entry_id` is its resource ID without the sequence suffix.

Stage, status, canonical-ID, and timestamp predicates support native index filtering. `transitioned_at` is exposed at millisecond precision. `next_at` is stored with second precision and exposed as a millisecond timestamp; comparisons preserve that precision without rounding fractional-second equality predicates onto stored values. Both tables report storage-scan metrics through `EXPLAIN ANALYZE`.

```sql
SELECT canonical_id, entry_id, next_at, status
FROM _idx_entry_next_at_by_stage
WHERE stage = 'inbox' AND status = 'scheduled'
ORDER BY next_at ASC
LIMIT 20;
```

### Why This Matters

These tables allow inspection of stage transitions and scheduled transition times across services, without reading primary entry records.

### Impact on Users

Index maintenance uses the existing `experimental-enable-indexes-v1` setting. These tables report persisted index contents, including finished entries until deletion. Existing entries are not backfilled when index maintenance is enabled, so the tables are not an authoritative inventory of all entries. Use `ORDER BY` for ordered SQL results across partitions.
