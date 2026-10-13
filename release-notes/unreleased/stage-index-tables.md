# Secondary-index inspection tables

## New Feature

### What Changed

Experimental SQL tables expose each persisted VQueue secondary index:

| Table | Indexed dimensions |
| --- | --- |
| `_idx_entry_by_service` | `stage`, `service_name`, `transitioned_at` |
| `_idx_entry_by_stage` | `stage`, `transitioned_at` |
| `_idx_entry_next_at_by_stage` | `stage`, `next_at`, `seq` |
| `_idx_entry_next_at_by_service` | `stage`, `service_name`, `next_at`, `seq` |
| `_idx_entry_by_virtual_object` | `service_name`, `scope`, `key`, `stage`, `transitioned_at` |
| `_idx_entry_next_at_by_virtual_object` | `service_name`, `scope`, `key`, `stage`, `next_at`, `seq` |
| `_idx_busy_vqueue` | `total_non_completed`, `last_modified`, `scope`, `vqueue_id` |

All entry tables also expose `canonical_id`, `entry_id`, and `partition_key`.
They include invocations and state mutations; virtual-object tables exclude unkeyed
services and workflows. `canonical_id` identifies the entry incarnation; `entry_id`
is its resource ID without the sequence suffix. The tables do not expose physical
`partition_id` or entry `status`, which is no longer stored in these index keys.

The busy-queue table additionally exposes `partition_key` and covering counters:
`num_inbox`, `num_running`, `num_suspended`, `num_paused`, and `num_finished`.
Absent stages have zero counts. Queues with no non-completed entries remain visible
until their metadata is deleted. No queue-metadata lookup is needed.

Indexed dimensions support native filtering. Timestamp predicates use millisecond
precision; `next_at` retains its stored second precision without rounding
fractional-second equality predicates onto stored values. ID equality and IN-list
predicates are pushed down; ordered ID-string comparisons remain residual.
Counter predicates on covering values remain residual. Key values are decoded
lazily according to the projected columns.

```sql
SELECT canonical_id, entry_id, next_at, seq
FROM _idx_entry_next_at_by_stage
WHERE stage = 'inbox'
ORDER BY next_at ASC
LIMIT 20;
```

### Why This Matters

These tables allow inspection of stage transitions, scheduled transition times,
individual virtual objects, and queue load without reading primary entry records.

### Impact on Users

Index maintenance uses the existing `experimental-enable-indexes-v1` setting. These tables report persisted index contents, including finished entries until deletion. Existing entries are not backfilled when index maintenance is enabled, so the tables are not an authoritative inventory of all entries. Use `ORDER BY` for ordered SQL results across partitions.
