# Vqueue entries show what they are doing now

## Behavioral Change

### What Changed

The `status` column of `sys_vqueues` and `sys_vqueue_entry_status` now shows what the entry is
doing now. Until now it showed the status the entry had stored, which an entry keeps while it runs,
is suspended or is paused. So a paused entry could show `backing-off`, a running one `started`, and
the status alone didn't tell the stage.

| `stage`     | `status` until now                        | `status` now                  |
|-------------|-------------------------------------------|-------------------------------|
| `inbox`     | `new`, `scheduled`, `backing-off`, `yielded` | unchanged                  |
| `inbox`     | `started`                                 | `ready`                       |
| `running`   | any                                       | `running`                     |
| `suspended` | any                                       | `suspended`                   |
| `paused`    | any                                       | `paused`                      |
| `finished`  | `succeeded`, `failed`, `killed`, `cancelled` | unchanged                  |

Every status now belongs to one stage: `status = 'backing-off'` returns only entries waiting in the
inbox to retry, not paused or running ones that once backed off. `started` is no longer shown.

These tables and the stats tables no longer show `unknown`, and `'unknown'` matches nothing, like
any other name these columns don't have. Where the server can't recognize a stored stage, status or
entry kind, they show NULL instead; running, suspended and paused entries still show their stage's
status.

### Impact on Users

- Queries that filter on `stage = 'inbox'` or `stage = 'finished'` together with a status, such as
  `stage = 'inbox' AND status = 'backing-off'`, return the same entries as before.
- For running, suspended and paused entries, `status` is now the stage's name: a query such as
  `stage = 'paused' AND status = 'backing-off'` now returns nothing.
- Queries that ask for `started` return nothing: ask for `ready` in the inbox, or for the stage,
  instead.
- Queries that filter on `status` without `stage` now return only entries in that status's stage.
- Tools that read these tables need to accept the new values `ready`, `running`, `suspended` and
  `paused`, and a NULL stage, status or entry kind.
- During a rolling upgrade, entries of partitions on nodes still running an older version show the
  statuses they stored until those nodes are upgraded.

### Migration Guidance

Replace conditions on `started`:

```sql
-- Before
SELECT * FROM sys_vqueues WHERE stage = 'inbox' AND status = 'started';
-- Now
SELECT * FROM sys_vqueues WHERE status = 'ready';
```

To find running, suspended or paused entries, filter on either `stage` or `status`: both now say
the same.
