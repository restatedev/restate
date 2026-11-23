# Release Notes: Entry indexes (experimental)

## New Feature

### What Changed

Partitions can now maintain these indexes of their invocations and state mutations:

- all of them by the hour they were created in and by service, newest created first;
- all of them in a known stage by stage and status, then newest created first, and again by stage
  and status, then newest by when they entered their stage.

With query engine V2, a query on `sys_invocation_status` that asks for the newest or oldest
invocations under a `LIMIT`, of every service or of the ones it names, like the UI's invocation
list or its page of a workflow's invocations, reads only those invocations through the first index
instead of every invocation of every partition. On `sys_vqueue_entry_status`, a query for the
newest or oldest entries of some stages under a `LIMIT` reads the stage-and-status indexes: by when
they were created, active ones included, or by when they entered their stage, such as the newest
finished entries. Other queries, and query engine V1, read as before.

Two new experimental options, both off by default, control it:

- `experimental-enable-track-entry-indexes` turns the indexes on. Each partition then indexes what
  is written from that point on, and builds the indexes for its existing entries in the background
  (see the background migration options under `[worker.storage]`).
- `experimental-enable-entry-indexes-reset` deletes the indexes on each partition when it starts,
  and stops maintaining them. While it is on, it takes precedence over the first option.

Both take effect when partition processors start.

### Impact on Users

- Nothing changes unless the indexes are turned on.
- A partition reads through an index only once it has fully migrated to vqueues and that index is
  complete; until then it reads as before. While some worker node still runs an older version, for
  example during a rolling upgrade, queries read as before too.
- With the indexes on, every invocation and state mutation also writes small index keys:
  - one by creation hour and service, written when it is created and removed when it is deleted;
  - two by stage and status, each moved every time it changes stage or status, and removed when it
    is deleted.
- Once a partition has maintained the indexes, it keeps doing so after restarts, even if
  `experimental-enable-track-entry-indexes` is removed. Only the reset option stops it.
- Turning the indexes on raises the partition's minimum Restate version to v1.8.2. That stays after the
  option is removed and after a reset: a node that has run with it cannot be rolled back to an older
  version.

### Migration Guidance

No action is needed. To try the indexes, enable them and restart the nodes:

```toml
experimental-enable-track-entry-indexes = true
```

To remove them, replace that line with the reset option and restart the nodes:

```toml
experimental-enable-entry-indexes-reset = true
```

To build them again later, remove the reset option, enable tracking again, and restart the nodes.
