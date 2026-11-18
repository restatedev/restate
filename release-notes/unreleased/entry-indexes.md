# Release Notes: Entry indexes (experimental)

## New Feature

### What Changed

Partitions can now maintain three indexes of their invocations and state mutations:

- all of them by service and handler, newest created first;
- the finished ones by outcome, service and handler, newest finished first;
- the active ones, in the inbox, running, suspended or paused, by stage, service and handler, newest
  created first.

With query engine V2, a query on `sys_invocation_status` that names
its services and asks for their newest invocations under a `LIMIT`, like the UI's page of a
workflow's invocations, reads only those invocations through the first index instead of every
invocation of every partition. On `sys_vqueue_entry_status`, a query for the newest finished
entries by when they finished, or for the newest active entries by when they were created, under a
`LIMIT`, reads the other two. Other queries, and query engine V1, read as before.

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
  - one by service, written when it is created and removed when it is deleted;
  - one by stage while it is active, moved each time it changes stage, and removed when it
    finishes;
  - one by outcome once it has finished, removed when it is deleted.

  Other status changes don't write to the indexes.
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
