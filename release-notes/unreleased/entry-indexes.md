# Release Notes: Entry indexes (experimental)

## New Feature

### What Changed

Partitions can now maintain an index of their invocations and state mutations by service and
handler, newest created first. With query engine V2, a query on `sys_invocation_status` that names
its services and asks for their newest invocations under a `LIMIT`, like the UI's page of a
workflow's invocations, reads only those invocations through the index instead of every invocation
of every partition. Other queries, and query engine V1, read as before.

Two new experimental options, both off by default, control it:

- `experimental-enable-track-entry-indexes` turns the index on. Each partition then indexes what is
  written from that point on, and builds the index for its existing entries in the background
  (see the background migration options under `[worker.storage]`).
- `experimental-enable-entry-indexes-reset` deletes the index on each partition when it starts, and
  stops maintaining it. While it is on, it takes precedence over the first option.

Both take effect when partition processors start.

### Impact on Users

- Nothing changes unless the index is turned on.
- A partition reads through the index only once it has fully migrated to vqueues and its index is
  complete; until then it reads as before. While some worker node still runs an older version, for
  example during a rolling upgrade, queries read as before too.
- With the index on, every invocation and state mutation also writes one small index key when it is
  created and removes it when it is deleted. Status changes don't write to the index.
- Once a partition has maintained the index, it keeps doing so after restarts, even if
  `experimental-enable-track-entry-indexes` is removed. Only the reset option stops it.
- Turning the index on raises the partition's minimum Restate version to v1.8.2. That stays after the
  option is removed and after a reset: a node that has run with it cannot be rolled back to an older
  version.

### Migration Guidance

No action is needed. To try the index, enable it and restart the nodes:

```toml
experimental-enable-track-entry-indexes = true
```

To remove it, replace that line with the reset option and restart the nodes:

```toml
experimental-enable-entry-indexes-reset = true
```

To build it again later, remove the reset option, enable tracking again, and restart the nodes.
