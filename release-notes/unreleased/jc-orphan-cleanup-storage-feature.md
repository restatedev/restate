# Release Notes: Startup cleanup of orphaned journal completion-id indexes

## Behavioral Change

### What Changed

A one-time local storage migration removes orphaned journal completion-id index (`jc`)
entries left behind by earlier versions. It runs when a partition starts and must finish
before the partition begins processing.

Before v1.9.0, opt in on each node with this top-level configuration option:

```toml
experimental-enable-jc-orphan-cleanup = true
```

From v1.9.0, the cleanup is enabled unconditionally. Each partition store runs it only once.
If it is interrupted, it resumes at the next partition startup.

### Impact on Users

- The first partition startup after enabling the cleanup can take longer, depending on the
  index size. Deleting a large backlog can also increase disk I/O and compaction work.
- A partition store that completed the cleanup requires Restate v1.7.10 or newer.

### Migration Guidance

Enable the option gradually after upgrading the whole cluster to a version that offers it.
Restart one node at a time and wait for its partitions to finish startup and catch up before
proceeding to the next node. Allow for longer recovery when setting rollout timeouts.

See the [rolling-update guide](https://docs.restate.dev/server/upgrading#rolling-updates)
for the readiness queries and prerequisites for maintaining cluster availability.
