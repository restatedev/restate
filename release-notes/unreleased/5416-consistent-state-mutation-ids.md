# Release Notes for Issue #5416: State changes via the Admin API applied on all replicas

## Bug Fix

### What Changed

With vqueues enabled, changing the state of a Virtual Object through the Admin API
(`POST /services/{service}/state`, e.g. `restate state edit|patch|clear`) could leave replicas of a
partition with different state: the change was applied on the partition leader but might be
skipped by the followers. State changes are now applied on all replicas. This also covers changes
that were submitted before the upgrade but have not been executed yet.

### Why This Matters

Followers take over when the leader changes. A follower that missed a state change continues with
the old state of the affected Virtual Object.

### Impact on Users

- Only partitions with more than one replica on which the state API was used while vqueues were
  enabled are affected. Clusters with a replication factor of 1 are not affected.
- A state change that a follower missed before the upgrade may still be executed by that replica
  once it becomes leader. State changes with a version condition (the CLI default unless `--force`
  is used) have no effect if the state of the Virtual Object has changed in the meantime.

### Migration Guidance

If you used the state API on replicated partitions with vqueues enabled, re-submit the state of the
affected Virtual Objects after upgrading (e.g. with `restate state edit`) to bring all replicas back
in line.

#### Optional: remove pending state changes from before the upgrade

A new experimental option removes all state changes that are still waiting to be executed, so that
state changes missed by a follower before the upgrade can no longer be executed later:

```toml
experimental-enable-inconsistent-state-mutation-removal = true
```

Or via the environment variable `RESTATE_EXPERIMENTAL_ENABLE_INCONSISTENT_STATE_MUTATION_REMOVAL=true`.

- The cleanup runs once per partition when its next leader is elected. Every removed state change
  is logged as a warning together with the affected Virtual Object; re-submit it if it should still
  be applied.
- Only enable it once all nodes run v1.8.0 or newer. Once a partition has been cleaned up, it
  can't be rolled back to a version older than v1.8.0.

### Related Issues

- Issue #5416: StateMutation EntryId is not consistent across replicas
