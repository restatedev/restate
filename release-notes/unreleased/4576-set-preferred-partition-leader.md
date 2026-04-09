# Release Notes for Issue #4576: Leadership policy for partition processors

## New Feature

### What Changed
Users can now control leader election for partition processors via `restatectl partition leader`.
The leadership policy supports two independent controls:

- **Pin to node**: pin a partition's leader to a specific node
- **Election freeze**: pause leader election with a reason; the current leader stays, and no
  new leader is elected even if the current one dies

The policy is persisted in the metadata store and survives cluster controller restarts.

### Usage
```bash
# Pin leader to a specific node
restatectl partition leader pin 0-4 --node 2

# Unpin leader (return to automatic selection)
restatectl partition leader unpin 0-4

# Freeze leader election
restatectl partition leader freeze 0 --reason "Maintenance window"

# Unfreeze leader election
restatectl partition leader unfreeze 0

# Show current leadership policy
restatectl partition leader show 0-4
```

### Why This Matters
This enables operators to:
- Pin leaders to a specific node for predictable placement
- Freeze leader election during sensitive operations (e.g. maintenance windows)

### Impact on Users
- No action required for existing clusters — the default behavior (automatic leader selection)
  is unchanged.
- Pinning is a soft preference: if the pinned node is unavailable, the scheduler falls
  back to automatic selection.
- Election freeze is a hard stop: even if the leader dies, no new leader will be elected until
  the freeze is lifted.

### Related Issues
- Issue #4576
