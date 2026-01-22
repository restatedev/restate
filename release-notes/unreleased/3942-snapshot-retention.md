# Release Notes for PR #3942: Integrated snapshot pruning

## New Feature (Experimental)

### What Changed

Restate can now be configured to retain a fixed number of recent snapshots and automatically delete older ones. Previously, snapshot cleanup required external lifecycle policies or manual intervention.

New configuration option:

```toml
[worker.snapshots]
experimental-num-retained = 10  # retain the 10 most recent snapshots
```

### Why This Matters

This feature:

- Eliminates the need for external cleanup scripts or object store lifecycle policies
- Ensures all retained snapshots are usable for partition recovery

### Behavioral Change: Archived LSN semantics

**Important**: When `experimental-num-retained` is configured, the reported **Archived LSN** changes meaning:

| Configuration                     | Archived LSN means                      |
| --------------------------------- | --------------------------------------- |
| No retention configured (default) | LSN of the latest snapshot              |
| `experimental-num-retained = N`   | LSN of the **oldest** retained snapshot |

This change ensures that any retained snapshot can be used for recovery. This it also means that:

- The log trim point will be further from the tail, retaining more records
- Log storage usage on log-server nodes will increase proportionally to the number of retained snapshots and activity in the cluster / snapshot interval

### Impact on Users

- **Existing deployments**: No change unless you enable `experimental-num-retained`
- **New deployments**: Opt-in feature, disabled by default
- **Enabling retention**: Updates the partition `latest.json` metadata format from V1 to V2

### Migration Guidance

- **Enabling**: Simply add `experimental-num-retained` to your configuration
- **Downgrading**: Not recommended once V2 format is written; older Restate versions will not recognize the retained snapshot tracking and will revert to V1 behavior
- **Disk planning**: Account for additional log retention when setting `num-retained` higher than 1

### Related Issues

- [#3942](https://github.com/restatedev/restate/pull/3942): Add support for managing a fixed number of retained snapshots
