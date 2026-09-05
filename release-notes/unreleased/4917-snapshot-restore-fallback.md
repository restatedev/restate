# Release Notes for PR #4917: Restore an earlier snapshot if the latest one fails

## Behavioral Change

### What Changed

When a partition bootstraps from the snapshot repository and the latest snapshot cannot be
restored — network error, corrupt or unreadable metadata, missing SST files — Restate now falls
back to the next older retained snapshot instead of failing the restore outright. Candidates are
tried in descending LSN order until one succeeds or all are exhausted.

Two metrics were added so operators can see when this happens:

| Metric | Meaning |
|---|---|
| `restate.partition_store.snapshots.download.fallback.total` | A partition started from a non-latest snapshot |
| `restate.partition_store.snapshots.fast_forward.failed.total` | No retained snapshot could satisfy a fast-forward from a trim gap |

A repository error that leaves the latest snapshot indeterminate no longer results in provisioning
an empty partition store. Previously an inconclusive repository lookup could be treated as "no
snapshot exists", which risked starting a partition from scratch on a transient error.

### Why This Matters

A single damaged or partially uploaded snapshot previously blocked a partition from starting even
when perfectly good older snapshots were still retained. Because `experimental-num-retained`
already keeps several snapshots, the recovery data was usually present — it just was not being
used.

Falling back is safe with respect to log trimming: the trim point is driven by the *earliest*
retained snapshot's LSN, so every candidate advertised in the repository is at or after the trim
point and the remaining log is available to replay the gap.

### Impact on Users

- Existing deployments: no configuration change required; the fallback is always enabled.
- Restores that would previously have failed may now succeed from a slightly older snapshot, at
  the cost of replaying more of the log.
- A non-zero `snapshots.download.fallback.total` is a signal to investigate the repository — the
  restore succeeded, but the latest snapshot is damaged and should be looked at.

### Migration Guidance

None. Consider alerting on `restate.partition_store.snapshots.download.fallback.total` and
`restate.partition_store.snapshots.fast_forward.failed.total`, since both indicate a snapshot
repository that needs attention.

### Related Issues

- PR #4917: Enable restoring an earlier snapshot if latest fails
