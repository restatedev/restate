# Release Notes for PR #4917: Restore an earlier snapshot if the latest one fails

## Behavioral Change

### What Changed

When a partition bootstraps from the snapshot repository and the latest snapshot is unusable —
missing or invalid metadata, missing or truncated data files, or corrupt data files, which are now
checksum-verified in full after the import and before the partition starts — Restate now falls back
to the next older retained snapshot instead of failing the restore outright. Candidates are tried in descending LSN order
until one succeeds or all are exhausted.

Errors that are not about the snapshot itself, such as an unreachable repository, rejected
credentials or a local disk error, do not trigger a fallback: the partition fails to open and
retries the latest snapshot later.

Two metrics were added so operators can see when this happens:

| Metric | Meaning |
|---|---|
| `restate.partition_store.snapshots.download.fallback.total` | A partition started from a non-latest snapshot |
| `restate.partition_store.snapshots.fast_forward.failed.total` | A partition had to fast-forward past a trim gap, but no retained snapshot is recent enough |

If every candidate fails, or the repository cannot be read, the partition fails to open and is
retried; Restate never starts a partition from an empty store while a snapshot may exist.

### Why This Matters

A single damaged or partially uploaded snapshot previously blocked a partition from starting even
when perfectly good older snapshots were still retained. Where `num-retained` is
set above its default of 1, older snapshots are kept but were never used for recovery.

Falling back is safe with respect to log trimming: the trim point is driven by the *earliest*
retained snapshot's LSN, so every candidate advertised in the repository is at or after the trim
point and the remaining log is available to replay the gap.

### Impact on Users

- Existing deployments: no configuration change required; the fallback is always enabled, but
  it only has something to fall back to if `num-retained` is greater than 1.
- Restores that would previously have failed may now succeed from a slightly older snapshot, at
  the cost of replaying more of the log.
- A non-zero `snapshots.download.fallback.total` is a signal to investigate the repository — the
  restore succeeded, but the latest snapshot is missing files, has invalid metadata, or contains
  corrupt data.

### Migration Guidance

None. Consider alerting on `restate.partition_store.snapshots.download.fallback.total` and
`restate.partition_store.snapshots.fast_forward.failed.total`, since both indicate a snapshot
repository that needs attention.

### Related Issues

- PR #4917: Enable restoring an earlier snapshot if latest fails
