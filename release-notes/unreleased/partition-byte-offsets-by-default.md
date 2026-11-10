# Automatic partition byte-offset tracking from v1.9.0

## Behavioral Change

### What Changed

Partition leaders running v1.9.0 or newer turn byte-offset tracking on automatically once every
worker node in the cluster runs v1.9.0 or newer. Each partition turns it on the next time it elects
a leader after that, for example when its leader's node restarts. Disabled worker nodes don't
count. In v1.8.2, it remains opt-in through `experimental-enable-byte-offsets = true`.

### Why This Matters

Partitions track how much log data they have applied in bytes, starting from when tracking is
enabled.

### Impact on Users

- On v1.9.0 and newer, tracking is enabled even if `experimental-enable-byte-offsets` is unset or
  `false`, once every worker node runs v1.9.0 or newer.
- While a worker node still runs an older version, tracking isn't turned on automatically, so a
  cluster that is being upgraded to v1.9 can still roll back to any v1.8 release, unless you
  enabled `experimental-enable-byte-offsets`.
- Once enabled, tracking stays enabled and the partition requires Restate v1.8.2 or newer.

### Migration Guidance

No action is needed to upgrade. Once every worker node runs v1.9.0 or newer and partitions have
turned tracking on, rollback requires v1.8.2 or newer; v1.8.0 and v1.8.1 are no longer supported
rollback targets.
