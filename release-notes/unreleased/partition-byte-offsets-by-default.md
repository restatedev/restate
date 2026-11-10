# Automatic partition byte-offset tracking from v1.9.0

## Behavioral Change

### What Changed

Partition byte-offset tracking is enabled automatically when a partition elects a leader running
v1.9.0 or newer, including development and release-candidate builds. In v1.8.2, it remains opt-in
through `experimental-enable-byte-offsets = true`.

### Why This Matters

Partitions track how much log data they have applied in bytes, starting from when tracking is
enabled.

### Impact on Users

- On v1.9.0 and newer, tracking is enabled even if `experimental-enable-byte-offsets` is unset or
  `false`.
- Once enabled, tracking stays enabled and the partition requires Restate v1.8.2 or newer.

### Migration Guidance

Upgrade every node to v1.8.2 or newer before introducing v1.9 nodes or enabling the option on
v1.8.2. After tracking is enabled, rollback requires v1.8.2 or newer; v1.8.0 and v1.8.1 are no
longer supported rollback targets.
