# Log Server: Remove `writer-batch-commit-count` configuration option

## Deprecation

### What Changed

The `writer-batch-commit-count` option under `[log-server]` has been removed.
The log-server writer now uses size-based batching (capped at one memtable's
worth of data) instead of count-based batching, making this option unnecessary.

### Impact on Users

- **Existing deployments** with `writer-batch-commit-count` in their
  configuration: the field is silently ignored on upgrade. No errors or
  behavioral change.
- **New deployments**: no action needed.

### Migration Guidance

Remove `writer-batch-commit-count` from your `[log-server]` configuration
section if present. It has no effect.

```toml
# Before
[log-server]
writer-batch-commit-count = 5000  # remove this line

# After
[log-server]
# (no replacement needed — batching is now automatic and size-based)
```
