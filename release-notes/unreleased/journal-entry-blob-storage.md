# Release Notes: Store large invocation outputs separately (experimental)

## New Feature

### What Changed

A new experimental option stores large invocation outputs (the journal's `Command: Output` entry)
separately from the journal. Small outputs and all other journal entries stay embedded.

### Why This Matters

Large outputs no longer bloat the journal, which is scanned in full far more often than the
output itself is needed.

### Impact on Users

Nothing changes without the option. With it, journals look the same in `sys_journal`, except with
the experimental query engine V2: there the content columns (`raw`, `entry_json`,
`entry_lite_json`, `name`) are empty for outputs stored separately.

Once enabled, the option cannot be turned off again, and you cannot roll back to an older Restate
version: outputs written meanwhile can't be read by older versions. This also applies to
partitions restored from snapshots taken with the option enabled.

### Migration Guidance

Enable the option after the whole cluster runs this version, since older versions can't restore
snapshots taken with it. Nodes can enable it independently:

```toml
experimental-enable-write-blob-table = true
```

Or via the environment variable `RESTATE_EXPERIMENTAL_ENABLE_WRITE_BLOB_TABLE=true`.
