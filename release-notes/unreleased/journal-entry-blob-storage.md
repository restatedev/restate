# Release Notes: Store large journal entries separately (experimental)

## New Feature

### What Changed

A new experimental option stores large journal entries separately from the journal. Entries up to
`worker.inline-journal-entry-threshold` (default 4 KiB) stay embedded, as storing them
separately isn't worth it.

### Why This Matters

Large entries, such as big inputs, call parameters or outputs, no longer bloat the journal, which
is scanned in full far more often than the content of a single entry is needed.

### Impact on Users

Nothing changes without the option. With it, journals look the same in `sys_journal`, except with
the experimental query engine V2: there the content columns (`raw`, `entry_json`,
`entry_lite_json`, `name`) are empty for entries stored separately.

Once enabled, the option cannot be turned off again, and you cannot roll back to an older Restate
version: entries written meanwhile can't be read by older versions. This also applies to
partitions restored from snapshots taken with the option enabled.

### Migration Guidance

Enable the option after the whole cluster runs this version, since older versions can't restore
snapshots taken with it. Nodes can enable it independently:

```toml
experimental-enable-write-blob-table = true
```

Or via the environment variable `RESTATE_EXPERIMENTAL_ENABLE_WRITE_BLOB_TABLE=true`.

Optionally tune which entries stay embedded (`0` stores every entry separately):

```toml
[worker]
inline-journal-entry-threshold = "4 KiB"
```
