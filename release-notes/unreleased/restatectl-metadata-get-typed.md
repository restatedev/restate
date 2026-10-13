# Release Notes: `restatectl metadata get` decodes well-known keys only

## Behavioral Change

### What Changed
`restatectl metadata get` now decodes and pretty-prints only the well-known metadata keys:
`nodes_config`, `bifrost_config`, `partition_table`, `schema_registry`, and partition epoch
keys (`pp_epoch_<partition-id>`). Requesting any other key without `--raw` fails with an error
that suggests using `--raw`. Requesting a key that does not exist now fails with an error
instead of printing `null`.

### Why This Matters
Metadata values can be stored in different encodings (for example, the schema registry when
its experimental compact encoding is enabled). The command previously assumed a single
encoding and failed to decode such values. Each well-known key is now decoded using its correct format.

### Impact on Users
- Scripts that call `restatectl metadata get` on keys other than those listed above must add
  `--raw`, which writes the stored bytes to stdout without decoding.
- Scripts that relied on `null` output for missing keys should handle the non-zero exit code instead.
