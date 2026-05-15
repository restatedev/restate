# Release Notes for Issue #3872: Universal --output flag for list commands

## New Feature

### What Changed

`restate inv list` (and `restate inv ls`) now supports a `--output` flag for
machine-readable output, making it easy to pipe invocation IDs into other commands.

```
--output <FORMAT>    human (default) | table | json | jsonl
```

- **`human`** — existing pretty output with colors and icons (unchanged default)
- **`table`** — plain columns: ID, TARGET, STATUS, DEPLOYMENT, CREATED AT
- **`json`** — JSON array of all results
- **`jsonl`** — one JSON object per line (newline-delimited JSON)

### Examples

```bash
# Cancel all backing-off invocations
restate inv list --status backing-off --output jsonl | jq -r .id | xargs restate inv cancel

# Pipe into grep then cancel
restate inv list --output table | grep MyService | awk '{print $1}' | xargs restate inv cancel

# Pretty-print full JSON
restate inv list --all --output json | jq .
```

### Related Issues
- Issue #3872: Universal --output option for all get/list/describe commands
