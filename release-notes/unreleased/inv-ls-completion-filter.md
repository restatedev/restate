# Release Notes: `restate inv ls --completion-result` filter

## New Feature

### What Changed

The `restate inv ls` command now supports a `--completion-result` flag that filters
completed invocations by their result. Accepted values are `success` and
`failure` (case-insensitive). This flag implies `--status=completed`.

### Why This Matters

Previously there was no way to list only failed (or only successful) invocations
from the CLI. Users had to list all completed invocations and visually scan for
failures, or fall back to raw SQL queries.

### Usage

```bash
# List failed invocations
restate inv ls --completion-result failure

# List successful invocations for a specific service
restate inv ls --completion-result success --service MyService
```
