# Release Notes: `restate invocations purge --completion-result`

## New Feature

### What Changed
`restate invocations purge` now accepts `--completion-result <success|failure>` to purge only completed invocations that finished with the given result.

### Why This Matters
You can clean up, for example, only successful invocations, while keeping failed ones around for investigation.

### Impact on Users
- Existing usage of `restate invocations purge` is unchanged; without the flag all matching completed invocations are purged.
- Example: `restate invocations purge Greeter/greet --completion-result success --dry-run`
