# Release Notes: Restart Invocation from Journal Prefix

## New Feature

### What Changed

The `restart-as-new` operation now supports restarting a completed invocation from a specific journal entry index, preserving partial progress from the original invocation.

### Why This Matters

- **Preserve expensive work**: When restarting an invocation, you can keep completed side effects (calls, sleeps, runs) instead of re-executing everything
- **Managed deployments**: Restart-as-new integrates with deployment flows. See the [deployment flows documentation](https://docs.restate.dev/services/deployment/overview) for details.
- **Recovery**: Recover an invocation that recorded a journal entry leading to unwanted results (e.g., poison pill, invalid token) while preserving the work from before

### Impact on Users

- **New API parameter**: The `restart-as-new` endpoint now accepts a `from` parameter to specify the journal entry index
- **Protocol requirement**: Restarting from `from > 0` requires **Service Protocol V6** or later

### Usage

**UI:**

Restart invocations as new directly from the Restate UI invocation detail page. For prefix-based restarts with a specific journal entry, use the UI or REST API.

**CLI:**
```bash
# Restart a completed invocation (from beginning)
restate invocations restart-as-new <invocation_id>
```

Note: The CLI currently restarts from the beginning (`from=0`). Use the UI or REST API for prefix-based restarts.

**REST API:**

For REST API details, see the [Admin API documentation](https://docs.restate.dev/admin-api/invocation/restart-as-new-invocation).

### Behavior Details

When restarting from a prefix:
1. Journal entries from index 0 to `from` (inclusive) are copied to the new invocation
2. The new invocation receives a new invocation ID and resumes from where the prefix ends

**Requirements:**
- The original invocation must be **completed** (not running, scheduled, or in inbox)
- **Workflows** are not supported for restart-as-new
- Restarting from `from > 0` requires Protocol V6+; older protocols only support `from=0`

### Related Issues

- [#3761](https://github.com/restatedev/restate/pull/3761): Restart as new from prefix
- [#3885](https://github.com/restatedev/restate/pull/3885): Restart as new changes for 1.6
