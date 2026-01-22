# Release Notes: Batch Invocation Operations

## New Feature / Improvement

### What Changed

Added support for batch invocation operations to efficiently manage multiple invocations at once, with improvements to both the UI and CLI.

### Why This Matters

- **Efficient management**: Operate on hundreds of invocations without manual iteration
- **Better UX**: Progress bars and concurrency limits provide visibility and prevent overwhelming the server
- **UI support**: The web UI can now perform batch operations efficiently

### Impact on Users

- **CLI users**: Batch commands now show progress and execute faster
- **UI users**: Can perform batch operations directly from the web interface

### Usage

**CLI batch operations:**
```bash
# Cancel all running invocations for a service (with progress bar)
restate invocations cancel MyService

# Kill invocations with a limit
restate invocations kill MyService --limit 100

# Purge completed invocations
restate invocations purge MyService/myHandler

# Pause all running invocations matching a query
restate invocations pause MyService

# Resume all paused invocations
restate invocations resume MyService

# Restart completed invocations as new
restate invocations restart-as-new MyService
```

**Query patterns supported:**
- Service name: `MyService`
- Service and handler: `MyService/myHandler`
- Virtual object with key: `MyVirtualObject/myKey`
- Specific invocation ID: `inv_1abc...`

**Options:**
- `--limit <N>`: Maximum number of invocations to process (default: 500)
- `--yes` / `-y`: Skip confirmation prompt

### Related Issues

- [#3856](https://github.com/restatedev/restate/issues/3856): Add support for bulk operations
