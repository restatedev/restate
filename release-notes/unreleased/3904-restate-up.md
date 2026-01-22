# Release Notes: `restate up` Command

## New Feature

### What Changed

A new `restate up` command (alias: `restate dev`) is now available in the CLI for quickly starting a local Restate instance for development and testing.

### Why This Matters

- **Quick start**: Spin up a local Restate server with a single command
- **Zero configuration**: Uses sensible defaults with ephemeral storage
- **Developer-friendly**: Opens the Admin UI automatically in your browser

### Impact on Users

- **New CLI command**: Available in npm packages, Homebrew, and cargo-dist releases
- **Not in Docker images**: This command is intentionally excluded from Docker images (use `restate-server` for containerized deployments)

### Usage

```bash
# Start with default settings
restate up

# Or use the alias
restate dev

# Use random ports to avoid conflicts
restate up --use-random-ports

# Use Unix sockets only
restate up --use-unix-sockets

# Keep the data directory after exit (for debugging)
restate up --retain
```

**On startup, you'll see:**
- Restate logo
- Bound addresses (Admin URL, HTTP Ingress URL)
- Data directory path
- Browser opens to Admin UI automatically

**Options:**
- `-r, --use-random-ports`: Start on random ports to avoid conflicts
- `-u, --use-unix-sockets`: Start on Unix sockets only
- `--retain`: Do not delete the temporary data directory after exiting

### Important Notes

> **This tool is for local development and testing only.**
> 
> Do NOT use `restate up` in production. For production deployments, use `restate-server` with proper configuration.

- Data is stored in an **ephemeral temporary directory** that is deleted on exit (unless `--retain` is used)
- Single node, single partition - no replication
- Press Ctrl+C to stop

### Related Issues

- [#3904](https://github.com/restatedev/restate/pull/3904): restate up command
