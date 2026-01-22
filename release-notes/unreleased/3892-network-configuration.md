# Release Notes for PR #3892: Network Configuration Overhaul

## New Feature / Behavioral Change

### What Changed

This release introduces a major rework of how network ports and addresses are configured in Restate Server. The changes bring new capabilities, simplified configuration, and better defaults.

Additionally, `bind-address` is no longer derived from `advertised-address`. These are now independent configuration options.

#### Unix Domain Sockets Support

Restate Server now supports listening on Unix domain sockets for all services (fabric, admin, ingress, and tokio-console). By default, the server listens on **both** TCP sockets and Unix domain sockets simultaneously. Unix sockets are created under the `restate-data/` directory:
- `restate-data/fabric.sock` - Node-to-node communication
- `restate-data/admin.sock` - Admin API
- `restate-data/ingress.sock` - HTTP ingress
- `restate-data/tokio.sock` - Tokio console (if enabled)

#### New Configuration Options

The following new global options are available and can be overridden per-service:

| Option | Description | Default |
|--------|-------------|---------|
| `listen-mode` | Controls socket types: `tcp`, `unix`, or `all` | `all` |
| `use-random-ports` | Use OS-assigned random ports | `false` |
| `bind-ip` | Local interface IP address to bind | `0.0.0.0` |
| `bind-port` | Network port to listen on | Service-specific |
| `advertised-host` | Hostname for advertised addresses | Auto-detected |
| `advertised-address` | Full URL for service advertisement | Auto-derived |

#### Automatic Advertised Address Detection

The server now automatically detects a reasonable advertised address:
- When `listen-mode=unix`, advertised addresses use `unix:/` format
- When `listen-mode=tcp` or `all`, the server detects the public routable IP address instead of defaulting to `127.0.0.1`

This significantly improves Docker deployments and multi-node clusters by reducing required configuration.

#### Simplified Metadata Client Configuration

Nodes no longer need to include their own address in `metadata-client.addresses`. For single-node setups, this field can be left empty. The server automatically includes its own address if running a metadata server role.

### Deprecated Options

The following options are deprecated and will be removed in a future release:

| Deprecated Option | Replacement |
|-------------------|-------------|
| `admin.advertised-admin-endpoint` | `admin.advertised-address` |
| `ingress.advertised-ingress-endpoint` | `ingress.advertised-address` |

### Why This Matters

1. **Easier development**: Unit tests and local development now use Unix sockets by default, eliminating port conflicts
2. **Better container support**: Auto-detection of public IP addresses reduces configuration burden in containerized deployments
3. **Enhanced flexibility**: Socket activation support enables zero-downtime restarts via systemd or similar tools
4. **Early failure detection**: The server now binds all required ports early in startup, before opening databases

### Impact on Users

- **Existing deployments**: Existing configurations continue to work. The server will additionally listen on Unix sockets unless `listen-mode=tcp` is set
- **New deployments**: Benefit from simplified configuration with auto-detection of advertised addresses
- **CLI tools**: Both `restate` and `restatectl` now support Unix socket connections via `unix:` prefix (e.g., `restatectl -s unix:restate-data/admin.sock status`)
- **Docker users**: Should see improved out-of-box experience with auto-detected advertised addresses

### Migration Guidance

No migration is required. To opt out of Unix socket listening:

```toml
# Only listen on TCP sockets
listen-mode = "tcp"
```

To use only Unix sockets (useful for embedded scenarios):

```toml
# Only listen on Unix sockets
listen-mode = "unix"
```

To use random ports (useful for testing):

```bash
restate-server --use-random-ports=true
```

To migrate from deprecated options:

```toml
# Old (deprecated)
[admin]
advertised-admin-endpoint = "http://my-host:9070"

# New
[admin]
advertised-address = "http://my-host:9070"
# Or just set the host part:
advertised-host = "my-host"
```

### Socket Activation Support

Restate now supports systemd-compatible socket activation. A parent process can pre-bind TCP or Unix socket listeners and pass them to restate-server via `LISTEN_FD`. This enables:
- Zero-downtime restarts (clients don't see connection errors during upgrades)
- Pre-allocated ports for test harnesses

Example with `systemfd`:
```bash
systemfd --no-pid -s http::9000 -- restate-server
```

### Related Issues
- [#3892](https://github.com/restatedev/restate/pull/3892): On listeners, addresses, and ports
