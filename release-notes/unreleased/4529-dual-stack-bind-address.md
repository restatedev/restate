# Dual-stack support: default bind address changed to `::`

## Behavioral Change

### What Changed

The default `bind-ip` has changed from `0.0.0.0` (IPv4 only) to `::` (IPv6
unspecified), which on most systems creates a dual-stack socket that accepts
both IPv4 and IPv6 connections.

Additionally, the automatic routable IP detection now prefers IPv6, falling
back to IPv4 when IPv6 is not available, improving support for IPv6-only and
dual-stack environments.

### Why This Matters

In IPv6-only environments (e.g. Kubernetes clusters with IPv6-only networking),
Restate would fail to start because `0.0.0.0` cannot bind when no IPv4 stack
is available. In dual-stack environments where DNS resolves peer addresses to
IPv6, nodes were unreachable because the server only listened on IPv4. Users
had to manually set `bind-ip = "::"` as a workaround.

By defaulting to `::`, the server now listens on both IPv4 and IPv6 interfaces,
making it reachable regardless of which address family peers or clients use.

### Impact on Users

- **Existing IPv4 deployments**: No action required. On Linux and macOS, `::` with
  the default `IPV6_V6ONLY=false` kernel setting creates a dual-stack socket that
  continues to accept IPv4 connections via IPv4-mapped IPv6 addresses.
- **IPv6-only deployments**: Restate now starts and works out of the box without
  needing to configure `bind-ip`.
- **Dual-stack deployments**: Restate is now reachable over both IPv4 and IPv6,
  regardless of which address family peers resolve to.
- **Explicit `bind-ip` configured**: If you have already set `bind-ip` in your
  configuration, this change has no effect — your explicit value takes precedence.
- **Edge case**: On the rare Linux systems where `net.ipv6.bindv6only=1` is set,
  binding to `::` will only accept IPv6 connections. Set `bind-ip = "0.0.0.0"` to
  restore IPv4-only behavior.

### Migration Guidance

No migration is required for the vast majority of deployments. If you need to
restore the previous behavior:

```toml
# Restore IPv4-only binding
bind-ip = "0.0.0.0"
```

Or via environment variable:

```bash
RESTATE_BIND_IP=0.0.0.0
```

### Related Issues
- [#4529](https://github.com/restatedev/restate/issues/4529): Add support for dual-stack
