# Release Notes: Deprecated SDK Versions

## Breaking Change

### What Changed

Starting with Restate 1.6, **new invocations will be rejected** for services deployed using the following deprecated SDK versions:

| SDK | Deprecated Versions |
|-----|---------------------|
| Java/Kotlin | < 2.0 |
| TypeScript | <= 1.4 |
| Go | < 0.16 |
| Python | < 0.6 |
| Rust | < 0.4 |

These SDK versions use Service Protocol V1-V4, which are no longer supported for new invocations. If you attempt to invoke a service using a deprecated SDK, you will receive error code **RT0020**.

**Note**: Existing in-flight invocations on deprecated deployments will continue to execute normally. Only new invocations are rejected.

### Why This Matters

Older SDK versions are no longer recommended for production use. Upgrading ensures you benefit from the latest protocol improvements, bug fixes, and new features like:
- Deterministic random number generation via `ctx.rand()`
- Error metadata propagation
- Improved retry policies

### Migration Guidance

If you encounter RT0020 errors:

1. **Identify affected deployments**: Use `restate deployments list` to see deployment protocol versions
2. **Upgrade your SDK** to a supported version:
   - [Java/Kotlin SDK](https://github.com/restatedev/sdk-java) >= 2.0
   - [TypeScript SDK](https://github.com/restatedev/sdk-typescript) >= 1.5
   - [Go SDK](https://github.com/restatedev/sdk-go) >= 0.16
   - [Python SDK](https://github.com/restatedev/sdk-python) >= 0.6
   - [Rust SDK](https://github.com/restatedev/sdk-rust) >= 0.4
3. **Re-register your deployment**: After upgrading, register the new deployment with Restate

### Related Issues

- [#3844](https://github.com/restatedev/restate/pull/3844): Enable Service Protocol v6
- [#3877](https://github.com/restatedev/restate/pull/3877): Don't allow requests to deployments using old protocol
