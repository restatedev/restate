# Release Notes for Issue #3961: Change abort timeout default

## Behavioral Change

### What Changed
The default value for `abort-timeout` has been changed from **1 minute (60 seconds)** to **10 minutes (600 seconds)**.

### Why This Matters
The abort timeout is a critical configuration that guards against stalled service/handler invocations. This timer:
- Starts after the `inactivity-timeout` has expired and the service has been asked to gracefully terminate
- Forces termination of the invocation when it expires
- **Can potentially interrupt user code**

### Impact on Users
- **Existing deployments**: Services that previously had invocations forcefully terminated after 1 minute of graceful shutdown time will now have 10 minutes
- **New deployments**: Will use the new 10-minute default automatically
- **Custom configurations**: Any explicitly configured `abort-timeout` values in configuration files will continue to work as before and are not affected by this change

If you need to revert to the previous behavior, you can explicitly configure:
```toml
[worker.invoker]
abort-timeout = "1m"
```

### Related Issues
- [#3961](https://github.com/restatedev/restate/issues/3961): Change defaults of abort timeout
- [#2761](https://github.com/restatedev/restate/issues/2761): Rethink inactivity timeout under high load
