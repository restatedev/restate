# Dedicated partition processor RPC messages

## New Feature

Partition processor clients can now use dedicated RPC messages instead of the legacy shared
envelope. The new messages are opt-in and require network protocol V4, so rolling upgrades remain
compatible with older nodes.

Enable them with:

```toml
[common]
experimental-enable-partition-processor-dedicated-messages = true
```

When the option is disabled, or when a connection negotiates a protocol older than V4, clients
continue to use the legacy RPC envelope.
