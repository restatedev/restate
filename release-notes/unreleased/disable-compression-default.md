# Nodes no longer compress the messages they send to each other by default

## Behavioral Change

### What Changed

`networking.disable-compression` now defaults to `true`: nodes send the messages of their internal
gRPC connections uncompressed. Previously they compressed them with Zstd.

### Why This Matters

Compression cost CPU and memory for every message between nodes, including very small ones, and
limited how much data a single connection between two nodes could carry. In our measurements on a
single machine, turning compression off made small messages between nodes several times cheaper to
send and raised the throughput of large transfers on one connection more than tenfold.

### Impact on Users

- Deployments that do not set `networking.disable-compression` stop compressing traffic between
  nodes after the upgrade. Nodes use less CPU for internal traffic, and send more bytes over the
  network for payloads that compress well.
- Deployments that set `networking.disable-compression` explicitly keep their setting.
- Nodes accept compressed and uncompressed messages either way, so a cluster can run nodes with
  either setting, including nodes of older versions, during a rolling upgrade.

### Migration Guidance

No action is needed. If the network between your nodes is slow or billed by volume, and the data
your services exchange compresses well, you can restore compression:

```toml
[networking]
disable-compression = false
```

or through the environment:

```sh
RESTATE_NETWORKING__DISABLE_COMPRESSION=false
```
