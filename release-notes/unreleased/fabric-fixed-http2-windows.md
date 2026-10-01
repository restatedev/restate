# Release Notes: Fixed, symmetric HTTP/2 flow-control windows between nodes

## Behavioral Change

### What Changed

Node-to-node connections now use one fixed HTTP/2 flow-control window, set by
`networking.data-stream-window-size`, on both ends of every connection.

- The default rises from 2 MiB to 4 MiB.
- `networking.http2-adaptive-window` is deprecated and has no effect.

Previously, `data-stream-window-size` and `http2-adaptive-window` each took effect on only one end
of a connection, so neither setting behaved as documented.

### Why This Matters

`data-stream-window-size` now means the same thing in both directions, and it bounds how much
received but unprocessed data each connection can hold.

### Impact on Users

- **Nodes in the same data center:** no change in throughput.
- **High-latency links:** a connection carries at most about one window per round trip. With the
  new 4 MiB default, responses from a remote node can be slower than before on links with long
  round trips. In our tests at a 100 ms round trip, remote SQL scans reached about 27 MiB/s
  instead of 33 MiB/s. Raise `data-stream-window-size` for such links; 8 MiB matched the
  previous throughput.
- **Windows above 4 MiB** also need larger TCP buffers in the operating system on every node. On
  Linux, raise the maximum (third) values of `net.ipv4.tcp_wmem` and `net.ipv4.tcp_rmem`, because
  the kernel limits each connection's buffers independently of this setting.
- **Memory:** each node-to-node connection can hold up to one window of received data that the
  node has not processed yet.

### Migration Guidance

No action is required for nodes in one data center. If you set `networking.http2-adaptive-window`,
remove it. For long-distance links, size `networking.data-stream-window-size` to about twice the
bandwidth-delay product, and raise the operating system's TCP buffer limits if you go above 4 MiB:

```toml
[networking]
data-stream-window-size = "8MiB"
```
