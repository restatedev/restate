# Separate control address advertisement

## New Feature

### What Changed

Restate nodes now publish a control address in cluster metadata when provisioning
or joining. By default, it is the same address and port as the fabric advertised
address, so no configuration change is needed.

### Configuration

To advertise a different route for control RPCs, such as a node-specific HTTPS
proxy, configure:

```toml
ctrl-advertised-address = "https://node-7.example.com"
```

Alternatively, set `ctrl-advertised-host` to change only the hostname while
using the shared listener's actual port and TLS mode. A full control advertised
address takes precedence over the hostname override.

The same overrides are available as `--ctrl-advertised-host` and
`--ctrl-advertised-address`, or through `RESTATE_CTRL_ADVERTISED_HOST` and
`RESTATE_CTRL_ADVERTISED_ADDRESS`. Restart the node to publish a changed
advertisement. Control RPCs still share the existing fabric listener; these
options do not open another port.

### Impact on Users

`restatectl` now uses each node's advertised control address for discovered-node
requests, including node inspection, storage compaction, metadata-server
administration, and log-server inspection. Nodes that do not publish a control
address continue to use their fabric address.

Initial discovery still uses the addresses supplied with `--address`. If you
configure a separate control route, make sure it is reachable from `restatectl`
and forwards the RPC services needed by your commands.
